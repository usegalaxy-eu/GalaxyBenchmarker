import dataclasses
import logging
import os
import time

import boto3

log = logging.getLogger(__name__)


@dataclasses.dataclass
class S3Config:
    base_url: str
    bucket_name: str

    @property
    def access_key_id(self):
        access_key = os.getenv("AWS_ACCESS_KEY_ID")
        if access_key is None:
            raise ValueError("Missing S3 credentials in env vars: AWS_ACCESS_KEY_ID")
        return access_key

    @property
    def secret_access_key(self):
        secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        if secret_key is None:
            raise ValueError(
                "Missing S3 credentials in env vars: AWS_SECRET_ACCESS_KEY"
            )
        return secret_key


def empty_bucket(config: S3Config) -> None:
    client = boto3.resource(
        "s3",
        aws_access_key_id=config.access_key_id,
        aws_secret_access_key=config.secret_access_key,
        endpoint_url=config.base_url,
    )
    bucket = client.Bucket(config.bucket_name)
    bucket.objects.all().delete()


def check_bucket_for_files(
    config: S3Config, expected_num_files: int, expected_size_in_bytes: int, timeout: int
) -> None:
    start_time = time.monotonic()
    num_files = _get_current_num_files(config, expected_size_in_bytes)

    while num_files < expected_num_files:
        log.info("Currently %d files present", num_files)
        current_runtime = time.monotonic() - start_time
        if current_runtime >= timeout:
            raise RuntimeError("Verification timed out")

        time.sleep(5)
        num_files = _get_current_num_files(config, expected_size_in_bytes)


def _get_current_num_files(config: S3Config, expected_size: int) -> int:
    client = boto3.client(
        "s3",
        aws_access_key_id=config.access_key_id,
        aws_secret_access_key=config.secret_access_key,
        endpoint_url=config.base_url,
    )
    result = client.list_objects_v2(Bucket=config.bucket_name, Delimiter="/")
    if "CommonPrefixes" not in result:
        return 0

    # Each prefix contains 1000 files, except the last one
    num = (len(result["CommonPrefixes"][:-1])) * 1000
    # Subtract one because prefix 000 only has 999 files
    num = max(0, num - 1)

    last_prefix = result["CommonPrefixes"][-1]["Prefix"]
    resp = client.list_objects_v2(Bucket=config.bucket_name, Prefix=last_prefix)
    num += len(list(item for item in resp["Contents"] if item["Size"] == expected_size))

    return num
