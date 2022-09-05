FROM golang:1.15-alpine

RUN apk add --update --no-cache \
    git \
  && git clone https://github.com/chinglinwen/s3-benchmark.git /s3-benchmark \
  && cd /s3-benchmark \
  && git checkout tags/v1.0-beta -b branch-v1.0-beta \
  && go get code.cloudfoundry.org/bytefmt \
  && go get github.com/aws/aws-sdk-go/aws \
  && go get github.com/jmespath/go-jmespath \
  && go build s3-benchmark.go

WORKDIR /s3-benchmark
ENTRYPOINT [ "./s3-benchmark" ]