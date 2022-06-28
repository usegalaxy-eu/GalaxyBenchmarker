FROM golang:1.14.7-alpine

WORKDIR /warp

ENV CGO_ENABLED=0

RUN apk add --update --no-cache \
    git \
  && git clone https://github.com/minio/warp /warp \
  && cd /warp \
  && git checkout tags/v0.5.5 -b branch-v0.5.5 \
  && go mod download \
  && go build -ldflags '-w -s' -a -o warp .


FROM scratch

COPY --from=0 /warp/warp /warp

ENTRYPOINT ["/warp"]