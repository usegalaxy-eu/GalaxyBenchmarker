FROM alpine

RUN apk add --no-cache fio

COPY scripts/ /scripts

RUN echo hello world

ENTRYPOINT [ "fio" ]