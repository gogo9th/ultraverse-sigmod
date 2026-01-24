FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential cmake pkg-config bison flex \
    git ca-certificates curl \
    libboost-all-dev libssl-dev zlib1g-dev libzstd-dev \
    libprotobuf-dev protobuf-compiler \
    libmysqlclient-dev libtbb-dev libtirpc-dev \
    golang-go \
  && rm -rf /var/lib/apt/lists/*

ENV GOPATH=/go
ENV PATH="${GOPATH}/bin:/usr/local/go/bin:/usr/bin:${PATH}"

RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28

WORKDIR /work
