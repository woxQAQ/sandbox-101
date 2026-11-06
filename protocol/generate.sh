#!/bin/sh
set -eu
cd "$(dirname "$0")"
# Install reproducibly with:
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.10
protoc --go_out=gen --go_opt=paths=source_relative sandbox.proto
