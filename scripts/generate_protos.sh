#!/bin/sh
set -eu

cd /app

echo 'Starting one-shot service for protobuf files re-generation'

echo 'Cleaning old generated protobuf files...'
find utils/pb -type f \( \
  -name '*_pb2.py' -o \
  -name '*_pb2_grpc.py' -o \
  -name '*_pb2.pyi' \
\) -delete

find utils/pb -type d -name '__pycache__' -exec rm -rf {} +

echo 'Generating protobuf files with local imports...'

find utils/pb -name '*.proto' | sort | while read -r proto; do
  dir="$(dirname "${proto}")"
  file="$(basename "${proto}")"

  echo "Generating ${proto}"

  (
    cd "$dir"

    python -m grpc_tools.protoc \
      -I. \
      --python_out=. \
      --pyi_out=. \
      --grpc_python_out=. \
      "$file"
  )
done

echo 'Proto generation completed.'