#!/usr/bin/env bash

set -eux -o pipefail

BASE_URL="https://github.com/duckdb/duckdb/releases/download"
VERSION="v0.8.0"
AARCH64_URL="${BASE_URL}/${VERSION}/duckdb_cli-linux-aarch64.zip"
X86_64_URL="${BASE_URL}/${VERSION}/duckdb_cli-linux-amd64.zip"

ARCHITECTURE="$(uname -m)"
case "$ARCHITECTURE" in
  aarch64)
    URL="${AARCH64_URL}"
    ;;
  x86_64)
    URL="${X86_64_URL}"
    ;;
  *)
    echo "ERROR: Unsupported ARCHITECTURE: $ARCHITECTURE"
    ;;
esac

cd /tmp
wget \
  --no-verbose \
  -O duckdb_cli-linux.zip \
  "${URL}"

unzip duckdb_cli-linux.zip
mv duckdb /usr/local/bin/
echo "Installed duckdb: $(duckdb -version)"