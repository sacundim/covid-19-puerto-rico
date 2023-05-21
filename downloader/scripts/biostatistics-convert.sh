#!/usr/bin/env bash

set -eu -o pipefail

HERE="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
TEMPLATES_DIR="${HERE}/../duckdb/Biostatistics/"
dataset="${1?"No dataset given"}"
downloaded_at="${2?"No downloaded_at given"}"

input_json="${dataset}_${downloaded_at}.json"
output_parquet="${dataset}_${downloaded_at}.parquet"

echo "$(date): Converting ${input_json} to Parquet..."
jinja \
  -D downloaded_at "${downloaded_at}" \
  -D input_json "${input_json}" \
  -D output_parquet "${output_parquet}" \
  "${TEMPLATES_DIR}/${dataset}.sql.j2" \
  | duckdb -bail
echo "$(date): Converted ${input_json} to Parquet."


echo "$(date): Compressing ${input_json}..."
lbzip2 -9 "${input_json}"
echo "$(date): Compressed ${input_json}."
