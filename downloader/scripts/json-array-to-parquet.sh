#!/usr/bin/env bash
#
# Convert a Bioportal .json.bz2 big array download to .jsonl and .parquet
#
set -eu -o pipefail

filename="${1?"No filename given"}"
basename="${filename%.json}"
if [ "${basename}.json" != "${filename}" ]
then
  echo "$(date): ERROR: Filename must have a .json extension; got ${filename}"
  exit 1
fi

echo "$(date): Converting ${basename}.json to ${basename}.jsonl..."
cat "${basename}".json \
  | json2jsonl \
  > "${basename}".jsonl
echo "$(date): Converted ${basename}.json to ${basename}.jsonl."


echo "$(date): Compressing ${basename}.json to ${basename}.json.bz2..."
(bzip2 -9 "${basename}".json \
  && echo "$(date): Compressed ${basename}.json to ${basename}.json.bz2.") &
BZIP2_PID=$!


echo "$(date): Converting ${basename}.jsonl to Parquet..."
json2parquet \
  --compression gzip \
  --statistics page \
  "${basename}".jsonl \
  "${basename}".parquet
echo "$(date): Converted ${basename}.jsonl to Parquet."


wait $BZIP2_PID