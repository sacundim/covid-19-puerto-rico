#!/usr/bin/env bash
#
# Take an uncompressed CSV file, and output Parquet and .csv.bz2
#
set -eu -o pipefail

filename="${1?"No filename given"}"
basename="${filename%.csv}"
if [ "${basename}.csv" != "${filename}" ]
then
  echo "$(date): ERROR: Filename must have a .csv extension; got ${filename}"
  exit 1
fi

echo "$(date): Converting ${filename} to Parquet..."
csv2parquet \
  --compression gzip \
  --statistics page \
  "${basename}".csv \
  "${basename}".parquet
echo "$(date): Converted ${filename} to Parquet."

echo "$(date): Compressing ${filename}..."
lbzip2 -9 "${basename}".csv
echo "$(date): Compressed ${filename}."
