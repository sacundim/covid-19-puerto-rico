#!/usr/bin/env bash

file="${1}"
date="${2}"
echo 'bulletin_date,municipality,count,percent' \
    > municipalities-molecular-"${date}".csv
cat "${file}" \
    | sed -e '/^"",Frecuencia,Porciento/d' \
    | sed -e '/^Característica,,/d' \
    | sed -e '/^"",(n),(%)/d' \
    | sed -e '/^Municipios de residencia,,/d' \
    | sed -e '/^"",,/d' \
    | sed -e 's/"\* /"/' \
    | sed -e "s/^/${date},/" \
    >> municipalities-molecular-"${date}".csv