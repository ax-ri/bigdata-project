#!/bin/bash

# set -x
set -e

# move the csv in sub-folders to csv files
cd data/generated
for dir in ./*; do
    if [ -d "$dir" ]; then
        b=$(basename "$dir")
        mv "$dir/"*.csv "$b.csv"
    fi
done
find -type d -exec rm -r '{}' \;
cd ../..

cd bigdata-front
uv run map-renderer.py ../data/generated/ ../data/output --clean
