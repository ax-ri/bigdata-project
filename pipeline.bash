#!/bin/bash

# set -x
set -e

# rm -rf generated && mkdir generated

# move the csv in sub-folders to csv files
cd data/generated
for dir in ./*; do
    if [ -d "$dir" ]; then
        b=$(basename "$dir")
        mv "$dir/"*.csv "$b.csv"
        rm -r "$dir"
    fi
done
mkdir map plot
# mv map-*.csv map/
mv plot-*.csv plot/
cd ../..

cd bigdata-front
# uv run main.py ../data/generated/ ../data/output --clean
