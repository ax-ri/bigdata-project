#!/bin/bash

set -e

cd data/generated
rm -rf af_map af_plot cluster_map success_plot
for dir in ./*; do
    if [ -d "$dir" ]; then
        b=$(basename "$dir")
        mv "$dir/"*.csv "$b.csv"
        rm -r "$dir"
    fi
done
mkdir af_map af_plot cluster_map success_plot
mv af_map-*.csv af_map/
mv af_plot-*.csv af_plot/
mv cluster_map-*.csv cluster_map/
mv success_plot-*.csv success_plot/
