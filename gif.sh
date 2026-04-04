#!/bin/bash

set -e

cd data/output

for f in $(ls png | cut -d- -f 1 | uniq); do
    ffmpeg -y -pattern_type glob -i "png/$f-*.png" -vf palettegen palette.png
    ffmpeg -y -framerate 0.85 -pattern_type glob -i "png/$f-*.png" -i palette.png -lavfi paletteuse "$f.gif"
done
rm palette.png

cd ..
rm -rf gif && mkdir gif
mv output/*.gif gif/
