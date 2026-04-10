#!/bin/bash

set -e

cd data/output
ffmpeg_cmd="$1"
for f in $(ls af_map | cut -d- -f 2 | uniq); do
    $ffmpeg_cmd -y -pattern_type glob -i "af_map/af_map-$f-*.png" -vf palettegen palette.png
    $ffmpeg_cmd -y -framerate 0.85 -pattern_type glob -i "af_map/af_map-$f-*.png" -i palette.png -lavfi paletteuse "$f.gif"
done
mkdir af_map/{png,gif}
mv af_map/*.png af_map/png
mv ./*.gif af_map/gif
rm palette.png
