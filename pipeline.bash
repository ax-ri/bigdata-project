#!/bin/bash

set -e

# cleanup
cd data
rm -rf generated output && mkdir generated output
cd ..

# run computations
docker run -it --rm --user 1000:1000 \
    -v "$(pwd)":/project \
    maven:3.9.14-eclipse-temurin-11 \
    bash -c ' \
        cd /project/bigdata-back && \
        mvn package && \
        java -cp target/bigdata-back-1.0-SNAPSHOT.jar fr.ensta.bigdata.utils.CsvToParquet /project/data/input/merged_data.csv /project/data/input/all-data.parquet  && \
        java -cp target/bigdata-back-1.0-SNAPSHOT.jar fr.ensta.bigdata.Main /project/data/input/all-data.parquet /project/data/input/countries_iso.csv /project/data/generated
    '

# move the csv in sub-folders to csv files
./scripts/flatten-generated.bash

# generate visualization
docker run -it --rm \
    -v "$(pwd)":/project \
    ghcr.io/astral-sh/uv:debian \
    bash -c ' \
        cd /project/bigdata-front && \
        uv sync && \
        uv run plotly_get_chrome -y && \
        apt update && apt install -y libnss3 libatk-bridge2.0-0 libcups2 libxcomposite1 libxdamage1 libxfixes3 libxrandr2 libgbm1 libxkbcommon0 libpango-1.0-0 libcairo2 libasound2 && \
        uv run main.py /project/data/generated /project/data/output --clean && \
        chown -R 1000:1000 /project/data
    '

# generate gif for feature maps
cwd="$(pwd)/data/output"
ffmpeg_cmd="docker run -it --rm --user 1000:1000 -v ""$cwd"":""$cwd"" -w ""$cwd"" jrottenberg/ffmpeg"
./scripts/generate-gif.bash "$ffmpeg_cmd"
