#!/bin/bash

set -e

HERE="$( dirname -- "${BASH_SOURCE[0]}"; )"
HERE="$( realpath -e -- "$HERE"; )"

bedrock tcp -c $HERE/config.json -v trace 1> mofka.log 2>&1 &
bedrock_pid=$!

sleep 5

export PYTHONPATH=$HERE/../../build/python:$HERE/../../python:$PYTHONPATH

METADATA_PROVIDER=$(
    python -m mochi.mofka.mofkactl metadata add \
            --rank 0 \
            --groupfile mofka.flock.json \
            --type log \
            --config.path /tmp/mofa/mofka-log \
            --config.create_if_missing true
        )

DATA_PROVIDER=$(
    python -m mochi.mofka.mofkactl data add \
            --rank 0 \
            --groupfile mofka.flock.json \
            --type abtio \
            --config.path /tmp/mofa/mofka-data \
            --config.create_if_missing true
        )

echo "Metadata provider: ${METADATA_PROVIDER}"
echo "Data provider: ${DATA_PROVIDER}"

python -m mochi.mofka.mofkactl \
    topic create my_topic --groupfile mofka.flock.json
python -m mochi.mofka.mofkactl \
    partition add my_topic --type default --rank 0 --groupfile mofka.flock.json \
                           --metadata "${METADATA_PROVIDER}" --data "${DATA_PROVIDER}"

echo "Starting python code"

python work.py

kill $bedrock_pid
wait $bedrock_pid
