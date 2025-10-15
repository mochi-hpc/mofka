#!/usr/bin/env bash

HERE=$(dirname $0)

$HERE/pre-test.sh

echo "{\"group_file\":\"mofka.json\", \"margo\":{\"use_progress_thread\":true}}" > benchmark_config.json

RET=0

echo "==> Creating topic"
python -m mochi.mofka.mofkactl topic create my_topic -g mofka.json

echo "==> Adding memory partition"
python -m mochi.mofka.mofkactl partition add my_topic -r 0 -t memory -g mofka.json

echo "==> Running producer benchmark"
diaspora-producer-benchmark -d mofka \
                            -c benchmark_config.json \
                            -t my_topic \
                            -n 100 \
                            -m 16 \
                            -s 128 \
                            -b 8 \
                            -f 10 \
                            -p 1

echo "==> Running consumer benchmark"
diaspora-consumer-benchmark -d mofka \
                            -c benchmark_config.json \
                            -t my_topic \
                            -n 100 \
                            -s 0.5 \
                            -i 0.8 \
                            -p 1

$HERE/post-test.sh
