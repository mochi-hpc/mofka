#!/usr/bin/env bash

HERE=$(dirname $0)

$HERE/pre-test.sh

echo "{\"group_file\":\"mofka.json\", \"margo\":{\"use_progress_thread\":true}}" > benchmark_config.json

echo "PYTHONPATH=$PYTHONPATH"
echo "LD_LIBRARY_PATH=$LD_LIBRARY_PATH"

export DIASPORA_CTL_DRIVER_OPTIONS="--driver mofka --driver.group_file mofka.json"

echo "==> Creating topic and partition"
diaspora-ctl topic create --name my_topic --topic.num_partitions 1
r="$?"
if [ "$r" -ne "0" ]; then
    $HERE/post-test.sh $r
    exit 1
fi

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
r="$?"
if [ "$r" -ne "0" ]; then
    $HERE/post-test.sh $r
    exit 1
fi

echo "==> Running consumer benchmark"
diaspora-consumer-benchmark -d mofka \
                            -c benchmark_config.json \
                            -t my_topic \
                            -n 100 \
                            -s 0.5 \
                            -i 0.8 \
                            -p 1
r="$?"
if [ "$r" -ne "0" ]; then
    $HERE/post-test.sh $r
    exit 1
fi

$HERE/post-test.sh 0
