#!/usr/bin/env bash

HERE=$(dirname $0)

echo "================================================"
echo "Before Test"
echo "================================================"

echo "==> Removing and re-creating log directory"
rm -rf /tmp/mofka-logs
mkdir /tmp/mofka-logs

echo "==> Starting Mofka server"
bedrock tcp -c $HERE/config.json 1> mofka.out 2> mofka.err &
BEDROCK_PID=$!

echo "==> Bedrock has started with PID ${BEDROCK_PID}"
echo $BEDROCK_PID > mofka.pid

disown $BEDROCK_PID

echo "==> Waiting for mofka.json to appear..."
while [ ! -f "mofka.json" ]; do
    sleep 1
done

echo "==> Mofka server is ready"
echo "================================================"
