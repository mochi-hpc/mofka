#!/usr/bin/env bash

HERE=$(dirname $0)

echo "================================================"
echo "Before Test"
echo "================================================"

echo "==> Removing and re-creating log directory"
rm -rf /tmp/mofka-logs
mkdir /tmp/mofka-logs

echo "==> Starting Mofka server"
bedrock ofi+tcp -c $HERE/config.json -v trace 1> mofka.out 2> mofka.err &
BEDROCK_PID=$!

echo "==> Bedrock has started with PID ${BEDROCK_PID}"
echo $BEDROCK_PID > mofka.pid

disown $BEDROCK_PID

echo "==> Waiting for mofka.json to appear..."
while [ ! -f "mofka.json" ]; do
    if ! kill -0 "$BEDROCK_PID" 2>/dev/null; then
        echo "==> Bedrock (PID ${BEDROCK_PID}) exited before mofka.json appeared" >&2
        echo "==> mofka.err:" >&2
        cat mofka.err >&2
        exit 1
    fi
    sleep 1
done

echo "==> Mofka server is ready"
echo "================================================"
