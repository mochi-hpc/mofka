#!/usr/bin/env bash

echo "================================================"
echo "After test"
echo "================================================"

echo "==> Shutting down server"
BEDROCK_PID=$(<mofka.pid)

if kill -0 $BEDROCK_PID 2>/dev/null; then
    kill $BEDROCK_PID
else
    echo "Mofka isn't running anymore, here are the logs:"
    cat mofka.err mofka.out
fi

sleep 1
rm mofka.json mofka.pid

echo "==> Removing log directory"
rm -rf /tmp/mofka-logs

echo "==> All done"
echo "================================================"
