#!/usr/bin/env bash

echo "================================================"
echo "After test"
echo "================================================"

echo "==> Shutting down server"
BEDROCK_PID=$(<mofka.pid)

show_logs=0

if kill -0 $BEDROCK_PID 2>/dev/null; then
    kill $BEDROCK_PID
    sleep 1
else
    echo "Mofka isn't running anymore when it should"
    show_logs=1
fi

if [ "$1" -ne "0" ]; then
    echo "The test didn't seem to succeed"
    show_logs=1
fi

if [ "$show_logs" -ne 0 ]; then
    echo "Mofka stderr ===================================="
    cat mofka.err
    echo "Mofka stdout ===================================="
    cat mofka.out
fi

rm mofka.json mofka.pid

echo "==> Removing log directory"
rm -rf /tmp/mofka-logs

echo "==> All done"
echo "================================================"
