#!/usr/bin/env bash

echo "================================================"
echo "After test"
echo "================================================"

echo "==> Shutting down server"
BEDROCK_PID=$(<mofka.pid)

kill $BEDROCK_PID

sleep 1
rm mofka.json mofka.pid

echo "==> Removing log directory"
rm -rf /tmp/mofka-logs

echo "==> All done"
echo "================================================"
