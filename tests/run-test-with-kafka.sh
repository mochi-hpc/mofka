#!/bin/bash

rm -rf /tmp/kraft-combined-logs

echo "[BASH] Generating cluster ID"
KAFKA_CLUSTER_ID="$(kafka-storage.sh random-uuid)"

echo "[BASH] Copying configuration file"
cp `spack location -i kafka`/config/kraft/server.properties .

echo "[BASH] Formatting log directory"
kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c server.properties

echo "[BASH] Starting kafka server"
kafka-server-start.sh server.properties 1> kafka.$KAFKA_CLUSTER_ID.out 2> kafka.$KAFKA_CLUSTER_ID.err &
KAFKA_PID=$!

sleep 5

echo "[BASH] Running test command: $@"
timeout 100s $@
RET=$?

echo "[BASH] Stopping kafka server"
kafka-server-stop.sh server.properties
wait $KAFKA_PID

echo "[BASH] Erasing data"
rm -rf /tmp/kraft-combined-logs
if [ "$RET" -eq "0" ]; then
    rm kafka.$KAFKA_CLUSTER_ID.out
    rm kafka.$KAFKA_CLUSTER_ID.err
fi

echo "[BASH] Script completed"
exit $RET
