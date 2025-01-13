#!/bin/bash

rm -rf /tmp/mofka-logs
mkdir /tmp/mofka-logs

timeout 300s $@
RET=$?

rm -rf /tmp/mofka-logs
exit $RET
