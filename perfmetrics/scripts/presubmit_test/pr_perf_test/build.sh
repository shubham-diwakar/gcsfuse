#!/bin/bash
# It will take approx 80 minutes to run the script.
set -e
df -H
sudo apt-get update
echo running integration tests
GODEBUG=asyncpreemptoff=1 go test ./tools/integration_tests/...  -p 1  --integrationTest -v --testbucket=integration-test-tulsishah-2 -timeout 15m
