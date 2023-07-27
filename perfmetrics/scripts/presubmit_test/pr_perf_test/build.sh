#!/bin/bash
# Running test only for when PR contains execute-perf-test label
# It will take approx 80 minutes to run the script.
set -e
echo gcloud version
pip install google-cloud-storage==402.0.0
gcloud version
sudo apt-get update
echo Installing git
sudo apt-get install git
echo Installing python3-pip
sudo apt-get -y install python3-pip
echo Installing libraries to run python script
pip install google-cloud
pip install google-cloud-vision
pip install google-api-python-client
pip install prettytable
echo Installing go-lang  1.20.5
wget -O go_tar.tar.gz https://go.dev/dl/go1.20.5.linux-amd64.tar.gz
sudo rm -rf /usr/local/go && tar -xzf go_tar.tar.gz && sudo mv go /usr/local
export PATH=$PATH:/usr/local/go/bin
echo Installing fio
sudo apt-get install fio -y

# Run on master branch
cd "${KOKORO_ARTIFACTS_DIR}/github/gcsfuse"
GODEBUG=asyncpreemptoff=1 go test ./tools/integration_tests/...  -p 1  --integrationTest -v --testbucket=integration-test-tulsishah-2 -timeout 15m
