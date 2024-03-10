#! /bin/bash
# Copyright 2023 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Print commands and their arguments as they are executed.
set -x
# Exit immediately if a command exits with a non-zero status.
set -e

readonly INTEGRATION_TEST_TIMEOUT=40m
readonly PROJECT_ID="gcs-fuse-test"
readonly BUCKET_LOCATION="us-west1"
#details.txt file contains the release version and commit hash of the current release.
gsutil cp  gs://gcsfuse-release-packages/version-detail/details.txt .
# Writing VM instance name to details.txt (Format: release-test-<os-name>)
curl http://metadata.google.internal/computeMetadata/v1/instance/name -H "Metadata-Flavor: Google" >> details.txt

# Based on the os type(from vm instance name) in detail.txt, run the following commands to add starterscriptuser
if grep -q ubuntu details.txt || grep -q debian details.txt;
then
#  For ubuntu and debian os
    sudo adduser --ingroup google-sudoers --disabled-password --home=/home/starterscriptuser --gecos "" starterscriptuser
else
#  For rhel and centos
    sudo adduser -g google-sudoers --home-dir=/home/starterscriptuser starterscriptuser
fi

# Run the following as starterscriptuser
sudo -u starterscriptuser bash -c '
# Exit immediately if a command exits with a non-zero status.
set -e
# Print commands and their arguments as they are executed.
set -x

#Copy details.txt to starterscriptuser home directory and create logs.txt
cd ~/
cp /details.txt .
touch logs.txt

echo User: $USER &>> ~/logs.txt
echo Current Working Directory: $(pwd)  &>> ~/logs.txt

# Based on the os type in detail.txt, run the following commands for setup

if grep -q ubuntu details.txt || grep -q debian details.txt;
then
#  For Debian and Ubuntu os
    # architecture can be amd64 or arm64
    architecture=$(dpkg --print-architecture)

    sudo apt update

    #Install fuse
    sudo apt install -y fuse

    # download and install gcsfuse deb package
    gsutil cp gs://gcsfuse-release-packages/v$(sed -n 1p details.txt)/gcsfuse_$(sed -n 1p details.txt)_${architecture}.deb .
    sudo dpkg -i gcsfuse_$(sed -n 1p details.txt)_${architecture}.deb |& tee -a ~/logs.txt

    # install wget
    sudo apt install -y wget

    #install git
    sudo apt install -y git

   # install python3-setuptools tools.
   sudo apt-get install -y gcc python3-dev python3-setuptools
   # Downloading composite object requires integrity checking with CRC32c in gsutil.
   # it requires to install crcmod.
   sudo apt install -y python3-crcmod

    #install build-essentials
    sudo apt install -y build-essential
else
#  For rhel and centos
    # uname can be aarch or x86_64
    uname=$(uname -i)

    if [[ $uname == "x86_64" ]]; then
      architecture="amd64"
    elif [[ $uname == "aarch64" ]]; then
      architecture="arm64"
    fi

    sudo yum makecache
    sudo yum -y update

    #Install fuse
    sudo yum -y install fuse

    #download and install gcsfuse rpm package
    gsutil cp gs://gcsfuse-release-packages/v$(sed -n 1p details.txt)/gcsfuse-$(sed -n 1p details.txt)-1.${uname}.rpm .
    sudo yum -y localinstall gcsfuse-$(sed -n 1p details.txt)-1.${uname}.rpm

    #install wget
    sudo yum -y install wget

    #install git
    sudo yum -y install git

    #install Development tools
    sudo yum -y install gcc gcc-c++ make
fi

# install go
wget -O go_tar.tar.gz https://go.dev/dl/go1.21.7.linux-${architecture}.tar.gz
sudo tar -C /usr/local -xzf go_tar.tar.gz
export PATH=${PATH}:/usr/local/go/bin
#Write gcsfuse and go version to log file
gcsfuse --version |& tee -a ~/logs.txt
go version |& tee -a ~/logs.txt

# Clone and checkout gcsfuse repo
export PATH=${PATH}:/usr/local/go/bin
git clone https://github.com/googlecloudplatform/gcsfuse |& tee -a ~/logs.txt
cd gcsfuse

# Installation of crcmod is working through pip only on rhel and centos.
# For debian and ubuntu, we are installing through sudo apt.
if grep -q rhel details.txt || grep -q centos details.txt;
then
    # install python3-setuptools tools and python3-pip
    sudo yum -y install gcc python3-devel python3-setuptools redhat-rpm-config
    sudo yum -y install python3-pip
    # Downloading composite object requires integrity checking with CRC32c in gsutil.
    # it requires to install crcmod.
    pip3 install --require-hashes -r tools/cd_scripts/requirements.txt --user
fi

git checkout $(sed -n 2p ~/details.txt) |& tee -a ~/logs.txt

# Create bucket for integration tests.
function create_bucket() {
  # The length of the random string
  length=5
  # Generate the random string
  random_string=$(tr -dc 'a-z0-9' < /dev/urandom | head -c $length)
  BUCKET_NAME=$bucketPrefix$random_string
  echo 'bucket name = '$BUCKET_NAME
  # We are using gcloud alpha because gcloud storage is giving issues running on Kokoro
  gcloud alpha storage buckets create gs://$BUCKET_NAME --project=$PROJECT_ID --location=$BUCKET_LOCATION --uniform-bucket-level-access
  return
}

#run tests with testbucket flag
function run_non_parallel_tests() {
  for test_dir_np in "${test_dir_non_parallel[@]}"
  do
    test_path_non_parallel="./tools/integration_tests/$test_dir_np"
    # Executing integration tests
    GODEBUG=asyncpreemptoff=1 go test $test_path_non_parallel -p 1 --integrationTest -v --testbucket=$BUCKET_NAME_NON_PARALLEL --testInstalledPackage=true -timeout 40m
    exit_code_non_parallel=$?
    if [ $exit_code_non_parallel != 0 ]; then
      test_fail_np=$exit_code_non_parallel
      echo "test fail in non parallel: " $test_fail_np
    fi
  done
  return $test_fail_np
}

function run_parallel_tests() {
  for test_dir_p in "${test_dir_parallel[@]}"
  do
    test_path_parallel="./tools/integration_tests/$test_dir_p"
    # Executing integration tests
    GODEBUG=asyncpreemptoff=1 go test $test_path_parallel -p 1 --integrationTest -v --testbucket=$BUCKET_NAME_PARALLEL --testInstalledPackage=true -timeout 40m &
    pid=$!  # Store the PID of the background process
    pids+=("$pid")  # Optionally add the PID to an array for later
  done

  # Wait for processes and collect exit codes
  for pid in "${pids[@]}"; do
    wait $pid
    exit_code_parallel=$?
    if [ $exit_code_parallel != 0 ]; then
      test_fail_p=$exit_code_parallel
      echo "test fail in parallel: " $test_fail_p
    fi
  done
  return $test_fail_p
}

# Test setup
# Create Bucket for non parallel e2e tests
# The bucket prefix for the random string
bucketPrefix="gcsfuse-non-parallel-e2e-tests-"
create_bucket
BUCKET_NAME_NON_PARALLEL=$BUCKET_NAME
# Test directory array
test_dir_non_parallel=(
  "explicit_dir"
  "implicit_dir"
  "list_large_dir"
  "operations"
  "read_large_files"
  "readonly"
  "rename_dir_limit"
  "managed_folders"
)

# Create Bucket for parallel e2e tests
# The bucket prefix for the random string
bucketPrefix="gcsfuse-parallel-e2e-tests-"
create_bucket
BUCKET_NAME_PARALLEL=$BUCKET_NAME
# Test directory array
test_dir_parallel=(
  "local_file"
  "log_rotation"
  "mounting"
  "read_cache"
  "gzip"
  "write_large_files"
)


# Run tests
test_fail_p=0
test_fail_np=0
set +e

echo "Running parallel tests..."
# Run parallel tests
run_parallel_tests &
my_process_p=$!
echo "Running non parallel tests..."
# Run non parallel tests
run_non_parallel_tests &
my_process_np=$!
wait $my_process_p
test_fail_p=$?
wait $my_process_np
test_fail_np=$?
set -e

# Cleanup
# Delete bucket after testing.
gcloud alpha storage rm --recursive gs://$BUCKET_NAME_PARALLEL/
gcloud alpha storage rm --recursive gs://$BUCKET_NAME_NON_PARALLEL/

if [ $test_fail_np != 0 ] || [ $test_fail_p != 0 ];
then
    echo "Test failures detected" &>> ~/logs.txt
else
    touch success.txt
    gsutil cp success.txt gs://gcsfuse-release-packages/v$(sed -n 1p ~/details.txt)/$(sed -n 3p ~/details.txt)/
fi

gsutil cp ~/logs.txt gs://gcsfuse-release-packages/v$(sed -n 1p ~/details.txt)/$(sed -n 3p ~/details.txt)/
'