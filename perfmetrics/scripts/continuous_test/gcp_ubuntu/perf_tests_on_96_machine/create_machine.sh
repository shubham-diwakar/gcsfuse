GCP_PROJECT="gcs-fuse-test-ml"
# Name of test VM.
VM_NAME="perf-tests-on-96-machine"
# Zone of test VM.
ZONE_NAME='us-central1-a'
RESERVATION="projects/$GCP_PROJECT/reservations/perf-tests-for-benchmark"
TEST_SCRIPT_PATH="github/gcsfuse/perfmetrics/scripts/continuous_test/gcp_ubuntu/perf_tests_on_96_machine/build.sh"

function delete_existing_vm_and_create_new () {
  (
    set +e

    echo "Deleting VM $VM_NAME in zone $ZONE_NAME."
    sudo gcloud compute instances delete $VM_NAME --zone $ZONE_NAME --quiet
    if [ $? -eq 0 ];
    then
      echo "Machine deleted successfully !"
    else
      echo "Machine was not deleted as it doesn't exist."
    fi
  )

  echo "Wait for 30 seconds for old VM to be deleted"
  sleep 30s

  echo "Creating VM $VM_NAME in zone $ZONE_NAME"
  # The below command creates VM using the reservation 'ai-ml-tests'
  sudo gcloud compute instances create $VM_NAME \
          --project=$GCP_PROJECT\
          --zone=$ZONE_NAME \
          --machine-type=n2-standard-96\
          --image-family=ubuntu-2004-lts \
          --image-project=ubuntu-os-cloud \
          --boot-disk-size=100GB \
          --boot-disk-type=pd-ssd \
          --network-interface=network-tier=PREMIUM,nic-type=GVNIC,stack-type=IPV4_ONLY,subnet=default \
          --metadata=enable-osconfig=TRUE,enable-oslogin=true \
          --maintenance-policy=TERMINATE \
          --provisioning-model=STANDARD \
          --scopes=https://www.googleapis.com/auth/cloud-platform \
          --reservation-affinity=specific \
          --reservation=$RESERVATION \
          --metadata=startup-script-url=https://storage.googleapis.com/tulsishah-scripts/build.sh


  echo "Wait for 30 seconds for new VM to be initialised"
  sleep 30s
}

delete_existing_vm_and_create_new

sudo gcloud compute ssh $VM_NAME --zone $ZONE_NAME
bash build.sh