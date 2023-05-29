#!/bin/bash

# Move to workdir
WORK_DIR="$(cd "$(dirname $0)"; pwd)"
cd "$WORK_DIR"

# Create venv
echo "## Create venv."
# python -m venv env
# source env/bin/activate

# Install apache-beam
echo "## Install apache-beam[gcp]."

sudo ACCEPT_EULA=Y apt-get install -y  msodbcsql17
sudo apt-get update

pip install --upgrade pip
pip install numpy 
pip install apache-beam[gcp]
pip install pyodbc
# Run pipeline
echo "## Run pipeline."
PROJECT=$(gcloud config get-value project)
REGION=us-east4

python -m conetsql --runner=DataflowRunner \
  --project pe-fesa-datalake-dev01 \
  --region=$REGION \
  --job_name=conectiontosql \
  --staging_location gs://pe-fe-rw-dv/test/staging \
  --temp_location gs://pe-fe-rw-dv/test/tmp \
  --db_host 172.32.1.22 \
  --db_name  fcsalmseusdbdev01 \
  --db_user  usrbireaderdev@fcsalmseussrvdev01 \
  --db_password OoPho0xuOophu6xe \
  --requirements_file requirements.txt \
  --setup_file=./setup.py \
  --output_table  raw \
  --sdk_container  gokuni/dataflow:tag \
  --service_account 394717392014-compute@developer.gserviceaccount.com \
  --subnetwork=https://www.googleapis.com/compute/v1/projects/pe-ferreyros-sharedvpc-dev-01/regions/us-east4/subnetworks/snetuse4dev01  \
  --network pe-ferreyros-sharedvpc-dev-01  \
  --template_location gs://pe-fe-rw-dv/test/templates 