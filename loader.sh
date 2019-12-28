#!/usr/bin/env bash
set -ex

function retry {
  local retries=$1
  shift

  local count=0
  until "$@"; do
    exit=$?
    wait=$((2 ** $count))
    count=$(($count + 1))
    if [ $count -lt $retries ]; then
      echo "Retry $count/$retries exited $exit, retrying in $wait seconds..."
      sleep $wait
    else
      echo "Retry $count/$retries exited $exit, no more retries left."
      return $exit
    fi
  done
  return 0
}

pattern=$1
region=$2

working_dir=$(realpath ".")

mkdir -p ${working_dir}/tmp/data

case $region in
    "eastus")
        account_name="dmpeastus"
        account_key="nuBoCNn3o9NB88kWa2CLWIyKE38SpOOMzbkweazEYnNrfAWrltuYaNZDn5707aa5ED9pQNRUptklPnXA/0I3Jg=="
        cassandra_ip=10.0.1.10
    ;;
    "westeurope")
        account_name="dmpwesteurope"
        account_key="qYBhnNAsc6WNPOvpgEbDAlHIr5UJjeuoPDlmNCuIoQRKMJ7clsjwks5MOKr4KV5tidG/DSJ6UhmBsS8jFxdY0A=="
        cassandra_ip=10.1.1.10
    ;;
esac

start=$(date +"%s%3N")
mkdir -p /data/tmp/sync_logs
az storage blob download-batch --destination /data/tmp/sync_logs --source "dmp-sync-data-container" --pattern "${pattern}" --account-name ${account_name} --account-key ${account_key}
find /data/tmp/sync_logs -type f -exec mv {} ${working_dir}/tmp/data \;
rm -rf /data/tmp/sync_logs
echo "Files downloaded in $(($(date +"%s%3N") - $start))ms"

start=$(date +"%s%3N")
sudo docker-compose up
echo "Processed files in $(($(date +"%s%3N") - $start))ms"

cassandra_passwd="NJ*hpzx]RnzY{2e]"

retry 5 sstableloader -d ${cassandra_ip} -u dmpuser -pw ${cassandra_passwd} ${working_dir}/data/mappings/profiles/
retry 5 sstableloader -d ${cassandra_ip} -u dmpuser -pw ${cassandra_passwd} ${working_dir}/data/mappings/segments_mapping/

rm -rf ${working_dir}/tmp/data/*
rm -rf ${working_dir}/data/mappings/*
