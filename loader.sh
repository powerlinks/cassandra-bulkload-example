#!/usr/bin/env bash
set -ex

pattern=$1
region=$2

working_dir=$(realpath ".")

case $region in
    "eastus")
        account_name="dmpeastus"
        account_key="nuBoCNn3o9NB88kWa2CLWIyKE38SpOOMzbkweazEYnNrfAWrltuYaNZDn5707aa5ED9pQNRUptklPnXA/0I3Jg=="
        cassandra_ip=10.0.1.10
    ;;
    "westeurope")
        account_name="dmpwesteurope"
        account_key="qYBhnNAsc6WNPOvpgEbDAlHIr5UJjeuoPDlmNCuIoQRKMJ7clsjwks5MOKr4KV5tidG/DSJ6UhmBsS8jFxdY0A=="
        cassandra_ip=10.1.1.4
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

sstableloader -d ${cassandra_ip} -u dmpuser -pw ${cassandra_passwd} ${working_dir}/data/mappings/profiles/
sstableloader -d ${cassandra_ip} -u dmpuser -pw ${cassandra_passwd} ${working_dir}/data/mappings/segments_mapping/

rm -rf ${working_dir}/tmp/data/*