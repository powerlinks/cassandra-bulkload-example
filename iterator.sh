#!/bin/bash
y="2019"
m="09"
region=$(curl -H "Metadata:true" "http://169.254.169.254/metadata/instance/compute/location?api-version=2017-08-01&format=text")

for d in {25..16}
do
    for h in {00..23}
        do
           bash loader.sh  sync/year=$y/month=$m/day=$d/hour=$h/*.gz $region
        done
done