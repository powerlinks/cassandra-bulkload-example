#!/bin/bash
y="2019"
m="09"

for d in {25..16}
do
    for h in {00..23}
        do
           bash loader.sh  sync/year=$y/month=$m/day=$d/hour=$h/*.gz
        done
done