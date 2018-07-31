#!/bin/bash

source /cvmfs/des.opensciencegrid.org/eeups/startupcachejob21i.sh

setup easyaccess
setup psycopg2
python getExposureInfo.py

for band in u g r i z Y
do
awk '($6 == "'$band'")' exposures.list > exposures_${band}.list
done

#exit