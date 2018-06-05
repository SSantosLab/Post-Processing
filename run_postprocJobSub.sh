#!/bin/bash


reed -p "Please provide the location of the files necessary to run this script. " location
cd $location
source  /cvmfs/des.opensciencegrid.org/eeups/startupcachejob21i.sh
. diffimg_setup.sh
time python run_postproc.py | tee outputfromJobSub.txt 2>stefromJobSub.txt



exit 0 