#!/bin/bash

echo "It's alive."
echo

echo "Running diffimg_setup.sh"
. diffimg_setup.sh

echo "diffimg_setup done."
echo

#Check exposure lists to see if they have grown and update ini files accordingly

ARRAY=($(ls postproc_*.ini)) #list of postproc_SEASON.ini files
echo ${ARRAY[@]}
ELEMENTS=${#ARRAY[@]}
echo $ELEMENTS

if [ $ELEMENTS -gt 1 ];
then
    echo $ELEMENTS
    for ((count=0;count<$ELEMENTS;count++));
    do
	INI=${ARRAY[${count}]}
	echo $INI
	python getSeason.py --ini $INI
	SEASON=`cat getSeason.txt`
	echo 'SEASON = '$SEASON

	export TOPDIR_SNFORCEPHOTO_IMAGES=${ROOTDIR2}/forcephoto/images/dp${SEASON}
	export TOPDIR_SNFORCEPHOTO_OUTPUT=${ROOTDIR2}/forcephoto/output/dp${SEASON}

	echo $TOPDIR_SNFORCEPHOTO_IMAGES
	echo $TOPDIR_SNFORCEPHOTO_OUTPUT

	python run_postproc.py --season $SEASON > output${SEASON}.txt &
	
	sleep 45s
	
	python run_checker.py --season $SEASON 
    done
echo "The deed is done."

else
    echo "Only one .ini."
    INI=${ARRAY[${0}]}
    echo $INI
    python getSeason.py --ini $INI
    SEASON=`cat getSeason.txt`
    echo 'SEASON = '$SEASON

    export TOPDIR_SNFORCEPHOTO_IMAGES=${ROOTDIR2}/forcephoto/images/dp${SEASON}
    export TOPDIR_SNFORCEPHOTO_OUTPUT=${ROOTDIR2}/forcephoto/output/dp${SEASON}

    echo $TOPDIR_SNFORCEPHOTO_IMAGES
    echo $TOPDIR_SNFORCEPHOTO_OUTPUT

    python run_postproc.py --season $SEASON > output${SEASON}.txt &
    
    sleep 45s

    python run_checker.py --season $SEASON

    echo "The deed is done."
fi