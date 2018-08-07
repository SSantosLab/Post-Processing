#!/bin/bash

echo "Set ExpLen as OriginalExpLen"
mv ExpLen.txt OriginalExpLen.txt
echo "OriginalExpLen"
originalLen=`cat OriginalExpLen.txt`
python lenExposureInfo.py
echo "New ExpLen"
newLen=`cat ExpLen.txt`
sleep 5s

if [[ $newLen -gt $originalLen ]]
then
    echo "Exposure list has grown"
    lastExp=`cat lastExp.txt`
    python getExposureInfoNFS.py --lastExp
    . getExpWPropIDandNite.sh

    echo "Sleeping for a bit"
    sleep 5s 

    echo "It's alive."
    echo
    
    echo "Running diffimg_setup.sh"
    . diffimg_setup.sh

    echo "diffimg_setup done."
    echo
    
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
	    
	    python run_postproc.py --season $SEASON > output${SEASON}.txt &
	    
	    sleep 45s
	    
	    python run_checker.py --season $SEASON & 
	done
	echo "The deed is done."

    else
	echo "Only one .ini."
	INI=${ARRAY[${0}]}
	echo $INI
	python getSeason.py --ini $INI
	SEASON=`cat getSeason.txt`
	echo 'SEASON = '$SEASON

	python run_postproc.py --season $SEASON > output${SEASON}.txt &
	
	sleep 45s
	
	python run_checker.py --season $SEASON
	
	echo "The deed is done."
    fi
else
    echo "Nothing new to see here."
    return
    ### If you are exectuting this script, change this `return` to and `exit`
fi