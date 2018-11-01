#!/bin/bash

echo "Set ExpLen as OriginalExpLen"
mv ExpLen.txt OriginalExpLen.txt

echo "OriginalExpLen is"
originalLen=`cat OriginalExpLen.txt`
echo $originalLen

python lenExposureInfo.py
echo "New ExpLen is"
newLen=`cat ExpLen.txt`
echo $newLen
sleep 5s

if [[ $newLen -gt $originalLen ]]
then
    echo "Exposure list has grown"
    lastExposure=`cat lastExp.txt`
    python getExposureInfoNFS.py --lastExp $lastExposure

    ARRAY=(postproc_*.ini) #list of postproc_SEASON.ini files
    for INIFILE in ${ARRAY[@]}
    do
	PROPID="$(awk -F "=" '/^propid/{print $NF}' $INIFILE)"
	today=`date +%Y%m%d`
	yesterday=`date -d "yesterday 13:00" +%Y%m%d`
	python getExpWPropIDandNite.py -n $today $yesterday -p $PROPID
    done

    echo "Sleeping for a bit"
    sleep 5s 

    echo "Running diffimg_setup.sh"
    . diffimg_setup.sh

    echo "diffimg_setup done."
    echo
    	
    echo "available .ini files: "${ARRAY[@]}
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
	    
	    #update exposure list in config file
	    python changeEXPlist.py --season $SEASON --expList curatedExposure.list 

	    python run_postproc.py --season $SEASON > output${SEASON}.txt &
	    
	    sleep 45s
	    
	    python run_checker.py --season $SEASON & 
	done
	echo "The deed is done."

    else
	echo "Only one .ini."
	INI=${ARRAY[0]}
	echo $INI
	python getSeason.py --ini $INI
	SEASON=`cat getSeason.txt`
	echo 'SEASON = '$SEASON

	python run_postproc.py --season $SEASON > output${SEASON}.txt &
	
	sleep 45s
	
	python run_checker.py --season $SEASON
	
	echo "The deed is done."
    fi

#    for INIFILE in ${ARRAY[@]}
#    do
#	OUTDIR="$(awk '$1=="outdir" {print $3}' $INI)"
#	mv $INIFILE $OUTDIR
#	echo "Moved "$INIFILE" to "$OUTDIR
#    done

else
    echo "Nothing new to see here."
    #return
    #exit
    ### If you are exectuting this script, change this `return` to and `exit`
fi