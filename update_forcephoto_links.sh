#!/bin/bash

if [ -z "$SEASON" ]; then
    echo "Error: SEASON not set." ; exit 1;
fi
if [ -z "$ROOTDIR" ]; then
    echo "Error: ROOTDIR not set." ; exit 1;
fi
if [ -z "$TOPDIR_SNFORCEPHOTO_IMAGES" ]; then
    echo "Error: TOPDIR_SNFORCEPHOTO_IMAGES not set." ; exit 1;
fi

NITE=""

while getopts "n:" opt $ARGS
do case $opt in
   n)
            [[ $OPTARG =~ ^[0-9]+$ ]] || { echo "Error: Night must be an integer! You put $OPTARG" ; exit 1; }
            NITE=$OPTARG #TODO export?                                                                                                                                                
            shift 2
            ;;
esac
done

for nite in $(ls $ROOTDIR/forcephoto/images/dp${SEASON})
do
    if [ -z "${NITE}" ] || [ $nite -eq $NITE ] ; then
	if [ ! -d ${TOPDIR_SNFORCEPHOTO_IMAGES}/$(basename $nite) ] ; then mkdir ${TOPDIR_SNFORCEPHOTO_IMAGES}/$(basename $nite) ; fi
	for exp in $(ls $ROOTDIR/forcephoto/images/dp${SEASON}/${nite})
	do
	    ln -sf $ROOTDIR/forcephoto/images/dp${SEASON}/${nite}/${exp}/*.fits $ROOTDIR/forcephoto/images/dp${SEASON}/${nite}/${exp}/*.psf ${TOPDIR_SNFORCEPHOTO_IMAGES}/$(basename $nite)/
	done
    fi
done
