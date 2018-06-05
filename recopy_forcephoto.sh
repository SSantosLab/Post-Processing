#!/bin/bash


if [ $# -lt 1 ]; then

    echo "no list specified"
    exit 1
fi

if [ ! -f $1 ]; then
    echo "file list does not exist"
    exit 1
fi

while read filename
do
    fname=$(basename $filename)
    fname_orig=$fname
    if [[ $fname =~ fits$ ]] ; then fname=${fname}.fz ; fi
    nite=$(echo $filename | sed -r -e "s/.*\/([0-9]{8})\/.*/\1/")
    expnum=$(echo $fname | sed -r -e "s/.*WS$(printf %04d ${SEASON})_([0-9]{6})_.*/\1/")
    band=$(echo $fname | sed -r -e "s/.*combined_([griz])_.*/\1/")
    ccdnum=$(echo $fname | sed -r -e "s/.*combined_[griz]_([0-9]{2}).*/\1/")
    tarname="/pnfs/des/persistent/gw/exp/${nite}/${expnum}/dp${SEASON}/${band}_${ccdnum}/outputs_dp${SEASON}_${nite}_${expnum}_${band}_${ccdnum}.tar.gz"
    dirpath="/pnfs/des/persistent/gw/exp/${nite}/${expnum}/dp${SEASON}/${band}_${ccdnum}"
    outdir="/pnfs/des/persistent/gw/forcephoto/images/dp${SEASON}/${nite}/${expnum}"
    if [ -f ${dirpath}/${fname} ]; then
	cp  ${dirpath}/${fname} .
	if [[ ${fname} =~ fz$ ]]; then
	    funpack -D $fname
	fi
	if [ ! -f ${outdir}/$fname_orig ]; then ifdh cp -D $fname_orig $outdir && rm $fname_orig ; else rm $fname_orig ; fi
    elif [ -f $tarname ]; then
	tar tzf $tarname $fname && tar xzmf $tarname $fname
	if [ $? -eq 0 ]; then
	    if [[ ${fname} =~ fz$ ]]; then
		funpack -D $fname
	    fi
	    if [ ! -f ${outdir}/$fname_orig ]; then ifdh cp -D $fname_orig $outdir && rm $fname_orig ; else rm $fname_orig ; fi 
	fi
    else
	# it really did fail
	echo "$nite $expnum $band $ccdnum" | sed -e "s/\ 0/\ /"
    fi

done < $1