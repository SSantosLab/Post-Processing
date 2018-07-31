#!/bin/bash

while getopts "n:p" opt
do case "${opt}" in
    n)
        nites=${OPTARG};;
    p)
	propid=${OPTARG};;

esac
done

echo $nites

otherEle="$(echo $nites | tr ',' ' ')"

echo $otherEle

ELEMENTS=${#otherEle[@]}

echo $ELEMENTS

awk '$8=="$propid" {print $0}' exposures.list > outfile.txt
for i in $otherEle 
do
    echo $i
done
for ((count=0;count<$ELEMENTS;count++));
do
    nite=${nites[${count}]}
    awk '$2 == "nite" {print $0}' outfile.txt >> curatedExposure.list
done
