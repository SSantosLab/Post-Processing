#!/bin/bash

if [ ! -f /data/des40.b/data/nsherman/exposures.list ]; then
    echo "No exposure list. Making one!"
    . getExposureInfo.sh    
    sleep 3.5m    
fi

echo "We have an exposure list!"
python lenExposureInfo.py
sleep 5s
if pidof -x "seasonCycler.sh" >/dev/null; then
    echo "seasonCycler is awake."
else
    echo "seasonCycler is asleep."
    echo "Waking seasonCycler"
    . seasonCycler.sh
fi