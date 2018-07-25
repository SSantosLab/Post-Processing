#!/bin/bash

if pidof -x "seasonCycler.sh" >/dev/null; then
    echo "seasonCycler is awake."
else
    echo "seasonCycler is asleep."
    echo "Waking seasonCycler"
    . seasonCycler.sh
fi