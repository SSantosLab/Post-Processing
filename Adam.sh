#!/bin/bash

echo "It's alive."
echo

echo "Running diffimg_setup.sh"
. diffimg_setup.sh

echo "diffimg_setup done."
echo

python run_postproc.py &

sleep 45s

python run_checker.py 

echo "The deed is done."