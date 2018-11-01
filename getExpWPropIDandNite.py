#!/bin/python 

import argparse
import os

parser = argparse.ArgumentParser()
parser.add_argument("-n", type=str, nargs='+', help="nites")
parser.add_argument("-p", type=str, default="2017B-0110", nargs='+', help="proposition id")
args=parser.parse_args()

nites=args.n
propid=args.p

os.system('''awk '$8=="%s" {print $0}' exposures.list > outfile.txt''' %propid[0])

nights=map(int, nites)
for i in range(len(nights)):
    nite=nights[i]
    os.system('''awk '$2 == "%s" {print $0}' outfile.txt >> curatedExposure.list''' %nite)
