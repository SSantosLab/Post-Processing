import time
import numpy as np
import os
from glob import glob
import checkStatusFunctions
import statusPage
import sys
import argparse


parser = argparse.ArgumentParser(description=__doc__,formatter_class=argparse.RawDescriptionHelpFormatter)
parser.add_argument('--season', help='Season number', type=int)
args = parser.parse_args()
season=str(args.season)


seaTim=open('seasonTime'+season+'.txt','r')
seTi=seaTim.readlines()
print(seTi)
seaTim.close()
season=seTi[0].split('\n')[0]
time=seTi[1]
print(season)
print(time)
print('./forcephoto/output/dp'+season+'/'+time+'_DESY'+season)

statusList=checkStatusFunctions.checkStatus('status'+season+'.txt',season,time)
print(statusList)
statusPage.statusPage(statusList,season)
