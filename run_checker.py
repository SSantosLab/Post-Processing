import time
import numpy as np
import os
from glob import glob
import checkStatusFunctions
import statusPage
import sys

seaTim=open('seasonTime.txt','r')
seTi=seaTim.readlines()
print(seTi)
seaTim.close()
season=seTi[0].split('\n')[0]
time=seTi[1]
print(season)
print(time)
print('./forcephoto/output/dp'+season+'/'+time+'_DESY'+season)

statusList=checkStatusFunctions.checkStatus('status.txt',season,time)
print(statusList)
statusPage.statusPage(statusList)
