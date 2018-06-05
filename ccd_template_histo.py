import numpy as np
from astropy.io import fits
import sys
from glob import glob
from collections import OrderedDict as OD
import itertools

master = fits.open('MasterExposureList4Epochs_new4.fits')
tb = master[1].data
exps1 = tb.field(3)
exps2 = tb.field(6)
exps3 = tb.field(9)
exps4 = tb.field(12)
exps = list(itertools.chain(exps1,exps2,exps3,exps4))
#exps = [606868]

ccds = OD()
for i in range(1,63):
    ccds[str(i)]=[]
#print ccds

length = len(exps)
prog = 0

for e in exps:
    prog = prog+1
    print e,':',str(prog)+'/'+str(length)
    path = '/pnfs/des/persistent/gw/exp/201701*/'+str(e)+'/dp300/*_'
    #print e
    #print glob(path+'.numList')
    for c in range(1,63):
        ccd = str("%02d" % (c))
        ccdpath = path+str(ccd)+'/'+'WSTemplate_'+str(e)+'*_i_'+str(ccd)+'.numList'
        #print ccdpath
        numlist = glob(ccdpath)
        if len(numlist)==0:
            continue
        if len(numlist)==1:
            f = open(numlist[0],'r')
            lines = f.readlines()
            f.close()
            ccds[str(c)].append(len(lines))
#print ccds

w = open('ccdtemplate_all.txt','w+')
for key in ccds:
    w.write(key+'\t'),w.write(str(sum(ccds[key]))+'\n')
w.close()
