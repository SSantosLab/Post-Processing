import numpy as np
#import pandas as pd
from glob import glob
import os
import sys
from collections import Counter

base = '/pnfs/des/persistent/gw/exp/*'

exps=[498267]
season=208
bad=[]

for e in exps:
    print e
    path = os.path.join(base,str(e))
    path = os.path.join(path,'dp'+str(season))
    for c in range(1,63):
        ccd = '%02d' % c
        globobj = glob(path+'/*'+ccd+'/WS*filterObj.out')
        #print path+'/*'+ccd+'/WS*filterObj.out'
        #print globobj
        #globfail = glob(path+'/*'+ccd+'/RUN03_expose_mkWStemplate.FAIL') 
        if len(globobj)==1:# and len(globfail)==1:
            f = open(globobj[0],'r')
            lines = f.readlines()
            f.close()
            #print lines[-1]
            if 'NOBJ_FAKEMATCH:' in lines[-1]:
                bad.append(int(lines[-1].split(' ')[1]))
                #bad.append(lines[-12])
            
print
print '---'
print
counts = Counter(bad)

print bad

sys.exit()

dcounts = dict(counts)
#print dcounts

for i in dcounts:
    print i,':',dcounts[i]

total = len(bad)
print 
print 'total :',total 
