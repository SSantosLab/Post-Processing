import numpy as np
import os

ldir = '/data/des41.b/data/rbutler/clean/diffimg-proc/logs'

files = os.listdir(ldir)

logs = []

for f in files:
    if 'jobsub' in f:
        logs.append(f)

#print logs

notsub = []

for l in logs:
    f = open(os.path.join(ldir,l),'r')
    lines = f.readlines()
    f.close()
    for i in lines:
        if '(Failed)' in i:
            notsub.append(l)

print notsub
        
