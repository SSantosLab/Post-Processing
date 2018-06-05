import numpy as np
import pandas as pd

proc1 = 767
check1 = 14455
proc2 = 5131
check2 = 10030
proc3 = 2785
check3 = 24957

proctot = proc1+proc2+proc3
checktot = check1+check2+check3

rate = float(proctot)/float(checktot)

print proctot
print checktot
print rate

path = '/data/des41.b/data/rbutler/sb/bench/PostProcessing/event3new/checkoutputs/checkoutputs.csv'
df = pd.read_csv(path)

#print list(df)
df=df.drop(['expnum','successes','fraction'], axis=1)
#print list(df)

#a=[]
#for i in list(df):
#    a.append(df[i].values)

fcodes = list(df.values.flatten())

#print a
print range(1,29)

if 1==1:
    for s in range(29):
        ct = fcodes.count(s)
        print str(s)+' : '+str(ct)
