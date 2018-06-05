import numpy as np
import re

e = 'execute_ev1.csv'

exps = np.genfromtxt(e,dtype=str,skip_header=1,usecols=(0))

#print exps

new = []

for i in exps:
    a = int(re.sub('[^0-9]','',i))
    new.append(a)

print new

np.savetxt('event1_new.list',new,fmt='%d')
