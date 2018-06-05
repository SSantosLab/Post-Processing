import numpy as np
import re

e = 'execute_ev1.csv'
mine = './getoverlaps/event1exps.list'

exps = np.genfromtxt(e,dtype=str,skip_header=1,usecols=(0))

#print exps

new = []

for i in exps:
    a = int(re.sub('[^0-9]','',i))
    new.append(a)

print new

#np.savetxt('event1_new.list',new,fmt='%d')

list245 = np.genfromtxt(mine,usecols=(0))

list245 = [int(x) for x in list245]

for i in list245:
    if i not in new:
        print i,'in 245 but not 269'

for j in new:
    if j not in list245:
        print j, 'in 269 but not 245'
