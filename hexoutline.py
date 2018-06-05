import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import sys
import math
from collections import OrderedDict as OD

out = '/pnfs/des/persistent/gw/exp/20170105/606889/606889.out'

odf = pd.read_table(out,delim_whitespace=True,header=None,names=['expnum','band','ccd','ra1','dec1','ra2','dec2','ra3','dec3','ra4','dec4'])

odf = odf.drop_duplicates()
odf = odf.reset_index(drop=True)

odf.ix[odf.ra1 > 270., 'ra1'] = odf.ix[odf.ra1 > 270., 'ra1'] - 360.
odf.ix[odf.ra2 > 270., 'ra2'] = odf.ix[odf.ra2 > 270., 'ra2'] - 360.
odf.ix[odf.ra3 > 270., 'ra3'] = odf.ix[odf.ra3 > 270., 'ra3'] - 360.
odf.ix[odf.ra4 > 270., 'ra4'] = odf.ix[odf.ra4 > 270., 'ra4'] - 360.

ras = np.concatenate((odf['ra1'].tolist(),odf['ra2'].tolist(),odf['ra3'].tolist(),odf['ra4'].tolist()),axis=0)

decs = np.concatenate((odf['dec1'].tolist(),odf['dec2'].tolist(),odf['dec3'].tolist(),odf['dec4'].tolist()),axis=0)

ccdix = odf[odf['ccd'].isin([1,5,6,3,7,12,18,24,31,38,44,50,55,59,62,58,57,60,56,51,45,39,32,25,19,13,8,4])].index.tolist()

allc = []

ccdix = odf[odf['ccd'].isin([7,12,18,24,31,38,44,50,55,59])].index.tolist()
for i in ccdix:
    ra = odf.ix[i,['ra1','ra2','ra3','ra4']]
    dec = odf.ix[i,['dec1','dec2','dec3','dec4']]
    cs = zip(ra,dec)
    cent=(sum([c[0] for c in cs])/len(cs),sum([c[1] for c in cs])/len(cs))
    cs.sort(key=lambda c: math.atan2(c[1]-cent[1],c[0]-cent[0]))
    allc.append(cs[1])
    allc.append(cs[2])

ccdix = odf[odf['ccd'].isin([56,51,45,39,32,25,19,13,8,4])].index.tolist()
for i in ccdix:
    ra = odf.ix[i,['ra1','ra2','ra3','ra4']]
    dec = odf.ix[i,['dec1','dec2','dec3','dec4']]
    cs = zip(ra,dec)
    cent=(sum([c[0] for c in cs])/len(cs),sum([c[1] for c in cs])/len(cs))
    cs.sort(key=lambda c: math.atan2(c[1]-cent[1],c[0]-cent[0]))
    allc.append(cs[0])
    allc.append(cs[3])

#ccdix = odf[odf['ccd'].isin([1,3,62,60])].index.tolist()
#for i in ccdix:
#    ra = odf.ix[i,['ra1','ra2','ra3','ra4']]
#    dec = odf.ix[i,['dec1','dec2','dec3','dec4']]
#    cs = zip(ra,dec)
#    cent=(sum([c[0] for c in cs])/len(cs),sum([c[1] for c in cs])/len(cs))
#    cs.sort(key=lambda c: math.atan2(c[1]-cent[1],c[0]-cent[0]))
#    allc.append(cs[0])
#    allc.append(cs[1])
#    allc.append(cs[2])
#    allc.append(cs[3])

ccdix = odf[odf['ccd'].isin([1])].index.tolist()
for i in ccdix:
    ra = odf.ix[i,['ra1','ra2','ra3','ra4']]
    dec = odf.ix[i,['dec1','dec2','dec3','dec4']]
    cs = zip(ra,dec)
    cent=(sum([c[0] for c in cs])/len(cs),sum([c[1] for c in cs])/len(cs))
    cs.sort(key=lambda c: math.atan2(c[1]-cent[1],c[0]-cent[0]))
    allc.append(cs[0])
    allc.append(cs[2])
    allc.append(cs[3])

ccdix = odf[odf['ccd'].isin([3])].index.tolist()
for i in ccdix:
    ra = odf.ix[i,['ra1','ra2','ra3','ra4']]
    dec = odf.ix[i,['dec1','dec2','dec3','dec4']]
    cs = zip(ra,dec)
    cent=(sum([c[0] for c in cs])/len(cs),sum([c[1] for c in cs])/len(cs))
    cs.sort(key=lambda c: math.atan2(c[1]-cent[1],c[0]-cent[0]))
    allc.append(cs[1])
    allc.append(cs[2])
    allc.append(cs[3])

ccdix = odf[odf['ccd'].isin([62])].index.tolist()
for i in ccdix:
    ra = odf.ix[i,['ra1','ra2','ra3','ra4']]
    dec = odf.ix[i,['dec1','dec2','dec3','dec4']]
    cs = zip(ra,dec)
    cent=(sum([c[0] for c in cs])/len(cs),sum([c[1] for c in cs])/len(cs))
    cs.sort(key=lambda c: math.atan2(c[1]-cent[1],c[0]-cent[0]))
    allc.append(cs[0])
    allc.append(cs[1])
    allc.append(cs[2])

ccdix = odf[odf['ccd'].isin([60])].index.tolist()
for i in ccdix:
    ra = odf.ix[i,['ra1','ra2','ra3','ra4']]
    dec = odf.ix[i,['dec1','dec2','dec3','dec4']]
    cs = zip(ra,dec)
    cent=(sum([c[0] for c in cs])/len(cs),sum([c[1] for c in cs])/len(cs))
    cs.sort(key=lambda c: math.atan2(c[1]-cent[1],c[0]-cent[0]))
    allc.append(cs[0])
    allc.append(cs[1])
    allc.append(cs[3])

#sys.exit()

#for i in ccdix:
#    for j in [1,2,3,4]:
#        for o in odf.ix[i,['ccd']]:
#            occd.append(o)
#    for x in odf.ix[i,['ra1','ra2','ra3','ra4']]:
#        ora.append(x)
#    for y in odf.ix[i,['dec1','dec2','dec3','dec4']]:
#        odec.append(y)

#dd = OD()
#dd['ccd'] = occd
#dd['ra'] = ora
#dd['dec'] = odec
#cdf = pd.DataFrame(dd)
#print cdf

#cs = zip(dd['ra'],dd['dec'])

cs = allc
cent=(sum([c[0] for c in cs])/len(cs),sum([c[1] for c in cs])/len(cs))
cs.sort(key=lambda c: math.atan2(c[1]-cent[1],c[0]-cent[0]))
plt.plot([c[0] for c in cs],[c[1] for c in cs],ls=':',lw=0.5,c='k')

plt.xlim(min(ras)-0.2,max(ras)+0.2)
plt.ylim(min(decs)-0.2,max(decs)+0.2)
#plt.show()

#sys.exit()

for i in chips:
    chip = str(odf.ix[i,'ccd'])
    midra = (max(ra)+min(ra))/2.
    middec = (max(dec)+min(dec))/2.
    middle = tuple([midra,middec])
    cs = zip(ra,dec)
    cent=(sum([c[0] for c in cs])/len(cs),sum([c[1] for c in cs])/len(cs))
    cs.sort(key=lambda c: math.atan2(c[1]-cent[1],c[0]-cent[0]))
    cs.append(cs[0])
    plt.plot([c[0] for c in cs],[c[1] for c in cs],ls=':',lw=0.5,c='k')
    plt.annotate(chip, xy=middle, ha='center',va='center',family='sans-serif',fontsize=12,alpha=0.4)

plt.xlim(min(ras)-0.2,max(ras)+0.2)
plt.ylim(min(decs)-0.2,max(decs)+0.2)
plt.show()
