import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import fitsio
import sys
import os
import easyaccess as ea
import math

if sys.argv[1]=='1':
    event=1
    trigger='GW150914'
    loc = 'upper right'
    print 'event 1'
    master = '/data/des41.b/data/rbutler/sb/bench/PostProcessing/event1_season108/masterlist/MasterExposureList_prelim.fits'
    blissexps = range(651235,651246)
elif sys.argv[1]=='2':
    event=2
    trigger='GW151226'
    loc = 'upper left'
    print 'event 2'
    master = '/data/des41.b/data/rbutler/sb/bench/PostProcessing/event2_7-16-2017/masterlist/MasterExposureList_prelim.fits'
    blissexps = range(658112,658117)
elif sys.argv[1]=='3':
    event=3
    trigger='GW170104'
    loc = 'upper right'
    print 'event 3'
    master = '/data/des41.b/data/rbutler/sb/bench/PostProcessing/MasterList_06_27_17.fits'
    blissexps = range(652979,652990)
else:
    print 'Which event?'
    sys.exit()

mlist = fitsio.read(master)
mlist = mlist.byteswap().newbyteorder()
masdf = pd.DataFrame.from_records(mlist)

#print len(masdf)

bexps = str(tuple(blissexps))

connection = ea.connect('desoper')
cursor = connection.cursor()

query = 'select expnum, radeg as ra, decdeg as dec from exposure where expnum in '+bexps

bdf = connection.query_to_pandas(query)

bdf = bdf.sort_values(by='EXPNUM')
bdf = bdf.reset_index(drop=True)

#print bdf

if event==3:
    masdf = masdf.loc[(masdf['band']=='i') & (masdf['epoch']==4)]
    masdf = masdf.drop_duplicates(['RA','DEC'])
    masdf.ix[masdf['RA'] > 180, 'RA'] = masdf.ix[masdf['RA'] > 180, 'RA'] - 360.
    bdf.ix[bdf['RA'] > 180, 'RA'] = bdf.ix[bdf['RA'] > 180, 'RA'] - 360.
    size=300
else:
    masdf = masdf.loc[masdf['band']=='i']
    masdf = masdf.drop_duplicates(['RA','DEC'])
    size=700
masdf = masdf.sort_values(by='expnum')
masdf = masdf.reset_index(drop=True)

#print masdf
#print len(masdf)

masdfra = [math.radians(x) for x in masdf['RA']]
masdfdec = [math.radians(x) for x in masdf['DEC']]

#fig = plt.figure(figsize=(10,5))
#ax = fig.add_subplot(111,projection="mollweide",axisbg='LightCyan')
#ax.grid(True)
#ax.scatter(masdfra,masdfdec)
#plt.show()

xmax = max([max(masdf['RA']),max(bdf['RA'])])+4
ymax = max([max(masdf['DEC']),max(bdf['DEC'])])+2
xmin = min([min(masdf['RA']),min(bdf['RA'])])-4
ymin = min([min(masdf['DEC']),min(bdf['DEC'])])-2

plt.xlim(xmin,xmax)
plt.ylim(ymin,ymax)

plt.scatter(masdf['RA'],masdf['DEC'],marker='H',c='b',s=size,label='templates previously acquired')
plt.scatter(bdf['RA'],bdf['DEC'],marker='H',c='g',s=size,label='BLISS templates used')
plt.title('BLISS Templates - '+trigger)
plt.xlabel('RA')
plt.ylabel('DEC')
plt.legend(markerscale=0.5,fontsize='medium',loc=loc)
plt.grid(True)
plt.tight_layout()
plt.savefig('BLISS_temps_ev'+str(event)+'.png',dpi=200)

