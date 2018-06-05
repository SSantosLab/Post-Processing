import sys
import numpy as np
import pandas as pd
from astropy.table import Table
import matplotlib.pyplot as plt
import matplotlib.cm as cm
from time import time

mjdtrigger = 57757
sn = 106684
band = 'i'

reals = '/data/des41.b/data/rbutler/sb/bench/PostProcessing/out300/makedatafiles/datafiles_combined.fits'

rtable = Table.read(reals)
rdf1 = rtable.to_pandas()
rdf = rdf1.loc[rdf1['PHOTFLAG'].isin([4096,12288])]

time1 = time()
new = rdf[['MJD','FLUXCAL','FLUXCALERR','PHOTPROB']].loc[rdf['SNID']==sn]
time2 = time()

mjd = np.array(new['MJD'].tolist())
flux = np.array(new['FLUXCAL'].tolist())
fluxerr = np.array(new['FLUXCALERR'].tolist())
ml_score = np.array(new['PHOTPROB'].tolist())
time3 = time()

#norm = plt.Normalize()
#colors = plt.cm.jet(norm(ml_score))
#print colors

mag = 27.5-2.5*np.log10(flux)

fig, ax1 = plt.subplots()
ax1.errorbar(mjd-mjdtrigger,flux,yerr=fluxerr,fmt='none',ecolor='k',zorder=0)
ax1.scatter(mjd-mjdtrigger,flux,c=ml_score,edgecolor='',s=40,zorder=1)
ax1.set_xlabel('MJD - MJD(TRIGGER)')
ax1.set_ylabel('FLUX')
#fig.colorbar().set_label('ml_score')

ax2 = ax1.twinx()
ax2.scatter(mjd-mjdtrigger,mag,c='k',marker='*',edgecolor='k',s=40,zorder=2)
ax2.set_ylabel('MAG')
ax2.set_ylim(ax2.get_ylim()[::-1])

plt.tight_layout()
plt.savefig('twintest.png')
plt.clf()

sys.exit()

plt.errorbar(mjd-mjdtrigger,flux,yerr=fluxerr,fmt='none',ecolor='k',zorder=0)
time4 = time()
plt.scatter(mjd-mjdtrigger,flux,c=ml_score,edgecolor='',s=40,zorder=1)
time5 = time()
plt.clim(0,1)
time6 = time()
plt.title('SNID '+str(sn)+' ('+band+')')
time7 = time()
plt.colorbar().set_label('ml_score')
time8 = time()
plt.xlabel('MJD - MJD(TRIGGER)')
plt.ylabel('FLUX')
plt.tight_layout()
plt.savefig('timetest.png')
time9=time()
plt.clf()

print '1-2',time2-time1
print '2-3',time3-time2
print '3-4',time4-time3
print '4-5',time5-time4
print '5-6',time6-time5
print '6-7',time7-time6
print '7-8',time8-time7
print '8-9',time9-time8
print
print 'tot',time9-time1
