import numpy as np
import csv
from astropy.io import fits 
import sys
import os
import pandas as pd
import psycopg2

filename = 'new_allexp_pdMasterExposureList.fits'
expnums = None
#hdulist = fits.open('/data/des41.b/data/rbutler/sb/bench/PostProcessing/MasterExposureList4Epochs_new4.fits')
#tbdata = hdulist[1].data
#e1,e2,e3,e4 = tbdata.field(3),tbdata.field(6),tbdata.field(9),tbdata.field(12)
#expnums = np.concatenate((e1,e2,e3,e4),axis=0)
#expnums = list(expnums)
#del expnums[338]
#expnums = [606588,606845]
seqid = "'GW170104'"

if os.path.isfile('blacklist.txt'):
    blacklist = list(np.genfromtxt('blacklist.txt',usecols=(0),unpack=True))
else:
    blacklist = []

if expnums:
    query_exp = """select id as expnum, ra, declination as dec, filter, exptime, airmass, seeing, qc_teff, seqnum, program, object as hex, EXTRACT(EPOCH FROM date - '1858-11-17T00:00:00Z')/(24*60*60) as mjd, TO_CHAR(date - '12 hours'::INTERVAL, 'YYYYMMDD') AS nite 
from exposure 
where propid='2016B-0124' and ra is not null 
and id IN """+str(tuple(expnums))+""" order by id"""
    query_count = """select * from (
WITH objnights AS (
SELECT obstac.nightmjd(date), object, ra, declination
FROM exposure.exposure
WHERE delivered
      AND propid='2016B-0124'
      AND seqid="""+seqid+"""
      AND id IN """+str(tuple(expnums))+"""
GROUP BY obstac.nightmjd(date), object,ra,declination
)
SELECT COUNT(*), ra, declination as dec, object as hex
FROM objnights
GROUP BY object,ra,declination
) as foo order by ra"""    

else:
    query_exp = """select id as expnum, ra, declination as dec, filter, exptime, airmass, seeing, qc_teff, seqnum, program, object as hex, EXTRACT(EPOCH FROM date - '1858-11-17T00:00:00Z')/(24*60*60) as mjd, TO_CHAR(date - '12 hours'::INTERVAL, 'YYYYMMDD') AS nite 
from exposure 
where propid='2016B-0124' and ra is not null 
order by id"""
    query_count = """select * from (
WITH objnights AS (
SELECT obstac.nightmjd(date), object, ra, declination
FROM exposure.exposure
WHERE delivered
      AND propid='2016B-0124'
      AND seqid="""+seqid+"""
GROUP BY obstac.nightmjd(date), object, ra,declination
)
SELECT COUNT(*), ra, declination as dec, object as hex
FROM objnights
GROUP BY object,ra,declination
) as foo order by ra"""

conn =  psycopg2.connect(database='decam_prd',
                           user='decam_reader',
                           host='des20.fnal.gov',
#                           password='THEPASSWORD',
                         port=5443) 

print query_exp
#print
#print query_count

expdf = pd.read_sql(query_exp,conn)

#ctdf = pd.read_sql(query_count,conn)

conn.close()

print
print list(expdf)

expdf = expdf.loc[~expdf['expnum'].isin(blacklist)]

expdf = expdf.sort_values(by=['ra','mjd'])

expdf['dup'] = expdf.duplicated(subset=['ra','nite'])
#print expdf['dup']
print expdf['expnum'].loc[expdf['dup']==True]
#sys.exit()

epoch = []

noDupes = []
[noDupes.append(i) for i in expdf['ra'] if not noDupes.count(i)]
for x in noDupes:
    ep = 0
    for y in range(len(expdf['ra'])):
        if x==expdf['ra'][y] and expdf['dup'][y]==True:
            ep = ep
            epoch.append(ep)
        elif x==expdf['ra'][y]:
            ep = ep + 1
            epoch.append(ep)

### strip hex string to just ra/dec term
striphex = []
for ihex in expdf['hex']:
    a = ihex
    a = a.split('hex')
    a = a[1].split('tiling')
    new = a[0].strip()
    striphex.append(new)

niteform = lambda x: int(x)
expdf['nite'] = expdf['nite'].map(niteform)

mjdform = lambda x: round(x,3)
expdf['mjd'] = expdf['mjd'].map(mjdform)

expdf['epoch'] = epoch

expdf['striphex'] = striphex

tbhdu1 = fits.BinTableHDU.from_columns(
[fits.Column(name='fullhex', format='A69', array=expdf['hex']),
fits.Column(name='hex', format='A8', array=expdf['striphex']),
fits.Column(name='epoch', format='K', array=expdf['epoch']),
fits.Column(name='expnum', format='K', array=expdf['expnum']),
fits.Column(name='RA', format='E', array=expdf['ra']),
fits.Column(name='DEC', format='E', array=expdf['dec']),
fits.Column(name='nite', format='K', array=expdf['nite']),
fits.Column(name='mjd', format='E', array=expdf['mjd']),
fits.Column(name='t_eff', format='E', array=expdf['qc_teff']),
])

tbhdu1.writeto(filename,clobber=True)

