import numpy as np
import pandas as pd
import sys
import math
import os

expfil = 'GW170104_area_rep.txt'

df = pd.read_csv(expfil,sep='\t',header=None,names=['expnum','nite','RA','DEC','band'])

#print df

length = len(df)

areas = []
areas_adj1 = []
areas_adj2 = []

def PolygonSort(corners):
    n = len(corners)
    cx = float(sum(x for x, y in corners)) / n
    cy = float(sum(y for x, y in corners)) / n
    cornersWithAngles = []
    for x, y in corners:
        an = (np.arctan2(y - cy, x - cx) + 2.0 * np.pi) % (2.0 * np.pi)
        cornersWithAngles.append((x, y, an))
    cornersWithAngles.sort(key = lambda tup: tup[2])
    return map(lambda (x, y, an): (x, y), cornersWithAngles)

def PolygonArea(corners):
    n = len(corners)
    area = 0.0
    for i in range(n):
        j = (i + 1) % n
        area += corners[i][0] * corners[j][1]
        area -= corners[j][0] * corners[i][1]
    area = abs(area) / 2.0
    return area

for i in range(len(df)):
    print str(i+1)+'/'+str(length)
    estr = str(df.loc[[i],['expnum']].values[0][0])
    nstr = str(df.loc[[i],['nite']].values[0][0])
    out = '/pnfs/des/persistent/gw/exp'
    out = os.path.join(out,nstr)
    out = os.path.join(out,estr)
    out = os.path.join(out,estr+'.out')
    odf = pd.read_table(out,delim_whitespace=True,header=None,names=['expnum','band','ccd','ra1','dec1','ra2','dec2','ra3','dec3','ra4','dec4'])

    odf = odf.drop_duplicates()
    odf = odf[odf['ccd'] != 31]
    odf = odf.reset_index(drop=True)

    #print odf

    odf.ix[odf.ra1 > 270., 'ra1'] = odf.ix[odf.ra1 > 270., 'ra1'] - 360.
    odf.ix[odf.ra2 > 270., 'ra2'] = odf.ix[odf.ra2 > 270., 'ra2'] - 360.
    odf.ix[odf.ra3 > 270., 'ra3'] = odf.ix[odf.ra3 > 270., 'ra3'] - 360.
    odf.ix[odf.ra4 > 270., 'ra4'] = odf.ix[odf.ra4 > 270., 'ra4'] - 360.

    ras = np.concatenate((odf['ra1'].tolist(),odf['ra2'].tolist(),odf['ra3'].tolist(),odf['ra4'].tolist()),axis=0)
    decs = np.concatenate((odf['dec1'].tolist(),odf['dec2'].tolist(),odf['dec3'].tolist(),odf['dec4'].tolist()),axis=0)

    for c in odf.index.tolist():
        ra = odf.ix[c,['ra1','ra2','ra3','ra4']]
        dec = odf.ix[c,['dec1','dec2','dec3','dec4']]
        cs = zip(ra,dec)
        corners_sorted = PolygonSort(cs)
        area = PolygonArea(corners_sorted)
        areas.append(area)
        area_adj1 = area/abs(math.cos(math.radians(np.mean(dec))))
        areas_adj1.append(area_adj1)
        area_adj2 = area*abs(math.cos(math.radians(np.mean(dec))))
        areas_adj2.append(area_adj2)

print        
print '            100%    81.5%'
print 'standard:   %.1f   %.1f' % (sum(areas),sum(areas)*.815)
print '/ cosine:   %.1f   %.1f' % (sum(areas_adj1),sum(areas_adj1)*.815)
print '* cosine:   %.1f   %.1f' % (sum(areas_adj2),sum(areas_adj2)*.815)
print
