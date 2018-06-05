import numpy as np
import pandas as pd
from astropy.io import fits
from astropy.table import Table
import sys

combo = '/data/des41.b/data/rbutler/sb/bench/PostProcessing/out300/makedatafiles/datafiles_combined.fits'

rtable = Table.read(combo)
rdf1 = rtable.to_pandas()

a = rdf1[['RA','DEC']].loc[rdf1['EXPNUM']==606582]
print a.sort_values(by='RA')

sys.exit()

master = '/data/des41.b/data/rbutler/sb/bench/PostProcessing/out300/masterlist/MasterExposureList.fits'

mlist = Table.read(master)
masdf = mlist.to_pandas()

for h in masdf['fullhex'].unique():
    exepteff = masdf[['expnum','epoch','t_eff']].loc[masdf['fullhex'] == h]
    cut = exepteff[['expnum','epoch','t_eff']].loc[exepteff['epoch']==1]
    if len(cut)>1:
        cut = cut.loc[cut['t_eff'] == cut['t_eff'].ix[cut['t_eff'].idxmax()]]
    cut['expnum'].values[0]
