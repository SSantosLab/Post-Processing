import numpy as np
import pandas as pd
import fitsio

master = '/data/des41.b/data/rbutler/sb/bench/PostProcessing/event1/masterlist/MasterExposureList_prelim.fits'

mlist = fitsio.read(master)
mlist = mlist.byteswap().newbyteorder()
masdf = pd.DataFrame.from_records(mlist)

three = masdf[['RA','DEC','band']].loc[masdf['epoch']==3]

three = three.sort_values(by='RA')
three = three.drop_duplicates()

print three
print len(three)

filename = 'ra_dec_band_1.txt'

three.to_csv(filename, sep='\t', index=False, header=False)
