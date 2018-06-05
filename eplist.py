import numpy as np
import pandas as pd
import fitsio

master = '/data/des41.b/data/rbutler/sb/bench/PostProcessing/makedatatest/masterlist/MasterExposureList.fits'

mlist = fitsio.read(master)
mlist = mlist.byteswap().newbyteorder()
masdf = pd.DataFrame.from_records(mlist)

three = masdf[['expnum','nite','RA','DEC','band']].loc[masdf['epoch']==3]

three = three.sort_values(by='RA')
three = three.drop_duplicates()

print three
print len(three)

filename = 'GW170104_area_rep.txt'

three.to_csv(filename, sep='\t', index=False, header=False)
