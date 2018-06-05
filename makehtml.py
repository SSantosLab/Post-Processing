import os
import sys
from time import time
import math
import subprocess
from glob import glob
import pandas as pd
from collections import OrderedDict as OD
import easyaccess
import numpy as np
#import matplotlib.pyplot as plt
#from matplotlib.ticker import MultipleLocator, FormatStrFormatter
from astropy.io import fits
from astropy.table import Table
import fitsio
import psycopg2
import HTML
import HTMLParser

season = 300

newfits = '/data/des41.b/data/rbutler/sb/bench/PostProcessing/out300/makedatafiles/datafiles_combinedstamps.fits'

html = 'candtest.html'

lcdir = '/data/des41.b/data/rbutler/sb/bench/PostProcessing/out300/plots/lightcurves'

print '1'

if 1==1:
    npcat = fitsio.read(newfits)
    npcat = npcat.byteswap().newbyteorder()
    sdf = pd.DataFrame.from_records(npcat)
    sdf = sdf.loc[sdf['cutflag']==1]

    old_width = pd.get_option('display.max_colwidth')

    for c in sdf['SNID'].unique():
        time1 = time()
        hp = HTMLParser.HTMLParser()

        html = 'cand'+str(c)+'.html'
        f = open(html,'w')
        cdf = sdf.loc[sdf['SNID']==c]
        cdf = cdf.sort_values(by='MJD')

        print c
        f.write("<html>\n")
        f.write("<head>\n<style>\n")
        f.write("table, th, td {\n\tborder: 1px solid black;\n}\n")
        f.write("table#stamps {\n\tborder: none;\n}\n")
        f.write("table#stamps th {\n\tborder: none;\n}\n")
        f.write("table#stamps td {\n\tborder: none;\n}\n")
        f.write("</head>\n</style>\n")
        f.write("<body>\n")
        f.write("<title>SNID "+str(c)+"</title>\n")
        f.write('<h1 align="center">Candidate '+str(c)+'</h1>\n')
        
        ra = np.mean(cdf['RA'].values,dtype=np.float64)
        dec = np.mean(cdf['DEC'].values,dtype=np.float64)
        nites = cdf['NITE'].values
        first = min(nites)
        last = max(nites)
        nnites = len(set(nites))
        
        topdf = pd.DataFrame(columns=['RA','DEC','First Nite','Latest Nite','Season'],index=[0])
        topdf.loc[0] = [ra,dec,first,last,season]

        top = topdf.to_html(index=False,float_format=lambda x: '%.5f' % x)

        top = top.replace('<table','<table align="center" cellpadding="5"')
        top = top.replace('<tr>','<tr style="text-align: center;">')
        top = top.replace('<tr style="text-align: right;">','<tr style="text-align: center;">')
        f.write(top+'\n')

        f.write('<br>\n<hr width="50%" align="center" color="#000000" noshade>\n')
        f.write('<h2 align="center">Detections</h2>\n')
        
        cdf['htmlsrch'] = '<a href="'+cdf['srchstamp'].str[:-4]+'.fits"><img src="'+cdf['srchstamp'].str[:]+'" width=100 height=100 alt="no stamp found">'
        cdf['htmltemp'] = '<a href="'+cdf['tempstamp'].str[:-4]+'.fits"><img src="'+cdf['tempstamp'].str[:]+'" width=100 height=100 alt="no stamp found">'
        cdf['htmldiff'] = '<a href="'+cdf['diffstamp'].str[:-4]+'.fits"><img src="'+cdf['diffstamp'].str[:]+'" width=100 height=100 alt="no stamp found">'

        cdf['MAG'] = 27.5-2.5*np.log10(cdf['FLUXCAL'])
        cdf['SEASON'] = season

        detdf = cdf[['OBJID','MJD','EXPNUM','CCDNUM','BAND','XPIX','YPIX','RA','DEC','MAG','FLUXCAL','NITE','HEX']]
        detdf.columns = ['Object ID','MJD','Exposure','CCD','Band','x','y','RA','DEC','Magnitude','Flux','Nite','Hex']

        det = detdf.to_html(index=False,formatters={'x': lambda x: '%.1f' % x, 'y': lambda x: '%.1f' % x, 'RA': lambda x: '%.5f' % x, 'DEC': lambda x: '%.5f' % x, 'MJD': lambda x: '%.3f' % x, 'Flux': lambda x: '%.1f' % x, 'Magnitude': lambda x: '%.3f' % x})

        det = det.replace('<table border="1"','<table border="1" align="center" cellpadding="5"')
        det = det.replace(' class="dataframe"','')
        det = det.replace('<tr>','<tr style="text-align: center;">')
        det = det.replace('<tr style="text-align: right;">','<tr style="text-align: center;">')

        det = hp.unescape(det)

        f.write(det+'\n')

        f.write('<br>\n<hr width="50%" align="center" color="#000000" noshade>\n')
        f.write('<h2 align="center">Stamps</h2>\n')

        stpdf = cdf[['OBJID','BAND','NITE','MJD','htmlsrch','htmltemp','htmldiff','PHOTPROB']]
        stpdf.columns = ['OBJID','BAND','NITE','MJD','search','template','difference','ml_score']
        pd.set_option('display.max_colwidth', -1)
        stp = stpdf.to_html(index=False,float_format=lambda x: '%.3f' % x)

        stp = stp.replace('<table border="1"','<table id="stamps" align="center" cellpadding="5"')
        stp = stp.replace('<tr>','<tr style="text-align: center;">')
        stp = stp.replace('<tr style="text-align: right;">','<tr style="text-align: center;">')

        stp = hp.unescape(stp)

        f.write(stp+'\n')

        f.write('<br>\n<hr width="50%" align="center" color="#000000" noshade>\n')
        f.write('<h2 align="center">Light Curve</h2>\n')

        lc = os.path.join(lcdir,'SNID'+str(c)+'.png')

        f.write('<p style="text-align:center;"><img src="'+lc+'"></p>\n')        
        
        f.write("\n</body>\n</html>")
        f.close()
        pd.set_option('display.max_colwidth', old_width)
        time2=time()
        print time2-time1

        sys.exit()
