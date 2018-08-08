import os
import shutil
import tarfile
import sys
import time
import math
import subprocess
from glob import glob
import pandas as pd
from collections import OrderedDict as OD
import easyaccess
import ConfigParser
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import MultipleLocator, FormatStrFormatter
from astropy.io import fits
from astropy.table import Table
import fitsio
import psycopg2
import fnmatch
import configparser

def MakeDaPlots(season,ccddf,master,truthplus,fitsname,expnums,mjdtrigger,ml_score_cut=0.,skip=False):

    config = ConfigParser.ConfigParser()
    if os.path.isfile('./postproc_'+str(season)+'.ini'):
        inifile = config.read('./postproc_'+str(season)+'.ini')[0]
    outdir = config.get('general','outdir')

    plt.clf()

    statMLS=0
    statRADEC=0
    
    #season = os.environ.get('SEASON')
    season = str(season)

    rootdir = os.environ.get('ROOTDIR')
    rootdir = os.path.join(rootdir,'exp')

### get data                                                                   
    if os.path.isfile(master):                       
        mlist = fitsio.read(master)
        mlist = mlist.byteswap().newbyteorder()
        masdf = pd.DataFrame.from_records(mlist)
    else:
        skip = True
        print "No master list found with filename",master+'.'
        print "Plots requiring a master list (SNR, RA/DEC hex maps) will not be created."

    df1 = truthplus

    outdir = os.path.join(os.environ.get('ROOTDIR2'),'plots')
    if not os.path.isdir(outdir):
        os.mkdir(outdir)

    rtable = fitsio.read(fitsname)
    rtable = rtable.byteswap().newbyteorder()
    rdf1 = pd.DataFrame.from_records(rtable)
    df = df1.loc[df1['REJECT'] == 0]
    rdf = rdf1.loc[rdf1['PHOTFLAG'].isin([4096,12288])]


    ### ML_SCORE HISTOGRAM - FAKES ###                                         

    plt.hist(df1['ML_SCORE'],bins=np.linspace(0.3,1,100))
    plt.title('ML_SCORE OF FAKES')
    plt.xlabel('ml_score')
    plt.ylabel('# of fakes')
    plt.savefig(os.path.join(outdir,'fakemltest_'+season+'.png'))
    plt.clf()
    ML_ScoreFake=os.path.join(outdir,'fakemltest_'+season+'.png')
    mls=ML_ScoreFake.split('/')[-1]
    shutil.copy(ML_ScoreFake,'./'+mls)
    print('A img was made!', './'+mls)
    
    ### RA/DEC MAPS ###                                                                       

    radecdf = rdf
    if abs(max(radecdf['RA'])-min(radecdf['RA']))>180:
        for ira in range(len(radecdf['RA'])):
            if radecdf['RA'][ira]>180:
                radecdf['RA'][ira] = radecdf['RA'][ira]-360

    radecdf = radecdf.drop_duplicates('cand_ID')
    radecdf = radecdf.loc[radecdf['PHOTPROB'] > ml_score_cut]
    mapdir = os.path.join(outdir,'maps')
    if not os.path.isdir(mapdir):
        os.mkdir(mapdir)

    hexex = []
    ### this loop gets the full set of first epoch exposures of each hex.      
    ### if there are two (or more), it chooses the one with the best t_eff.    
    for h in masdf['hex'].unique():
        exepteff = masdf[['expnum','epoch','t_eff']].loc[masdf['hex'] == h]
        cut = exepteff[['expnum','epoch','t_eff']].loc[exepteff['epoch']==1]
        if len(cut)>1:
            cut = cut.loc[cut['t_eff'] == cut['t_eff'].ix[cut['t_eff'].idxmax()]]
        try:
            hexex.append(cut['expnum'].values[0])
        except IndexError:
            continue

    radecdf = radecdf.loc[radecdf['EXPNUM'].isin(hexex)]

    ### overall map                                                            
    plt.figure(figsize=(16,9))
    plt.scatter(radecdf['RA'],radecdf['DEC'],c=radecdf['PHOTPROB'],edgecolor='',s=10)
    plt.xlim(min(radecdf['RA'])-0.2,max(radecdf['RA'])+0.2)
    plt.ylim(min(radecdf['DEC'])-0.2,max(radecdf['DEC'])+0.2)
    plt.clim(0,1)
    plt.colorbar().set_label('ml_score')
    plt.title('Candidate Sky Map')
    plt.xlabel('RA')
    plt.ylabel('DEC')
    plt.tight_layout()
    plt.savefig(os.path.join(outdir,'fullmap_'+season+'.png'))
    RADECName=os.path.join(outdir,'fullmap_'+season+'.png')
    plt.clf()
    #sys.exit()
    RADECName=os.path.join(outdir,'fullmap_'+season+'.png')
    radec=RADECName.split('/')[-1]
    shutil.copy(RADECName,'./'+radec)
    print('A img was made!','./'+radec)
    


    if not skip:
        for e in hexex:
            print e
            out = ''
            out = os.path.join(rootdir,str(masdf['nite'].loc[masdf['expnum']==e].values[0]))
            out = os.path.join(out,str(e))
            out = os.path.join(out,str(e)+'.out')

            odf = pd.read_table(out,delim_whitespace=True,header=None,names=['expnum','band','ccd','ra1','dec1','ra2','dec2','ra3','dec3','ra4','dec4'])

            odf = odf.drop_duplicates()
            odf = odf.reset_index(drop=True)

            odf.ix[odf.ra1 > 270., 'ra1'] = odf.ix[odf.ra1 > 270., 'ra1'] - 360.
            odf.ix[odf.ra2 > 270., 'ra2'] = odf.ix[odf.ra2 > 270., 'ra2'] - 360.
            odf.ix[odf.ra3 > 270., 'ra3'] = odf.ix[odf.ra3 > 270., 'ra3'] - 360.
            odf.ix[odf.ra4 > 270., 'ra4'] = odf.ix[odf.ra4 > 270., 'ra4'] - 360.

            ras = np.concatenate((odf['ra1'].tolist(),odf['ra2'].tolist(),odf['ra3'].tolist(),odf['ra4'].tolist()),axis=0)
    
            decs = np.concatenate((odf['dec1'].tolist(),odf['dec2'].tolist(),odf['dec3'].tolist(\
),odf['dec4'].tolist()),axis=0)

            for i in range(len(odf)):
                ra = odf.ix[i,['ra1','ra2','ra3','ra4']]
                dec = odf.ix[i,['dec1','dec2','dec3','dec4']]
                chip = str(odf.ix[i,'ccd'])
                midra = (max(ra)+min(ra))/2.
                middec = (max(dec)+min(dec))/2.
                middle = tuple([midra,middec])
                cs = zip(ra,dec)
                cent=(sum([c[0] for c in cs])/len(cs),sum([c[1] for c in cs])/len(cs))
                cs.sort(key=lambda c: math.atan2(c[1]-cent[1],c[0]-cent[0]))
                cs.append(cs[0])
                plt.plot([c[0] for c in cs],[c[1] for c in cs],ls=':',lw=0.5,c='k')
                plt.annotate(chip, xy=middle, ha='center',va='center',family='sans-serif',fontsize=12,alpha=0.3)

            plt.xlim(min(ras)-0.2,max(ras)+0.2)
            plt.ylim(min(decs)-0.2,max(decs)+0.2)


            pltdf = radecdf.loc[radecdf['EXPNUM'] == e]
            plt.scatter(pltdf['RA'],pltdf['DEC'],c=pltdf['PHOTPROB'],edgecolor='')
            plt.clim(0,1)
            plt.colorbar().set_label('ml_score')
            hexname = masdf['hex'].loc[masdf['expnum'] == e].values[0]
            plt.title('Candidate Sky Map: Hex '+str(hexname)+' (Exposure '+str(e)+')')
            plt.xlabel('RA')
            plt.ylabel('DEC')
            plt.tight_layout()
            plt.savefig(os.path.join(mapdir,'map_'+str(hexname)+'_'+str(e)+'_'+season+'.png'),dpi=200)
            plt.clf()
            
            hexm=os.path.join(mapdir,'map_'+str(hexname)+'_'+str(e)+'_'+season+'.png')
            hexmap=hexm.split('/')[-1]
            shutil.copy(hexm,'./'+hexmap)
            print('A img was made!','./'+hexmap)



    if os.path.isfile('./'+radec) and os.path.isfile('./'+mls):
        stat6=True
    else:
        stat6=False

        

    bins = np.arange(17,25,1)

    fhist, bin_edges = np.histogram(df['MAG'], bins=bins)
    thist, bin_edges = np.histogram(df1['TRUEMAG'], bins=bins)


    plt.xlim(17,25)
    plt.ylim(0,100)
    plt.plot(bins[:-1], fhist*100.0/thist, lw=4)
    plt.scatter(bins[:-1], fhist*100.0/thist, lw=4)
    plt.title('Efficiency')
    plt.xlabel('Mag')
    plt.ylabel('Percent Found')
    plt.savefig(os.path.join(outdir,'efftest_'+season+'.png'))
    EffName=os.path.join(outdir,'efftest_'+season+'.png')
    plt.clf()
    eff=EffName.split('/')[-1]
    shutil.copy(EffName,'./'+eff)
    print("You have and efficiency plot!")

    
    

    
    return stat6,mls,radec
