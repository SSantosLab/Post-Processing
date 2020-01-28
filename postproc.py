import os
import tarfile
import sys
import time
import math
import subprocess
from glob import glob
import pandas as pd
from collections import OrderedDict as OD
import easyaccess
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import MultipleLocator, FormatStrFormatter
from astropy.io import fits
from astropy.table import Table
import fitsio
import psycopg2
import fnmatch
import configparser
import csv
from joblib import Parallel, delayed
import datetime
import gc


def prep_environ(rootdir,indir,outdir,season,setupfile,version_hostmatch,db,schema):
### set environment variables for things that will be used often
    os.environ['ROOTDIR']=rootdir
    os.environ['ROOTDIR2']=outdir
    os.environ['INDIR']=indir
    os.environ['EXPDIR']=os.path.join(rootdir,'exp')
    os.environ['SEASON']=season
    os.environ['SETUPFILE']=setupfile
    os.environ['TOPDIR_HOSTMATCH'] = os.path.join(rootdir,'hostmatch/'+version_hostmatch)
    os.environ['OUTDIR_HOSTMATCH'] = os.path.join(outdir,'hostmatch')
    os.environ['DB'] = db
    os.environ['SCHEMA'] = schema
    return True

def masterlist(filename,blacklist_file,ligoid,propid,bands=None,expnums=None,a_blacklist=None):
    indir = os.environ.get('INDIR')
    outdir = os.environ.get('ROOTDIR2')
    outdir = os.path.join(outdir,'masterlist')
    if not os.path.isdir(outdir):
        os.mkdir(outdir)

    filename = os.path.join(outdir,filename) #filename=

    if os.path.isfile(os.path.join(indir,blacklist_file)):
        blacklist = list(np.genfromtxt(blacklist_file,usecols=(0),unpack=True))
    else:
        blacklist = []

    if a_blacklist:
        blacklist = np.concatenate((blacklist,a_blacklist),axis=0)

    blacklist = [int(x) for x in blacklist]

    if expnums:
        if len(expnums)>1:
            query_exp = """select id as expnum, ra, declination as dec, filter, exptime, airmass, seeing, qc_teff, seqnum, program, object as hex, EXTRACT(EPOCH FROM date - '1858-11-17T00:00:00Z')/(24*60*60) as mjd, TO_CHAR(date - '12 hours'::INTERVAL, 'YYYYMMDD') AS nite 
from exposure 
where ra is not null and (program='des gw' or program='survey' or program='des nu' or program='DESGW ER TEST EXPOSURE' or program='GROWTH DECam GW') and id IN """+str(tuple(expnums))+""" order by id"""
        else:
            query_exp = """select id as expnum, ra, declination as dec, filter, exptime, airmass, seeing, qc_teff, seqnum, program, object as hex, EXTRACT(EPOCH FROM date - '1858-11-17T00:00:00Z')/(24*60*60) as mjd, TO_CHAR(date - '12 hours'::INTERVAL, 'YYYYMMDD') AS nite 
from exposure 
where ra is not null and (program='des gw' or program='survey' or program='des nu' or program='DESGW ER TEST EXPOSURE' or program='GROWTH DECam GW') and id="""+str(expnums[0])+""" order by id"""

#         query_count = """select * from (
# WITH objnights AS (
# SELECT obstac.nightmjd(date), object, ra, declination
# FROM exposure.exposure
# WHERE delivered
#       AND propid='"""+propid+"""'
#       AND seqid="""+seqid+"""
#       AND id IN """+str(tuple(expnums))+"""
# GROUP BY obstac.nightmjd(date), object,ra,declination
# )
# SELECT COUNT(*), ra, declination as dec, object as hex
# FROM objnights
# GROUP BY object,ra,declination
# ) as foo order by ra"""
      

    else:
        query_exp = """select id as expnum, ra, declination as dec, filter, exptime, airmass, seeing, qc_teff, seqnum, program, object as hex, EXTRACT(EPOCH FROM date - '1858-11-17T00:00:00Z')/(24*60*60) as mjd, TO_CHAR(date - '12 hours'::INTERVAL, 'YYYYMMDD') AS nite 
from exposure 
where propid='"""+propid+"""' and seqid='"""+ligoid+"""' and ra is not null and (program='des gw' or program='survey' or program='DESGW ER TEST EXPOSURE') 
order by id"""
#         query_count = """select * from (
# WITH objnights AS (
# SELECT obstac.nightmjd(date), object, ra, declination
# FROM exposure.exposure
# WHERE delivered
#       AND propid='"""+propid+"""'
#       AND seqid="""+seqid+"""
# GROUP BY obstac.nightmjd(date), object, ra,declination
# )
# SELECT COUNT(*), ra, declination as dec, object as hex
# FROM objnights
# GROUP BY object,ra,declination
# ) as foo order by ra"
    conn =  psycopg2.connect(database='decam_prd',
                               user='decam_reader',
                               host='des61.fnal.gov',
                               password='reader',
                               port=5443) 

    print(query_exp)

    expdf = pd.read_sql(query_exp,conn)

    #ctdf = pd.read_sql(query_count,conn)

    conn.close()

    expdf = expdf.loc[~expdf['expnum'].isin(blacklist)]

    expdf['dup'] = expdf.duplicated(subset=['ra','nite'])

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

    ### generate GW hex name
    striphex = []
    for iex in expdf['expnum']:
        ra10 = (expdf['ra'].loc[expdf['expnum']==iex]).values[0]*10
        dec10 = (expdf['dec'].loc[expdf['expnum']==iex]).values[0]*10
        ra10 = str(ra10).split('.')[0]
        dec10 = '%+f' % dec10
        dec10 = str(dec10).split('.')[0]
        newhex = 'GW'+ra10+dec10
        striphex.append(newhex)
    
    niteform = lambda x: int(x)
    expdf['nite'] = expdf['nite'].map(niteform)

    expform = lambda x: int(x)
    expdf['expnum'] = expdf['expnum'].map(expform)

    mjdform = lambda x: round(x,3)
    expdf['mjd'] = expdf['mjd'].map(mjdform)

    expdf['epoch'] = epoch

    expdf['striphex'] = striphex

    expdf = expdf.loc[expdf['filter'].isin(bands)]
    #expdf = expdf.sort_values(by='EXPNUM')
    expdf = expdf.reset_index(drop=True)    
    
    tbhdu1 = fits.BinTableHDU.from_columns(
        [fits.Column(name='object', format='A69', array=expdf['hex']),
         fits.Column(name='hex', format='A8', array=striphex),
         fits.Column(name='epoch', format='K', array=expdf['epoch']),
         fits.Column(name='expnum', format='K', array=expdf['expnum']),
         fits.Column(name='RA', format='E', array=expdf['ra']),
         fits.Column(name='DEC', format='E', array=expdf['dec']),
         fits.Column(name='nite', format='K', array=expdf['nite']),
         fits.Column(name='band',format='A1', array=expdf['filter']),
         fits.Column(name='mjd', format='E', array=expdf['mjd']),
         fits.Column(name='t_eff', format='E', array=expdf['qc_teff']),
         ])

    tbhdu1.writeto(filename,clobber=True) #filename
    
    f=open(os.path.join(outdir,blacklist_file),'w')
    f.write(str(sorted(set(blacklist))))
    f.close()
    print(filename)
    
    return expdf[['expnum','nite','filter']],filename, True

def checkoutputs(expdf,logfile,ccdfile,goodchecked,steplist):
    expnums = expdf['expnum'].tolist()
    nites = expdf['nite'].tolist()
    bands = expdf['filter'].tolist()

    season = os.environ.get('SEASON')
    outdir = os.path.join(os.environ.get('ROOTDIR2'),'checkoutputs')
    if not os.path.isdir(outdir):
        os.mkdir(outdir)

    steplist = os.path.join(os.environ.get('INDIR'),steplist)
    f = open('steplist.txt','r')
    stepnames = f.readlines()
    f.close()

    stepnames = map(lambda x: x.strip(), stepnames)

    goodchecked = os.path.join(outdir,goodchecked)
    if os.path.isfile(goodchecked):
        f = open(goodchecked,'r')
        good = f.readlines()
        f.close()
        good = map(lambda x: int(x.strip()), good)
#        print("good list", good)
    else:
        good = []
        print("GOOD IS EMPTY")

    totexp = len(expnums)

    expdir = os.environ.get('EXPDIR')
    logname = os.path.join(outdir,logfile)
    lf = open(logname,'w+')
    lf.write('CHECKOUTPUTS\n\n')
    lf.write('---'),lf.write('\n\n')
    lf.write('EXPOSURES PROVIDED: '),lf.write(','.join(map(str,sorted(expnums))))
    lf.write('\n\n')
    t1 = time.time()
    os.fsync(lf)

    d = OD()
    d['expnum'] = []
    chips = range(1,63)
    steps = range(1,29)
    chips.remove(2),chips.remove(31),chips.remove(61)
    for ch in chips:
        ch = '%02d' % ch
        d[ch] = []
    for e in expnums:
        nite = nites[expnums.index(e)]
        nite = str(nite)
        band = bands[expnums.index(e)]
        e = str(e)
        end = nite+'/'+e+'/'+'dp'+season
        p = os.path.join(expdir,end)
        if int(e) not in good:
            if os.path.isdir(p):
                print(str(expnums.index(int(e))+1)+'/'+str(len(expnums))+' - '+p)
                d['expnum'].append(e)
            else:
                print(str(expnums.index(int(e))+1)+'/'+str(len(expnums))+' - '+p, 'does not exist. Check diffimg outputs.')
                continue
            for c in chips:
                c = '%02d' % c
                p2 = os.path.join(p,band+'_'+c)
                outtar = 'outputs_' + 'dp' + season + '_' + nite + '_' + e + '_' + band + '_' + c + '.tar.gz'
                gptar = os.path.join(p2,outtar)
### The current assumption is that .FAIL files are cleared out when a CCD is reprocessed. 
### If this is not true, uncomment the 3 lines below and tab the append and break lines. 
### In that event, one must also consider how to deal with a RUN28 failure.
                if os.path.isdir(p2):
                #     #tb = time.time()
                #     ldir = subprocess.check_output('ls '+p2, shell=True).splitlines()
                #     ldir = os.listdir(p2)
                #     #ta = time.time()
                #     #print(ta-tb)
                
                # for fil in ldir:
                #     if fnmatch.fnmatch(fil,'RUN*.FAIL'):
                #         r = int((fil.split('RUN')[1]).split('_')[0])
                #         d[c].append(r)
                #         break
                # else:
                #     if stepnames[0] not in ldir:
                #         d[c].append(-1)
                #     elif (stepnames[27]+'.LOG') not in ldir:
                #         d[c].append(-9)
                #     else:
                #         d[c].append(0)
                #     #timeb = time.time()
                    for r in steps:
                        fail = stepnames[r-1]+'.FAIL'
                        gpfail = os.path.join(p2,fail)
                        gpstep1 = os.path.join(p2,stepnames[0])
                        gpstep28 = os.path.join(p2,stepnames[27])
                        exists = os.path.isfile(gpfail)
                        if exists:
                            #log = stepnames[r]+'.LOG'
                            #plog = os.path.join(gp,log)
                            #if os.path.isfile(plog):
                            d[c].append(int(r))
                            break
                        elif not exists and not os.path.isfile(gpstep1) and not os.path.isfile(gptar):
                            d[c].append(-1)
                            print(p2 + ' is a -1 CCD.')
                            break
                    else:
                        if not os.path.isfile(gpstep28+'.LOG') and not os.path.isfile(gptar):
                            d[c].append(-9)
                            print(p2 + ' is a -9 CCD.')
                        else:
                            d[c].append(0)
                    #timea = time.time()
                    #print(timea-timeb)
        else:
            
            print(str(expnums.index(int(e))+1)+'/'+str(len(expnums))+' - '+p)
            d['expnum'].append(e)
            for c in chips:
                c = '%02d' % c
                d[c].append(0)

    totchk = len(d['expnum'])
    totnot = totexp-totchk

    lf.write('EXPOSURES CHECKED: '),lf.write(','.join(map(str,sorted(d['expnum']))))
    lf.write('\n\n')
    lf.flush()
    os.fsync(lf)

    nonex,yesex = [],[]
    for x in sorted(expnums):
        x=str(x)
        if x not in d['expnum']:
            nonex.append(x)
        else:
            yesex.append(x)
    lf.write('EXPOSURES NOT FOUND: ')
    if len(nonex)==0:
        lf.write('none')
    else:
        lf.write(','.join(map(str,nonex)))
    lf.write('\n\n')
    
    lf.write('TOTAL EXPOSURES PROVIDED: '+str(totexp)+'\n')
    lf.write('TOTAL EXPOSURES FOUND: '+str(totchk)+'\n')
    lf.write('TOTAL EXPOSURES NOT FOUND: '+str(totnot)+'\n\n')
    lf.write('----------\n\n')

    lf.flush()
    os.fsync(lf)

    for c in chips:
        c = '%02d' % c
        #print(len(d[c]))

    df1 = pd.DataFrame(d)
    df = df1.set_index('expnum')
    print("")

    ccddf = df.copy()

    listgood = df.loc[df.sum(axis=1) == 0].index
    print("")
#    print("listgood", listgood)
    listgood = listgood.tolist()
    for l in listgood:
        if len(set(df.loc[l].values))!=1:
            listgood.remove(l)
    listgood = map(lambda x: int(x), listgood)
    np.savetxt(goodchecked,sorted(listgood),fmt='%d')

    df['unfinished']=(df<0).astype(int).sum(axis=1)
#    print("unfinished", df['unfinished'])
    
    dfsuc = df.drop('unfinished',1)
    
    df['successes']=(dfsuc==0).astype(int).sum(axis=1)
    print("df[successes]",df['successes'])
    
    df['fraction'] = ""
    for exp in list(df.index.values):
        frac = float(df.get_value(exp,'successes'))/59.
        frac = round(frac,3)
        df.set_value(exp,'fraction',frac)
    
    lf.write('TOTAL CCDS CHECKED: ')
    ccdtot = 59*len(df['successes'])
    lf.write(str(ccdtot)),lf.write('\n')
    
    lf.write('TOTAL CCDS SUCCEEDED: ')
    ccdsum = sum(df['successes'])
    lf.write(str(ccdsum)),lf.write('\n')
    
    lf.write('CCD SUCCESS RATE: ')
    print
    print("ccdsum, ccdtoto", ccdsum, ccdtot)
    div = float(ccdsum)/float(ccdtot)
    div = div*100
    lf.write('%.1f%%' % div)
    
    lf.write('\n\n----------\n\n')

    lf.flush()
    os.fsync(lf)

    allf = np.concatenate(([-9,-1],range(1,29)),axis=0)

    for s in allf:
        lf.write('STEP '+str(s)+' FAILURES: ')
        tot=((ccddf==s).astype(int).sum(axis=1)).sum()
        lf.write(str(tot)+'\n')
        div = float(tot)/float(ccdtot)
        div = div*100
        lf.write('FAILURE RATE: %.1f%%\n\n' % div)
        lf.flush()
        os.fsync(lf)

    lf.close()

    df.sort_index(inplace=True)
    df.to_csv(os.path.join(outdir,ccdfile))

    #baddf = ccddf[ccddf.apply(lambda x: min(x) == max(x),1)]
    #print(baddf)
    #baddf = baddf[baddf.apply(lambda x: min(x) == -1,1)]
    #print(baddf)
    #print(baddf.index.values.tolist())

    return yesex,nonex,ccddf,True
            
def forcephoto(season,ncore=4,numepochs_min=0,writeDB=False):    
    #season = os.environ.get('SEASON')
    a = './forcePhoto_master.pl ' 
    a = a + ' -season ' + season 
    a = a + ' -numepochs_min ' + numepochs_min 
    a = a + ' -ncore ' + str(ncore) 
    a = a + ' -noprompt '
    a = a + ' -SKIP_CORRUPTFILE '
    if writeDB == True:
        a = a + ' -writeDB ' 
    print(a)
    #a = 'source '+os.getenv('SETUPFILE')+'; '+a
    subprocess.call(a,shell=True)
 
def truthtable(season,expnums,filename,truthplus):
    #season = os.environ.get('SEASON')
    outdir = os.path.join(os.environ.get('ROOTDIR2'),'truthtable'+season)
    if not os.path.isdir(outdir):
        os.mkdir(outdir)
    db = os.environ.get('DB')
    schema = os.environ.get('SCHEMA')

    explist=','.join(map(str,expnums))

### Truth table (normal)
    query='select distinct SNFAKE_ID, EXPNUM, CCDNUM, TRUEMAG, TRUEFLUXCNT, FLUXCNT, BAND, NITE, MJD, SEASON from '+ schema +'.SNFAKEIMG where EXPNUM IN ('+explist+') and SEASON='+ season +' order by SNFAKE_ID'
    print(query)

    filename=os.path.join(outdir,filename)
    connection=easyaccess.connect(db)
    connection.query_and_save(query,filename)

    print("")

### Truth table plus

    #query='select f.SNFAKE_ID, f.EXPNUM, f.CCDNUM, o.RA, o.DEC, o.MAG, o.FLUX, o.FLUX_ERR, f.TRUEMAG, f.TRUEFLUXCNT, o.FLUX, o.SEXFLAGS, f.BAND, f.NITE, f.MJD, f.SEASON from '+ schema +'.SNFAKEIMG f, '+ schema +'.SNOBS o where f.SNFAKE_ID=o.SNFAKE_ID and f.EXPNUM=o.EXPNUM and f.SEASON='+ season +' and f.SEASON=o.SEASON order by SNFAKE_ID'

    query = 'select SNFAKE_ID, EXPNUM, CCDNUM, RA, DEC, -2.5*log(10,FLUXCNT)+ZERO_POINT as MAG, MAGOBS_ERR as MAGERR, FLUXCNT, TRUEMAG, TRUEFLUXCNT, SNR_DIFFIM as SNR, REJECT, ML_SCORE, BAND, NITE, SEASON from '+ schema +'.SNFAKEMATCH where SEASON='+ season +' order by SNFAKE_ID'

    print(query)

    plus = connection.query_to_pandas(query)
    connection.query_and_save(query,os.path.join(outdir,truthplus))

    connection.close()
    
    status=True
    
    return plus,status

def makedatafiles(season,format,numepochs_min,two_nite_trigger,outfile,outdir,ncore,fakeversion=None):

    print("gc is enabled %d" % int(gc.isenabled())) 
    #season = os.environ.get('SEASON')
    datafiles_dir = os.path.join(os.environ.get('ROOTDIR2'),'makedatafiles')
    db = os.environ.get('DB')
    schema = os.environ.get('SCHEMA')

    query = 'select distinct SNID from '+ schema +'.SNCAND where SEASON='+ str(season) +' order by SNID'

    connection = easyaccess.connect(db)
    cursor = connection.cursor()
    QQ = cursor.execute(query)
    rows = cursor.fetchall()
    cols = np.array(zip(*rows))
    allcand = cols[0]
    numcands = len(allcand)

    splcand = np.array_split(allcand,ncore)

    if not os.path.isdir(datafiles_dir):
        os.mkdir(datafiles_dir)

    # test = splcand[0][:1000]
    # numtest = len(test)
    
    # if 1==1:
    #     datdir = os.path.join(datafiles_dir,'LightCurvesReal')
    #     a = 'makeDataFiles_fromSNforce' 
    #     a = a + ' -format ' + format 
    #     a = a + ' -season ' + season
    #     a = a + ' -snid_min ' + str(min(test))
    #     a = a + ' -snid_max ' + str(max(test))
    #     a = a + ' -numepochs_min ' + numepochs_min  
    #     a = a + ' -outFile_stdout ' + outfile+'_test2' 
    #     a = a + ' -outDir_data ' + outdir
    #     if not two_nite_trigger == 'null':
    #         a = a + ' -2nite_trigger ' + trigger 
    #     if not fakeversion == None: 
    #         datdir = os.path.join(datafiles_dir,'LightCurvesFake')
    #         a = a + ' -fakeVersion ' + fakeversion
    #     a = '(source '+os.getenv('SETUPFILE')+'; cd '+datafiles_dir+ '; '+ a + '; cd -)&'
    #     print(a)
    #     subprocess.call(a, shell=True)


    procs = []

    for i in range(ncore):
        datdir = os.path.join(datafiles_dir,outdir)
        if not os.path.isdir(datdir):
            os.mkdir(datdir)
        a = 'makeDataFiles_fromSNforce' 
        a = a + ' -format ' + format 
        a = a + ' -season ' + season
        a = a + ' -snid_min ' + str(min(splcand[i]))
        a = a + ' -snid_max ' + str(max(splcand[i]))
        a = a + ' -numepochs_min ' + numepochs_min  
        a = a + ' -outFile_stdout ' + outfile + '_' + str(i+1) 
        a = a + ' -outDir_data ' + outdir
        a = a + ' -ML_trigger ' + '0.7'
        if not two_nite_trigger == 'null':
            a = a + ' -2nite_trigger ' + trigger 
        if not fakeversion == None: 
            a = a + ' -fakeVersion ' + fakeversion
        a = '(source '+os.getenv('SETUPFILE')+'; cd '+datafiles_dir+ '; '+ a + '; cd -)&'
        print(a)
        s = subprocess.Popen(a, shell=True)
        procs.append(s)

    percand = 1 # max sec per cand; usually takes ~0.5 to 0.8 
    maxtime = (percand*numcands/float(ncore))+60 # in sec        
#    print maxtime
#    print maxtime/3600.

    alltime = 0
    last = -1
    while len(os.listdir(datdir)) < (numcands+3):
        length = len(os.listdir(datdir))
        if length == last and length>0:
            dats = os.listdir(datdir)
            dats = [x for x in dats if '.dat' in x]
            cs,nodat = [],[]
            for d in dats:
                cand = int(filter(str.isdigit,d))
                cs.append(cand)
            for c in allcand:
                if c not in cs:
                    nodat.append(c)
            print("Total datafiles made not equal to number of candidates in SNCAND for season "+str(season)+".")
            print("")
            print("MADE: "+str(length-3))
            print( "SNCAND: "+str(numcands))
            print("")
            print("CANDS LEFT OUT: "+str(nodat)[1:-1])
            print("")
            break
        last = length
        print("Not done yet.",str(len(os.listdir(datdir)))+'/'+str(numcands+3))
        yawn = 60 # seconds to wait before checking again
        time.sleep(yawn)
        alltime += yawn
        if alltime>maxtime:
            break



###The following two functions are heavily based on a tutorial
def gif_files(members):
    for tarinfo in members:
        if os.path.splitext(tarinfo.name)[1] == ".gif":
            yield tarinfo
def fits_files(members):
    for tarinfo in members:
        if os.path.splitext(tarinfo.name)[1] == ".fits":
            yield tarinfo
###

####Make an html file full of git files
def makeHTML(tar, Name):
    ###get tar files
    ####Get the distinguishing number at the end of the tar file
    tarsplit=tar.split('/')
    tarlen=len(tarsplit)
    quality=tarsplit[tarlen-1]
    definingQuality=quality.split('.')[0] #stamp
    specificGifAndFitsDir='GifAndFits'+definingQuality+'/'
    ####Use or make a dir in which to put the tar files
    if not os.path.isdir(specificGifAndFitsDir):
        os.makedirs(specificGifAndFitsDir)
    lilTar=tarfile.open(tar)
    lilTar.extractall(members=gif_files(lilTar), path = specificGifAndFitsDir)
    #lilTar.extractall(members=fits_files(lilTar), path = specificGifAndFitsDir)
    
    ###create html
    #Name='theProtoATC'+definingQuality+'.html'
    #htmlYeah=open(Name,'a')
    allTheGifs=glob(specificGifAndFitsDir+'/*.gif')
    gifDict={}
    #topLines=['<!DOCTYPE HTML>\n','<html>\n','<head>','<title> Plots from'+definingQuality+'</title>\\n','</head>\n','<body>']
    #bottomLines=['</body>\n','</head>']
    ####Create the beginning text for an html file
    #for tag in topLines:
     #   htmlYeah.write(tag)
    #htmlYeah.close()
    ####Group template, difference, and search images of observation together by number at end of file name 
    for File in allTheGifs:
        value=''
        for char in File:
            try:
                char=int(char)
            except:
                pass
            if isinstance(char,int):
                value+=str(char)
        if not value in gifDict.keys():
            aList=[]
            gifDict[value]=aList
        gifDict[value].append(File)
    #htmlYeah=open(Name,'a')
    ####Create body text and embed images
    #for tag in topLines:
     #   htmlYeah.write(tag)
   # htmlYeah.close()
    htmlYeah=open(Name,'a')
    for gifSet in list(gifDict.values()):
        gifSet.sort()
        #imgLocation=GifAndFitsDir+gif                                              
        lines=['<h1>What Is Going on Here?</h1>\n','<p>It is a gif!</p>\n','<div id="gifs">','<span title='+gifSet[0]+'>','<img src=\''+gifSet[0]+'\' alt='+gifSet[0]+' width="200" height="200"/></span>\n','<span title='+gifSet[1]+'>','<img src=\''+gifSet[1]+'\' alt='+gifSet[1]+' width="200" height="200"/></span>\n','<span title='+gifSet[2]+'>','<img src=\''+gifSet[2]+'\' alt='+gifSet[2]+'width="200" height="200"/></span>\n','</div>']
        for line in lines:
            htmlYeah.write(line)
    #omLines:
    #te(line)
    #
    htmlYeah.close()
    return



def ExtracTarFiles(tar,season):
    ###get tar files                                                           
    ####Get the distinguishing number at the end of the tar file               
    tarsplit=tar.split('/')
    tarlen=len(tarsplit)
    quality=tarsplit[tarlen-1]
    definingQuality=quality.split('.')[0] #stamp                               
    specificGifAndFitsDir='GifAndFits'+definingQuality+'/'
    ####Use or make a dir in which to put the tar files                        
    if not os.path.isdir(specificGifAndFitsDir):
        os.makedirs(specificGifAndFitsDir)
    lilTar=tarfile.open(tar)
    lilTar.extractall(members=gif_files(lilTar), path = (specificGifAndFitsDir))
    allTheGifs=glob(specificGifAndFitsDir+'/*.gif')
    #print('in extract stuff', type(allTheGifs),len(allTheGifs), allTheGifs[0])
    return allTheGifs




def MakeDictforObjidsHere(stamps4file,ObjidList):
    ObjidDict={}###dictionary pointing to gifs and fits for dat file

    #print('you just entered the twilight zone.')
    for n in range(len(ObjidList)):
        ObjidDict[str(int(ObjidList[n]))]=[]
        
    for File in stamps4file:
        #print(File)
        NumID=''
        for char in File.split('/')[-1].split('.')[0]:
            try:
                char=int(char)
            except:
                pass
            if isinstance(char,int):
                NumID+=str(char)
        #NumID=int(NumID)
        #print(NumID)
        if NumID in list(ObjidDict.keys()):
            #print('Match!')
            preHappy=[]
            preHappy.append(File)
            #print('this is preHappy',preHappy)
            happyList=ObjidDict[NumID]+preHappy
            ObjidDict[NumID]=happyList

    return ObjidDict




def ErrorMag(flux,fluxerror):
    dmdflux=1/(flux)
    almostError=(dmdflux**2)*fluxerror
    Error=almostError**(.5)
    return Error


def getBandsandField(lines):
    band=[]
    field=[]
    for line in lines:
        if line.split(' ')[0]=='OBS:':
            ##get bands
            bandy=str(line.split(' ')[5])
            band.append(bandy)
            ##get field
            fieldy=str(line.split(' ')[7])
            field.append(fieldy)

    return band,field



def MakeobjidDict(mjd,fluxcal,fluxcalerr,photflag,photprob,zpflux,psf,skysig,skysig_t,gain,xpix,ypix,nite,expnum,ccdnum,objid,band,field):
    objidDict={}
    try:
        for i in range(len(objid)):
            if objid[i] != 0:
                objidDict[str(int(objid[i]))]=[str(mjd[i]),band[i],str(field[i]),str(fluxcal[i]),str(fluxcalerr[i]),str(photflag[i]),str(photprob[i]),str(zpflux[i]),str(psf[i]),str(skysig[i]),str(skysig_t[i]),str(gain[i]),str(xpix[i]),str(ypix[i]),str(int(nite[i])),str(int(expnum[i])),str(int(ccdnum[i]))]
            else:
                continue
    except TypeError:
        if objid != 0:
            objidDict[str(int(objid))]=[str(mjd),str(band[0]),str(field),str(fluxcal),str(fluxcalerr),str(photflag),str(photprob),str(zpflux),str(psf),str(skysig),str(skysig_t),str(gain),str(xpix),str(ypix),str(int(nite)),str(int(expnum)),str(int(ccdnum))]
    return objidDict



######### BEST
'''def makeLightCurves(datFile,lines, skipheader,triggermjd,season):
    #triggermjd = config.get('general','triggermjd')
    snid = lines[1].split()[1]
    Filter,Flux,FluxErr,Mjd,Nite,Objid=np.genfromtxt(datFile,skip_header=skipheader,usecols=(2,4,5,1,15,18),unpack=True,dtype=str)
    try:
        Flux = [float(flux) for flux in Flux]
        FluxErr = [float(fluxerr) for fluxerr in FluxErr]
        Mjd = [float(mjd) for mjd in Mjd]
        Nite = [float(nite) for nite in Nite]
        Objid = [float(objid) for objid in Objid]
    
    except:
        Flux = float(Flux)
        FluxErr = float(FluxErr)
        Mjd = float(Mjd)
        Nite = float(Nite)
        Objid = float(Objid)

    Mjd = np.asarray(Mjd)
    triggermjd = float(triggermjd)
    try:
        Time = Mjd - triggermjd
    except:
        Time=0

    bd = {}
    try:
        for filter_ in Filter:
             bd[filter_] = [[],[],[]]
    except:
        bd[Filter] = [[],[],[]]

    try:
        for i in range(len(Objid)):
            if Objid[i] != 0.0:
                if Flux[i] == 0.0:
                    m = 99
                    magErr = 0.0
                else:
                    m=-2.5*np.log10(Flux[i])+27.5
                    magErr=ErrorMag(Flux[i],FluxErr[i])
                bd[Filter[i]][0].append(m)
                bd[Filter[i]][1].append(Time[i])
                bd[Filter[i]][2].append(magErr)
    except:
        if Objid !=0.0:
            if Flux == 0.0:
                m = 99
                magErr = 0.0
            else:
                m =-2.5*np.log10(Flux)+27.5
                magErr=ErrorMag(Flux,FluxErr)
            bd[Filter[0]][0].append(m)
            bd[Filter[0]][1].append(Time)
            bd[Filter[0]][2].append(magErr)

    fig = plt.figure()
    ax = fig.gca()
    for b in bd.keys():
        if not bd[b]==[[],[],[]]:
            myyerr=np.asarray(bd[b][2])
            plt.errorbar(bd[b][1],bd[b][0],yerr=myyerr, fmt='o', label=b+" band")               

    ax.grid()
    ax.invert_yaxis()
    ax.legend(title="SNID: " + str(snid) + "\n Season: " + str(season),bbox_to_anchor=(1.1,.9), bbox_transform=plt.gcf().transFigure)
    plt.xlabel('Days Since Merger')
    plt.ylabel('Magnitude')

    LightCurveName='LightCurve_'+datFile.split('.')[1].split('/')[-1]+'.png'
    fig.savefig(LightCurveName,bbox_inches='tight')
    return LightCurveName'''
######### BEST



def ZapHTML(season,Dict,objidDict,theDat,datInfo,LightCurveName,snidDict): #Dict with obs and associated gifs, dict with OBJIDS and associated dat file info ,list of tar files that correspond to observations, list=[snid,raval,decval], name of the Light curve for the dat file, dictionary mapping snid to host galaxy info
    if datInfo[0] not in list(snidDict.keys()) or snidDict[datInfo[0]]==[('N/A', '0','N/A', 'N/A')]: ##This condition is to work around when clearing the database also removes (hides?) the SNIDs with no potetial host galaxy
        snidDict[datInfo[0]]=[('-999', '0','-999', '-999')]
    GalInfo=snidDict[datInfo[0]]
    HostID=GalInfo[0][0]
    HostRank=GalInfo[0][1]
    HostRA=GalInfo[0][2]
    HostDEC=GalInfo[0][3]

    dat=theDat.split('_')[-1]
    
    Name='theProtoATC_'+season+'_'+theDat+'.html'
    htmlYeah=open(Name,'w+')
    topLines=['<!DOCTYPE HTML>\n','<html>\n','<head>',
          '<link rel="stylesheet" type="text/css" href="../../theProtoATCStyleSheet.css">',
          '<title> Plots from '+theDat+'</title>\n','<h1>This is the title for '+theDat+'</h1>','\n','</head>\n',
          '<body>','<p> Candidates whose ML scores are less than 0.7 are not displayed. </p>',
          '<table align="center">\n','<caption>Candidate (SNID  '+str(datInfo[0])+') Info</caption>','<tr>','<th>RA</th>\n','<td>'+str(datInfo[1])+'</td>\n',
          '<th>DEC</th>\n','<td>'+str(datInfo[2])+'</td>\n','</tr>','</table>',
          '<table align="center">','<caption>Host Galaxy Info</caption>','<tr>','<th>HOST_ID</th>','<td>'+HostID+'</td>','</tr>',
          '<tr>','<th>HOST_RANK</th>','<td>'+HostRank+'</td>','</tr>',
          '<tr>','<th>HOST_RA</th>','<td>'+HostRA+'</td>','</tr>',
          '<tr>','<th>HOST_DEC</th>','<td>'+HostDEC+'</td>','</tr>','</table>']
    for tag in topLines:
        htmlYeah.write(tag)
    htmlYeah.close()
    

    ##obs info table
#    openingLines=['<table width="750" align="center">','<caption>Observation Info</caption>','<tr>','<th>OBJID</th>','<th>MJD</th>','<th>FLT</th>','<th>FIELD</th>','<th>FLUXCAL</th>','<th>FLUXCALERR</th>','<th>MAG</th>','<th>MAGERR</th>','<th>PHOTFLAG</th>','<th>PHOTPROB</th>','<th>ZPFLUX</th>','<th>PSF</th>','<th>SKYSIG</th>','<th>SKYSIG_T</th>','<th>GAIN</th>','<th>XPIX</th>','<th>YPIX</th>','<th>NITE</th>','<th>EXPNUM</th>','<th>CCDNUM</th>','</tr>']

    #AG add sortTable option, if breaks use line above 
    openingLines=['<table width="750" align="center">','<caption>Observation Info. Click on header to sort</caption>','<tr>','<th onclick="sortTable(0)">OBJID</th>','<th onclick="sortTable(1)">MJD</th>','<th onclick="sortTable(2)">FLT</th>','<th onclick="sortTable(3)">FIELD</th>','<th onclick="sortTable(4)">FLUXCAL</th>','<th onclick="sortTable(5)">FLUXCALERR</th>','<th onclick="sortTable(6)">MAG</th>','<th onclick="sortTable(7)">MAGERR</th>','<th onclick="sortTable(8)">PHOTFLAG</th>','<th onclick="sortTable(9)">PHOTPROB</th>','<th onclick="sortTable(10)">ZPFLUX</th>','<th onclick="sortTable(11)">PSF</th>','<th onclick="sortTable(12)">SKYSIG</th>','<th onclick="sortTable(13)">SKYSIG_T</th>','<th onclick="sortTable(14)">GAIN</th>','<th onclick="sortTable(15)">XPIX</th>','<th onclick="sortTable(16)">YPIX</th>','<th onclick="sortTable(17)">NITE</th>','<th onclick="sortTable(18)">EXPNUM</th>','<th onclick="sortTable(19)">CCDNUM</th>','</tr>']

    htmlYeah=open(Name,'a')
    for line in openingLines:
        htmlYeah.write(line)
    htmlYeah.close()
    htmlYeah=open(Name,'a')
    for key in list(objidDict.keys()):
        mjd=objidDict[key][0]
        band=objidDict[key][1]
        field=objidDict[key][2]
        FLUXCAL=objidDict[key][3]
        FLUXCALERR=objidDict[key][4]
        FLUXCAL = float(FLUXCAL)
        FLUXCALERR = float(FLUXCALERR)
        MAG = -2.5*np.log10(FLUXCAL)+27.5
        MAGERR = ErrorMag(FLUXCAL,FLUXCALERR)
        FLUXCAL = str(FLUXCAL)
        FLUXCALERR = str(FLUXCALERR)
        MAG =str(MAG)
        MAGERR =str(MAGERR)
        PHOTFLAG=objidDict[key][5]
        PHOTPROB=objidDict[key][6]
        ZPFLUX=objidDict[key][7]
        PSF=objidDict[key][8]
        SKYSIG=objidDict[key][9]
        SKYSIG_T=objidDict[key][10]
        GAIN=objidDict[key][11]
        XPIX=objidDict[key][12]
        YPIX=objidDict[key][13]
        NITE=objidDict[key][14]
        EXPNUM=objidDict[key][15]
        CCDNUM=objidDict[key][16]

        tableLines=['<tr>','<td>'+key+'</td>','<td>'+mjd+'</td>','<td>'+band+'</td>','<td>'+field+'</td>','<td>'+FLUXCAL+'</td>','<td>'+FLUXCALERR+'</td>','<td>'+MAG+'</td>','<td>'+MAGERR+'</td>','<td>'+PHOTFLAG+'</td>','<td>'+PHOTPROB+'</td>','<td>'+ZPFLUX+'</td>','<td>'+PSF+'</td>','<td>'+SKYSIG+'</td>','<td>'+SKYSIG_T+'</td>','<td>'+GAIN+'</td>','<td>'+XPIX+'</td>','<td>'+YPIX+'</td>','<td>'+NITE+'</td>','<td>'+EXPNUM+'</td>','<td>'+CCDNUM+'</td>','</tr>']
        for line in tableLines:
            htmlYeah.write(line)
    htmlYeah.close()
    
    closingLines=['</table>']
    htmlYeah=open(Name,'a')
    htmlYeah.write(closingLines[0])
    htmlYeah.close()
    
    for key in list(objidDict.keys()):
        mjd=objidDict[key][0]
        Dict[key].sort
        #print('this is Key!',key)
        #print(len(Dict[key]))
        #print(Dict[key])
        if Dict[key]==[]:
            continue
        for i in range(0,len(Dict[key]),3):
            keyHole=key[16:-1]
            Info=Dict[key][i].split('/')[0].split('_')[1]+Dict[key][i].split('/')[0].split('_')[2]
#            print("ag 1", Dict[key][i])
#            print("ag 2", Dict[key][i+1])
#            print("ag 3", Dict[key][i+2])
            htmlYeah=open(Name,'a')
            lines=['<h1>Observation OBJID: '+key+'</h1><h2>MJD: '+mjd+'</h2>\n','<h2>'+Info+'</h2>\n','<h2> Diff, Search, Temp</h2>\n','<div id="gifs">','<span title='+Dict[key][i]+'>','<img src=\''+Dict[key][i]+'\' width="200" height="200"/></span>\n','<span title='+Dict[key][i+1]+'>','<img src=\''+Dict[key][i+1]+'\' width="200" height="200"/></span>\n','<span title='+Dict[key][i+2]+'><img src=\''+Dict[key][i+2]+'\' width="200" height="200"/></span>','</div>']
            for line in lines:
                htmlYeah.write(line)
            htmlYeah.close()
    htmlYeah=open(Name,'a')
    someLines='<h1>Collected Observations</h1>'
    htmlYeah.write(someLines)
    for key in list(Dict.keys()):
        Dict[key].sort
        for i in range(0,len(Dict[key]),3):
            newLines=['<div id="gifs">','<span title='+Dict[key][i]+'>','<img src=\''+Dict[key][i]+'\' width="100" height="100"/></span>\n','<span title='+Dict[key][i+1]+'>','<img src=\''+Dict[key][i+1]+'\' width="100" height="100"/></span>\n','<span title='+Dict[key][i+2]+'><img src=\''+Dict[key][i+2]+'\' width="100" height="100"/></span>','</div>']
            for line in newLines:
                htmlYeah.write(line)
    htmlYeah.close()
    htmlYeah=open(Name,'a')
    LightLines=['<h1>Light Curve</h1>','<img src=\''+LightCurveName+'\'width="533" height="400">']
    for line in LightLines:
        htmlYeah.write(line)
    htmlYeah.close()
    htmlYeah=open(Name,'a')
    bottomLines=['</body>\n','</html>']
    for line in bottomLines:
        htmlYeah.write(line)
    htmlYeah.close()

    return 




def FindTarsforObjids(ListOtarFiles,ListOobjids): #Takes a list of all the tar found for dat file and the list of objids for the dat file                     
    ListOunusedTarFiles=[] #List of tarFiles not pertaining to an observation in current data file                                                            
    ListOTarsforDat=[] #List of tarFiles corresponding to observations in data file                                                                           
    for File in ListOtarFiles:
        preNumID=File.split('.')[-1].split('/')[0]
        NumID=''
        for char in File:
            try:
                char=int(char)
            except:
                pass
            if isinstance(char,int):
                NumID+=str(char)
        NumID=int(NumID) #Find the number                                      
        if any(ListOobjids)==NumID:
            ListOTarsforDat.append(File)
        else:
            ListOunusedTarFiles

    return ListOTarsforDat





###determines whether a dat file contains more than one data point. Unnecesasry, though, because you can just do a np.getfromtext and determine the length of the resulting array.
def checkDatFile(exposure_file):
    file_under_scrutiny=open(exposure_file)
    DatCount=0
    Continue='Yes'
    List=file_under_scrutiny.readlines()
    file_under_scrutiny.close()
    #print(List)
    for string in List:
        part=string.split(' ')
        #print(part)
        if part[0]=='OBS:':
            DatCount+=1
    if DatCount==1:
       Continue='No'
    return Continue


def getGTL():
    if not os.path.exists('GiantTarList.txt'):
        return []
    else:
        GTL=open('GiantTarList.txt','r')
        lines=GTL.readlines()
        GTL.close()

        GiantTarList=[]
        for i in lines:
            GiantTarList.append(i[:-1])

        return GiantTarList

def updateGTL(newTars):
    GTL=open('GiantTarList.txt','w+')
    for new in newTars:
        GTL.write(new+'\n')
    GTL.close()
    return "Updated GiantTarList"

def makeLightCurves(dat_df,md,triggermjd,season, datfile, outdir, post):   
 
    Mjd = np.asarray(dat_df['MJD'].values)
    triggermjd = float(triggermjd)
    try:
        Time = Mjd - triggermjd
    except:
        Time=0

    bd = {}
    for filter_ in dat_df['FLT'].values:
        bd[filter_] = [[],[],[]]
        
    for i in range(len(dat_df['OBJID'].values)):
        if dat_df['OBJID'].values[i] != 0.0:
            if dat_df['FLUXCAL'].values[i] == 0.0:
                m = 99
                magErr = 0.0
            else:
                m=-2.5*np.log10(dat_df['FLUXCAL'].values[i])+27.5
                magErr=ErrorMag(dat_df['FLUXCAL'].values[i],dat_df['FLUXCALERR'].values[i])
            bd[dat_df['FLT'].values[i]][0].append(m)
            bd[dat_df['FLT'].values[i]][1].append(Time[i])
            bd[dat_df['FLT'].values[i]][2].append(magErr)
    
    fig = plt.figure()
    ax = fig.gca()
    for b in bd.keys():
        if not bd[b]==[[],[],[]]:
            myyerr=np.asarray(bd[b][2])
            plt.errorbar(bd[b][1],bd[b][0],yerr=myyerr, fmt='o', label=b+" band")               

    ax.grid()
    ax.invert_yaxis()
    ax.legend(title="SNID: " + str(md['snid'].values[0]) + "\n Season: " + str(season),bbox_to_anchor=(1.1,.9), bbox_transform=plt.gcf().transFigure)
    plt.xlabel('Days Since Merger')
    plt.ylabel('Magnitude')


    LightCurveName='LightCurve_'+datfile.split('.')[1].split('/')[-1]+'.png'
    fig.savefig(str(outdir)+'/pngs/'+str(LightCurveName),bbox_inches='tight')
    full = str(outdir)+'/pngs/'+str(LightCurveName)
    plt.close(fig)
#    if post == True:
#        os.system("rsync -aR "+full+" codemanager@desweb.fnal.gov:/des_web/www/html/desgw/post-processing-all/dp"+str(season)+"/")

    return LightCurveName



def make_obj_and_stamp_dict(dat_df,season,schema, outdir, post, MLcutoff=0.7):
    # Create dictionary with datfile data per objid
    objidDict = {}
    
    objidStampDict = {}
#    MLcutoff = 0.7

    for mjdk,bandk,fieldk,fluxcalk,fluxcalerrk,photflagk,photprobk,zpfluxk,psfk,skysigk,skysig_tk,gaink,xpixk,ypixk,nitek,expnumk,ccdnumk,objidk in zip(dat_df['MJD'].values,dat_df['FLT'].values,dat_df['FIELD'].values,dat_df['FLUXCAL'].values,dat_df['FLUXCALERR'].values,dat_df['PHOTFLAG'].values,dat_df['PHOTPROB'].values,dat_df['ZPFLUX'].values,dat_df['PSF'].values,dat_df['SKYSIG'].values,dat_df['SKYSIG_T'].values,dat_df['GAIN'].values,dat_df['XPIX'].values,dat_df['YPIX'].values,dat_df['NITE'].values,dat_df['EXPNUM'].values,dat_df['CCDNUM'].values,dat_df['OBJID'].values):

        if float(objidk) != 0.0:
            if float(fluxcalk) == 0.0:
                mk = 99
                merrk = 0.0
            else:
                mk = -2.5*np.log10(float(fluxcalk))+27.5
                merrk = ErrorMag(float(fluxcalk),float(fluxcalerrk))

            mk = str(mk)
            merrk = str(merrk)
        
            objidDict[objidk] = [mjdk,bandk,fieldk,fluxcalk,fluxcalerrk,mk,merrk,photflagk,photprobk,zpfluxk,psfk,skysigk,skysig_tk,gaink,xpixk,ypixk,nitek,expnumk,ccdnumk]

    # Extract stamps per objid and create dictionary with stamps per objid
            tarFiles = glob('/pnfs/des/persistent/'+schema+'/exp/'+str(int(nitek))+'/'+str(int(expnumk))+'/dp'+str(int(season))+'/'+str(bandk)+'_'+str(int(ccdnumk))+'/stamps_'+str(int(nitek))+'_*_'+str(bandk)+'_'+str(int(ccdnumk))+'/*.tar.gz')
 #           print(tarFiles)
            try:
                if float(photprobk) < MLcutoff:
                    #print("This observation has an ML score less than %f, so no stamps will be extracted." %MLcutoff)
                    objidStampDict[objidk] = []
                    continue
                tarFile = tarFiles[0]
                tar = tarfile.open(tarFile)
                try:
                    for stamp in ['temp' + str(int(objidk)) + '.gif','srch' + str(int(objidk)) + '.gif','diff' + str(int(objidk)) + '.gif']:
                        tar.extract(stamp, path=str(outdir)+'/stamps/')
                    gifs = glob(str(outdir)+'/stamps/*'+str(int(objidk))+'.gif')
                    #os.system('mv *'+str(int(objidk))+'.gif '+str(datadir)+'/stamps/')
                    gifs = sorted(gifs)
                    #gifs = [gifs[2], gifs[1], gifs[0]]
                    gifs = ['./stamps/'+gifs[2].split('/')[-1], './stamps/'+gifs[1].split('/')[-1], './stamps/'+gifs[0].split('/')[-1]]
                    objidStampDict[objidk] = gifs
                    if post == True:
                        for gif in gifs:
                            #full = str(outdir)+'/stamps/'+str(gif)
                            full = str(outdir)+'/'+str(gif)
#                            os.system("rsync -aR "+full+" codemanager@desweb.fnal.gov:/des_web/www/html/desgw/post-processing-all/dp"+str(season)+"/")

                except KeyError:
                    print("The stamps you are looking for are not here")
                    objidStampDict[objidk] = []
                    continue
                tar.close()
            except IndexError:
                print("No tarball matching your criteria")
                objidStampDict[objidk] = []
                continue
    return objidDict, objidStampDict

def createHTML(dat_df,season,triggermjd,schema, objidDict, objidStampDict, md, datfile, MLcutoff, outdir, post, c=0):
#    lightCurve = makeLightCurves(datfile,band,fluxcal,fluxcalerr,mjd,nite,objid,snid,skipheader,triggermjd,season)
    lightCurve = makeLightCurves(dat_df,md,triggermjd,season, datfile, outdir, post)

    name = 'candidate_' + str(int(md['snid'].values[0])) + '_dp' + str(season) + '.html'
    
    topLines=['<!DOCTYPE HTML>\n','<html>\n','<head>',
          '<link rel="stylesheet" type="text/css" href="../../theProtoATCStyleSheet.css">', '<script src = "candJava.js" type = "text/javascript"/></script>',
          '<title> SNID ' + str(md['snid'].values[0]) + ' Data </title>\n','<h1> SNID ' + str(md['snid'].values[0]) + ' Data </h1>','\n','</head>\n',
          '<body>','<p> Stamps from observations with ML scores less than ' + str(MLcutoff) + ' are not displayed. </p>',
          '<table align="center">\n','<caption>Candidate Info</caption>','<tr>','<th>RA</th>\n','<td>' + str(md['raval'].values[0]) + '</td>\n',
          '<th>DEC</th>\n','<td>' + str(md['decval'].values[0])  + '</td>\n','</tr>','<th>Host final_z</th>\n','<td>' + str(md['redshift_final'].values[0]) + '</td>\n','<th>Host final_z Error</th>\n','<td>' + str(md['redshift_finalerr'].values[0])  + '</td>\n','</tr>','<tr>','<th> Trigger MJD</th>','<td>'+str(triggermjd)+'</td>','<th>GWID</th>','<td> -- </td>','</tr>','<tr>','<th>AREA</th>','<td> -- </td>','<th>FAR</th>','<td> -- </td>','</tr>','</table>\n']

    openingLines=['<p> Click any of OBJID, FLUXCAL, PHOTFLAG, SKYSIG, and NITE to expand the hidden columns. Hover mouse over each to reveal hidden options.</p>','<table id="mytable" width="750" align="center">','<caption>Observation Info</caption>','<tr>','<th title="OBJID, MJD, BAND, FIELD" onclick="toggleColumn(1)">OBJID</th>','<th class="col1">MJD</th>','<th class="col1">FLT</th>','<th class="col1">FIELD</th>','<th title="FLUXCAL, FLUXCALERR, MAG, MAGERR" onclick="toggleColumn(2)">FLUXCAL</th>','<th class="col2">FLUXCALERR</th>','<th class="col2">MAG</th>','<th class="col2">MAGERR</th>','<th title="PHOTFLAG, PHOTPROB, ZPFLUX, PSF" onclick="toggleColumn(3)">PHOTFLAG</th>','<th class="col3">PHOTPROB</th>','<th class="col3">ZPFLUX</th>','<th class="col3">PSF</th>','<th title="SKYSIG, SKYSIG_T, GAIN, XPIX, YPIX"  onclick="toggleColumn(4)">SKYSIG</th>','<th class="col4">SKYSIG_T</th>','<th class="col4">GAIN</th>','<th class="col4">XPIX</th>','<th class="col4">YPIX</th>','<th title="NITE, EXPNUM, CCDNUM"  onclick="toggleColumn(5)">NITE</th>','<th class="col5">EXPNUM</th>','<th class="col5">CCDNUM</th>','</tr>\n']

    infoTablines = []
    mjdDict = {}
    for obs,mjds in zip(dat_df['OBJID'].values,dat_df['MJD'].values):
        mjdDict[mjds] = obs
#    for obs in objidDict.keys():
    for imjd, obs in sorted(mjdDict.items()):
        mjd_,band_,field_,fluxcal_,fluxcalerr_,m_,merr_,photflag_,photprob_,zpflux_,psf_,skysig_,skysig_t_,gain_,xpix_,ypix_,nite_,expnum_,ccdnum_ = objidDict[obs]
        tableLines=['<tr>','<td>' + str(obs) + '</td>','<td class="col1">'+str(mjd_)+'</td>','<td class="col1">'+str(band_)+'</td>','<td class="col1">'+str(field_)+'</td>','<td>'+str(fluxcal_)+'</td>','<td class="col2">'+str(fluxcalerr_)+'</td class="col2">','<td class="col2">'+str(m_)+'</td>','<td class="col2">'+str(merr_)+'</td>','<td>'+str(photflag_)+'</td >','<td class="col3">'+str(photprob_)+'</td>','<td class="col3">'+str(zpflux_)+'</td>','<td class="col3">'+str(psf_)+'</td>','<td>'+str(skysig_)+'</td>','<td class="col4">'+str(skysig_t_)+'</td>','<td class="col4">'+str(gain_)+'</td>','<td class="col4">'+str(xpix_)+'</td>','<td class="col4">'+str(ypix_)+'</td>','<td>'+str(nite_)+'</td>','<td class="col5">'+str(expnum_)+'</td>','<td class="col5">'+str(ccdnum_)+'</td>','</tr>\n']
        
        infoTablines += tableLines
    closingLines=['</table>']
    infoTablines += closingLines

    observations = []
#    mjdDict = {}
#    for obs,mjds in zip(dat_df['OBJID'].values,dat_df['MJD'].values):
#        mjdDict[mjds] = obs
    mjds = list(mjdDict.keys())
    mjds.sort()
    #orderedObs = []
    #for MJD__ in mjds:
    #    orderedObs.append(mjdDict[MJD__])
    for modifiedjliandate in mjds:
        obs = mjdDict[modifiedjliandate]
        if float(obs) == 0.0:
            continue
        if objidStampDict[obs] == []:
            continue
#        indivObs = ['<h1>OBJID: '+str(obs)+'</h1><h2>MJD: '+str(modifiedjliandate)+'</h2>\n','<h2> Template, Search, Difference</h2>\n','<div id="gifs">','<span title=Temp>','<img src=\''+str(objidStampDict[obs][0])+'\' width="200" height="200"/></span>\n','<span title=Search>','<img src=\''+str(objidStampDict[obs][1])+'\' width="200" height="200"/></span>\n','<span title=Diff><img src=\''+str(objidStampDict[obs][2])+'\' width="200" height="200"/></span>','</div>\n']
        indivObs = ['<h1>OBJID: '+str(obs)+'</h1><h2>MJD: '+str(modifiedjliandate)+'</h2>\n','<h2> Template, Search, Difference</h2>\n','<div id="gifs">','<span title=Temp>','<img src=\'../'+str(objidStampDict[obs][0])+'\' width="200" height="200"/></span>\n','<span title=Search>','<img src=\'../'+str(objidStampDict[obs][1])+'\' width="200" height="200"/></span>\n','<span title=Diff><img src=\'../'+str(objidStampDict[obs][2])+'\' width="200" height="200"/></span>','</div>\n']
        observations += indivObs

    collectedObs = ['<h1>Collected Observations Ordered by MJD</h1>\n','<h2>Temp, Search, Diff</h2>\n']
    for mjd, obs in sorted(mjdDict.items()):
        #mjdDict[mjds] = obs
        if float(obs) == 0.0:
            continue
        if objidStampDict[obs] == []:
            continue
        newLines=['<div id="gifs">','<span title=Temp>','<img src=\'../'+str(objidStampDict[obs][0])+'\' width="100" height="100"/></span>\n','<span title=Search>','<img src=\'../'+str(objidStampDict[obs][1])+'\' width="100" height="100"/></span>\n','<span title=Diff><img src=\'../'+str(objidStampDict[obs][2])+'\' width="100" height="100"/></span>','</div>\n']
        collectedObs += newLines

    LightLines=['<h1>Light Curve</h1>','<img src=\'../pngs/'+str(lightCurve)+'\'width="533" height="400">']

#    weirdHostGalStuff = ['<p>HOSTGAL_SB_FLUXCAL: '+hsbf+'</p>\n']

    # General Table

    gentable = ['<table align="center">\n','<caption>General Data</caption>\n','<tr>\n','<th scope="row">SURVEY</th>\n','<td>' + str(md['survey'].values[0])  + '</td>\n','</tr>','<tr>','<th scope="row">SNID</th>\n','<td>' + str(md['snid'].values[0])  + '</td>\n','</tr>','<tr>','<th scope="row">PIXSIZE</th>\n','<td>' + str(md['pixsize'].values[0]) + '</td>\n','</tr>','<tr>','<th scope="row">RA</th>\n','<td>' + str(md['raval'].values[0]) + '</td>\n','</tr>','<tr>','<th scope="row">DEC</th>\n','<td>' + str(md['decval'].values[0]) + '</td>\n','</tr>','<tr>','<th scope="row">MWEBV</th>\n','<td>' + str(md['mwebv'].values[0]) + '</td>\n','</tr>','<tr>','<th scope="row">MWBEV_ERR</th>\n','<td>' + str(md['mwebv_err'].values[0]) + '</td>\n','</tr>','<tr>','<th scope="row">REDSHIFT_HELIO</th>\n','<td>' + str(md['redshift_helio'].values[0]) + '</td>\n','</tr>','<tr>','<th scope="row">REDSHIFT_FINAL</th>\n','<td>' + str(md['redshift_final'].values[0]) + '</td>\n','</tr>\n','</table>\n', '<tr>','<th scope="row">REDSHIFT_FINALERR</th>\n','<td>' + str(md['redshift_finalerr'].values[0]) + '</td>\n','</tr>\n','</table>\n']

    # Host Data
    
    hosttable = ['<table align="center">\n','<caption>Host Galaxy Info</caption>\n']
    content = ['<tr>\n','<td></td>']
#    for hdlrrank in host_dlrrank:
#        content += ['<th scope="col">'+hdlrrank+'</th>']
    content += ['</tr>\n','<tr>\n','<th scope="row">HOSTGAL_OBJID</th>\n']
    content += ['<td>'+str(md['host_id'].values[0])+'</td>']
    content += ['</tr>\n','<tr>','<th scope="row">HOSTGAL_PHOTOZ</th>']
    content += ['<td>'+str(md['photo_z'].values[0])+'</td>']
    content += ['</tr>\n','<tr>','<th scope="row">HOSTGAL_SPECZ</th>']
    content += ['<td>'+str(md['spec_z'].values[0])+'</td>']
    content += ['</tr>\n','<tr>','<th scope="row">HOSTGAL_RA</th>']
    content += ['<td>'+str(md['host_ra'].values[0])+'</td>'] 
    content += ['</tr>\n','<tr>','<th scope="row">HOSTGAL_DEC</th>'] 
    content += ['<td>'+str(md['host_dec'].values[0])+'</td>']
    content += ['</tr>\n','<tr>','<th scope="row">HOSTGAL_SNSEP</th>']
    content += ['<td>'+str(md['host_sep'].values[0])+'</td>'] 
    content += ['</tr>\n','<tr>','<th scope="row">HOSTGAL_GMAG</th>']
    content += ['<td>'+str(md['h_gmag'].values[0])+'</td>']
    content += ['</tr>\n','<tr>','<th scope="row">HOSTGAL_IMAG</th>']
    content += ['<td>'+str(md['h_imag'].values[0])+'</td>']
    content += ['</tr>\n','<tr>','<th scope="row">HOSTGAL_RMAG</th>']
    content += ['<td>'+str(md['h_rmag'].values[0])+'</td>']
    content += ['</tr>\n','<tr>','<th scope="row">HOSTGAL_ZMAG</th>']
    content += ['<td>'+str(md['h_zmag'].values[0])+'</td>']
    content += ['</tr>\n','</table>\n'] 

    # Private Table

#    privatetable = ['<table align="center">\n','<caption>Private Data</caption>\n',]
#    for key,value in zip(privateKey,privateValue):
#        privatetable += ['<tr>\n','<th scope="row">'+key+'</th>\n','<td>'+value+'</td>','</tr>']
#    privatetable += ['</tr>\n','</table>\n']

#    extraLines = gentable + weirdHostGalStuff + hosttable + content + privatetable
    extraLines = gentable + hosttable + content

    bottomLines=['</body>\n','</html>']

    allLines = topLines + openingLines + infoTablines + observations + collectedObs + LightLines + extraLines + bottomLines

    candPage = open(str(outdir)+'/htmls/'+str(name),'w+')
    for tag in allLines:
        candPage.write(tag)
    candPage.close()

    del allLines
    del bottomLines
    del extraLines
    del openingLines
    del infoTablines
    del observations
    del collectedObs
    del LightLines

##    if post == True:
##        try:
### dont use        subprocess.Popen(['rsync','-a','*.html',' codemanager@desweb.fnal.gov:/des_web/www/html/desgw/post-processing-all/'])
##            full = str(outdir)+'/htmls/'+str(name)
##            os.system("rsync -aR "+full+" codemanager@desweb.fnal.gov:/des_web/www/html/desgw/post-processing-all/dp"+str(season)+"/")
##
##        except:
##            print("Can't rsync right now. Try again once Post-Processing has finished.")
    
#    os.system('mv '+str(name)+' '+str(datadir)+'/htmls/')
#    snid = md['snid'].values[0]
#    print("html for "+str(snid)+" done.")
    return 

def make_full_array(dat_df, md, datfile, c):
    MJD,BAND,FIELD,FLUXCAL,FLUXCALERR,PHOTFLAG,PHOTPROB,ZPFLUX,PSF,SKYSIG,SKYSIG_T,GAIN,XPIX,YPIX,NITE,EXPNUM,CCDNUM,OBJID = [],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[]
    RA,DEC,CAND_ID,DATAFILE,SN_ID = [],[],[],[],[]
    HOSTID,PHOTOZ,PHOTOZERR,SPECZ,SPECZERR,HOSTSEP,HOST_GMAG,HOST_RMAG,HOST_IMAG,HOST_ZMAG = [],[],[],[],[],[],[],[],[],[]

    MJD += [float(i) for i in dat_df['MJD'].values]
    BAND += [str(i) for i in dat_df['FLT'].values]
    FIELD += [str(i) for i in dat_df['FIELD'].values]
    FLUXCAL += [float(i) for i in dat_df['FLUXCAL'].values]
    FLUXCALERR += [float(i) for i in dat_df['FLUXCALERR'].values]
    PHOTFLAG += [float(i) for i in dat_df['PHOTFLAG'].values]
    PHOTPROB += [float(i) for i in dat_df['PHOTPROB'].values]
    ZPFLUX += [float(i) for i in dat_df['ZPFLUX'].values]
    PSF += [float(i) for i in dat_df['PSF'].values]
    SKYSIG += [float(i) for i in dat_df['SKYSIG'].values]
    SKYSIG_T += [float(i) for i in dat_df['SKYSIG_T'].values]
    GAIN += [float(i) for i in dat_df['GAIN'].values]
    XPIX += [float(i) for i in dat_df['XPIX'].values]
    YPIX += [float(i) for i in dat_df['YPIX'].values]
    NITE += [float(i) for i in dat_df['NITE'].values]
    EXPNUM += [float(i) for i in dat_df['EXPNUM'].values]
    CCDNUM += [float(i) for i in dat_df['CCDNUM'].values]
    OBJID += [float(i) for i in dat_df['OBJID'].values]

    n = len(dat_df['OBJID'].values)

    RA += [float(md['raval'].values[0])] * n
    DEC += [float(md['decval'].values[0])] * n
    CAND_ID += [float(c)] * n
    DATAFILE += [datfile.split('/')[-1]] * n
    SN_ID += [float(md['snid'].values[0])] * n

    HOSTID += [float(md['host_id'].values[0]) for i in range(n)]
    PHOTOZ += [float(md['photo_z'].values[0]) for i in range(n)]
    PHOTOZERR += [float(md['photo_zerr'].values[0]) for i in range(n)]
    SPECZ += [float(md['spec_z'].values[0]) for i in range(n)]
    SPECZERR += [float(md['spec_zerr'].values[0]) for i in range(n)]
    HOSTSEP += [float(md['host_sep'].values[0]) for i in range(n)]
    HOST_GMAG += [float(md['h_gmag'].values[0]) for i in range(n)]
    HOST_RMAG += [float(md['h_rmag'].values[0]) for i in range(n)]
    HOST_IMAG += [float(md['h_imag'].values[0]) for i in range(n)]
    HOST_ZMAG += [float(md['h_zmag'].values[0]) for i in range(n)]

    return MJD,BAND,FIELD,FLUXCAL,FLUXCALERR,PHOTFLAG,PHOTPROB,ZPFLUX,PSF,SKYSIG,SKYSIG_T,GAIN,XPIX,YPIX,NITE,EXPNUM,CCDNUM,OBJID,RA,DEC,CAND_ID,DATAFILE,SN_ID,HOSTID,PHOTOZ,PHOTOZERR,SPECZ,SPECZERR,HOSTSEP,HOST_GMAG,HOST_RMAG,HOST_IMAG,HOST_ZMAG


def open_dat(dat_file):
    infile = open(dat_file, 'r')
    lines = infile.readlines()
    infile.close()
    return lines

def get_dat_data(lines):

    columns = [y for y in [x.split(' ') for x in lines if x[0:8] == 'VARLIST:'][0] if y != ''][1:-1]
    data = [[y for y in x.split(' ') if y != ''][1:-1] for x in lines if x[0:4] == 'OBS:']
    df = pd.DataFrame(data=data, columns=columns)
    df = df[df.OBJID != '0']
    df = df.reset_index(drop=True)

    for col in columns:
        if col != 'FLT' and col != 'FIELD':
            #df[col] = pd.to_numeric(df[col])
            df[col] = df[col].astype(float)

    return df

def get_metadata(lines, hashost): 
    survey = str([y for y in [x for x in lines if x[0:6] == 'SURVEY'][0].split(' ') if y != ''][1])
    snid = float([val for val in [line for line in lines if line[0:4]=='SNID'][0].split(' ') if val !=''][1])
    raval = float([y for y in [x for x in lines if x[0:2] == 'RA'][0].split(' ') if y != ''][1])
    decval = float([y for y in [x for x in lines if x[0:3] == 'DEC'][0].split(' ') if y != ''][1])
    redshift_final = float([y for y in [x for x in lines if x[0:14] == 'REDSHIFT_FINAL'][0].split(' ') if y != ''][1])
    redshift_finalerr = float([y for y in [x for x in lines if x[0:14] == 'REDSHIFT_FINAL'][0].split(' ') if y != ''][3])
    redshift_helio = float([y for y in [x for x in lines if x[0:14] == 'REDSHIFT_HELIO'][0].split(' ') if y != ''][1])
    pixsize = float([y for y in [x for x in lines if x[0:7] == 'PIXSIZE'][0].split(' ') if y != ''][1])
    mwebv = float([y for y in [x for x in lines if x[0:5] == 'MWEBV'][0].split(' ') if y != ''][1])
    mwebv_err = float([y for y in [x for x in lines if x[0:9] == 'MWEBV_ERR'][0].split(' ') if y != ''][1])
    if hashost:
        host_id = float([y for y in [x for x in lines if x[0:13]=='HOSTGAL_OBJID'][0].split(' ') if y !=''][1])
        photo_z = float([y for y in [x for x in lines if x[0:14]=='HOSTGAL_PHOTOZ'][0].split(' ') if y !=''][1])
        photo_zerr = float([y for y in [x for x in lines if x[0:14]=='HOSTGAL_PHOTOZ'][0].split(' ') if y !=''][3])
        spec_z = float([y for y in [x for x in lines if x[0:13]=='HOSTGAL_SPECZ'][0].split(' ') if y !=''][1])
        spec_zerr = float([y for y in [x for x in lines if x[0:13]=='HOSTGAL_SPECZ'][0].split(' ') if y !=''][3])
        host_sep = float([y for y in [x for x in lines if x[0:13]=='HOSTGAL_SNSEP'][0].split(' ') if y !=''][1])
        h_gmag = float([y for y in [x for x in lines if x[0:11]=='HOSTGAL_MAG'][0].split(' ') if y !=''][1])
        h_rmag = float([y for y in [x for x in lines if x[0:11]=='HOSTGAL_MAG'][0].split(' ') if y !=''][2])
        h_imag = float([y for y in [x for x in lines if x[0:11]=='HOSTGAL_MAG'][0].split(' ') if y !=''][3])
        h_zmag = float([y for y in [x for x in lines if x[0:11]=='HOSTGAL_MAG'][0].split(' ') if y !=''][4])
        host_ra = float([y for y in [x for x in lines if x[0:10]=='HOSTGAL_RA'][0].split(' ') if y !=''][1])
        host_dec = float([y for y in [x for x in lines if x[0:11]=='HOSTGAL_DEC'][0].split(' ') if y !=''][1])
    else:
        host_id = -9
        photo_z = -9.0
        photo_zerr = -9.0
        spec_z = -9.0
        spec_zerr = -9.0
        host_sep = 999.0
        h_gmag = 888.0
        h_rmag = 888.0
        h_imag = 888.0
        h_zmag = 888.0
        host_ra = 999.0
        host_dec = 999.0
        
    
    mddict = {'snid':[snid],'raval':[raval],'decval':[decval],'host_id':[host_id],'photo_z':[photo_z],'photo_zerr':[photo_zerr],'spec_z':[spec_z],'spec_zerr':[spec_zerr],'host_sep':[host_sep],'h_gmag':[h_gmag],'h_rmag':[h_rmag],'h_imag':[h_imag],'h_zmag':[h_zmag], 'survey':[survey], 'redshift_final':[redshift_final], 'redshift_finalerr':[redshift_finalerr], 'pixsize':[pixsize], 'redshift_helio':[redshift_helio], 'mwebv':[mwebv], 'mwebv_err':[mwebv_err], 'host_ra':[host_ra], 'host_dec':[host_dec]}
    md = pd.DataFrame(mddict)

    del snid, raval, decval, host_id, photo_z, photo_zerr, spec_z, spec_zerr, host_sep, h_gmag, h_rmag, h_imag, h_zmag, survey, redshift_final, redshift_finalerr, pixsize, redshift_helio, mwebv, mwebv_err,host_ra,host_dec
    return md
    

### END NS NEW FUNCTIONS
 
############ doALL #############
def doAll(outdir, season,triggermjd,path,c,allgood,masterTableInfo,MJD,BAND,FIELD,FLUXCAL,FLUXCALERR,PHOTFLAG,PHOTPROB,ZPFLUX,PSF,SKYSIG,SKYSIG_T,GAIN,XPIX,YPIX,NITE,EXPNUM,CCDNUM,OBJID,RA,DEC,CAND_ID,DATAFILE,SN_ID,HOSTID,PHOTOZ,PHOTOZERR,SPECZ,SPECZERR,HOSTSEP,HOST_GMAG,HOST_RMAG,HOST_IMAG,HOST_ZMAG,post,d):

#    c=c+1
#    print(c)
#    if c%1000==0:
#        print(c)
        #break                                                             
        
#    stamps4file=[]##Stamps found for dat file
    filename = d.split('\n')[0]
    datfile = os.path.join(path,filename)
    f = open(datfile,'r+')
    lines = f.readlines()
    f.close()
    del f

    hashost = False
    for myline in lines:
        if myline == '\n':
            continue
        splitline=myline.split()
        if splitline[0] == "HOSTGAL_NMATCH2:":
            hashost = True
    del splitline
    
    dat_df = get_dat_data(lines)
    md = get_metadata(lines, hashost)
    del lines

    ##ag test
    schema = 'gw'
    MLcutoff = 0.7
    ##
#    MJD_1,BAND_1,FIELD_1,FLUXCAL_1,FLUXCALERR_1,PHOTFLAG_1,PHOTPROB_1,ZPFLUX_1,PSF_1,SKYSIG_1,SKYSIG_T_1,GAIN_1,XPIX_1,YPIX_1,NITE_1,EXPNUM_1,CCDNUM_1,OBJID_1,RA_1,DEC_1,CAND_ID_1,DATAFILE_1,SN_ID_1,HOSTID_1,PHOTOZ_1,PHOTOZERR_1,SPECZ_1,SPECZERR_1,HOSTSEP_1,HOST_GMAG_1,HOST_RMAG_1,HOST_IMAG_1,HOST_ZMAG_1 = createHTML(datfile,myskipheader,str(season),str(triggermjd),str(schema),c)
    
    objidDict, objidStampDict = make_obj_and_stamp_dict(dat_df,season,schema, outdir, post, MLcutoff)
    createHTML(dat_df,season,triggermjd,schema, objidDict, objidStampDict, md, datfile,MLcutoff, outdir, post, c)

    ##----------- alyssa hack to make csv and event table-------------------
    mypaths=[]
    expnums = dat_df['EXPNUM'].values
    nites = dat_df['NITE'].values
    ccdnum = dat_df['CCDNUM'].values
    for i in range(len(nites)):
        mypaths.append('/pnfs/des/persistent/gw/exp/'+str(int(nites[i]))+'/'+str(int(expnums[i]))+'/D00'+str(int(expnums[i]))+'_*_'+str(int(ccdnum[i]))+'_r4p7_immask.fits.fz') #path to se proc. image

        #only list candidates with at least one exposure whose ml score is >= 0.7
    fluxcal = dat_df['FLUXCAL'].values
    m = []    
    try:
        for flux in fluxcal:
            if flux != 0:
                m.append(-2.5*np.log10(flux)+27.5)
        m = np.asarray(m)
        bestMag = min(m)
    except:
        bestMag = 99

    try:
        highestPhotProb = max(dat_df['PHOTPROB'].values)
    except:
        print("no phot prob ", dat_df)
        highestPhotProb = -999

    if highestPhotProb >= 0.7:
        masterTableInfo[md['snid'].values[0]]=[(float(md['raval'].values[0]),float(md['decval'].values[0])),float(highestPhotProb),float(bestMag),str(mypaths)]
#        print('Writing to masterTableInfo for SNID ' + str(md['snid'].values[0]))
#    print('Keys after ' + str(d) + ': ' + str(masterTableInfo.keys()))
#    writer = csv.writer(FollowupList)
#    sequence = [[str(datInfo[0]), str(datInfo[1]), str(datInfo[2]), str(highestPhotProb),float(bestMag), str(mypaths)]]
#    writer.writerows(sequence)
        
#    if all([int(x)==12288 for x in PHOTFLAG_1]):
    allgood+=1

    return masterTableInfo
#MJD,BAND,FIELD,FLUXCAL,FLUXCALERR,PHOTFLAG,PHOTPROB,ZPFLUX,PSF,SKYSIG,SKYSIG_T,GAIN,XPIX,YPIX,NITE,EXPNUM,CCDNUM,OBJID,RA,DEC,CAND_ID,DATAFILE,SN_ID,HOSTID,PHOTOZ,PHOTOZERR,SPECZ,SPECZERR,HOSTSEP,HOST_GMAG,HOST_RMAG,HOST_IMAG,HOST_ZMAG

################################


def combinedatafiles(season,master,fitsname,outdir, datadir, schema,triggermjd, post=False):
#    gc.set_debug(gc.DEBUG_LEAK)
    gc.set_threshold(300,5,5)
    config = configparser.ConfigParser()
    config.read('postproc_'+season+'.ini')
    #mySEASON=config.get('general','season')
    mySEASON=season

    #season = os.environ.get('SEASON')
    season = str(season)
    print('Starting combinedatafiles')
    #mlist = Table.read(master)
    #masdf = mlist.to_pandas()
    mlist = fitsio.read(master)
    mlist = mlist.byteswap().newbyteorder()
    masdf = pd.DataFrame.from_records(mlist)

    path = os.path.join(os.environ.get('ROOTDIR2'), 'makedatafiles')
    fitsname= os.path.join(path,fitsname)
    path = os.path.join(path,datadir)
    
    if os.path.isfile(fitsname):
        print('A combined .fits file for all real candidates already exists in the specified outdir with the specified name:')
        print("")
        print(fitsname)
        print("")
        print('If you want to recreate the file, either change the combined_fits key under the [GWmakeDataFiles-real] heading in the .ini file, or simply delete the existing one.')
        print("")
        
        status=False
        
        return fitsname, status, None

    if post == True:
        os.system('ssh codemanager@desweb.fnal.gov "mkdir -p /des_web/www/html/desgw/post-processing-all/dp' + str(season) + '/"')

    dats = os.listdir(path)
    dats = [x for x in dats if '.dat' in x]

    hostlist = []

    MJD,BAND,FIELD,FLUXCAL,FLUXCALERR,PHOTFLAG,PHOTPROB,ZPFLUX,PSF,SKYSIG,\
        SKYSIG_T,GAIN,XPIX,YPIX,NITE,EXPNUM,CCDNUM,OBJID = [],[],[],[],[],[],\
        [],[],[],[],[],[],[],[],[],[],[],[]

    RA,DEC,CAND_ID,DATAFILE,SN_ID = [],[],[],[],[]
    HOSTID,PHOTOZ,PHOTOZERR,SPECZ,SPECZERR,HOSTSEP,HOST_GMAG,HOST_RMAG,HOST_IMAG,\
        HOST_ZMAG = [],[],[],[],[],[],[],[],[],[]

    c=0
    allgood=0

    masterTableInfo={} ###Key by snid, provide RA and DEC, probability, nad Gal Dist

    # GTL=getGTL()##List of tar files already extracted
    
#    FollowupList=open('FollowupList'+str(season)+'.csv','w')
    mytime = datetime.datetime.now().strftime('%Y%m%d_%H:%M:%S')
    print("START DO ALL TIME", mytime)

    nparallel = 16
    #Parallel(n_jobs=nparallel)(delayed(doAll)(outdir, season,triggermjd,path,c,allgood,masterTableInfo,MJD,BAND,FIELD,FLUXCAL,FLUXCALERR,PHOTFLAG,PHOTPROB,ZPFLUX,PSF,SKYSIG,SKYSIG_T,GAIN,XPIX,YPIX,NITE,EXPNUM,CCDNUM,OBJID,RA,DEC,CAND_ID,DATAFILE,SN_ID,HOSTID,PHOTOZ,PHOTOZERR,SPECZ,SPECZERR,HOSTSEP,HOST_GMAG,HOST_RMAG,HOST_IMAG,HOST_ZMAG,post,d) for d in dats)
    masterTableInfo = Parallel(n_jobs=nparallel)(delayed(doAll)(outdir, season,triggermjd,path,c,allgood,masterTableInfo,MJD,BAND,FIELD,FLUXCAL,FLUXCALERR,PHOTFLAG,PHOTPROB,ZPFLUX,PSF,SKYSIG,SKYSIG_T,GAIN,XPIX,YPIX,NITE,EXPNUM,CCDNUM,OBJID,RA,DEC,CAND_ID,DATAFILE,SN_ID,HOSTID,PHOTOZ,PHOTOZERR,SPECZ,SPECZERR,HOSTSEP,HOST_GMAG,HOST_RMAG,HOST_IMAG,HOST_ZMAG,post,d) for d in dats)
    gc.collect()
    # Now joblib actually returns a list of single-key dictionaries above, rather than the singl many-key dict that we originally wanted. So convert back now
    masterTableInfo = {k: v for d in masterTableInfo for k, v in d.items()}
    print("PARALLEL DONE")
    mytime = datetime.datetime.now().strftime('%Y%m%d_%H:%M:%S')
    print("END DO ALL TIME", mytime)
    print('masterTableInfo keys:')
    print(str(masterTableInfo.keys()))
    # Copy stamps, html and lightcurve directories if post == True

    if post == True:
        try:
            os.system("rsync -a " + outdir + "/pngs codemanager@desweb.fnal.gov:/des_web/www/html/desgw/post-processing-all/dp" + str(season) +"/") 
        except:
            print("Error copying png directory to desweb. Please investigate.")
        try:
            os.system("rsync -a " + outdir + "/htmls codemanager@desweb.fnal.gov:/des_web/www/html/desgw/post-processing-all/dp" + str(season) +"/") 
        except:
            print("Error copying htmls directory to desweb. Please investigate.")
        try:
            os.system("rsync -a " + outdir + "/stamps codemanager@desweb.fnal.gov:/des_web/www/html/desgw/post-processing-all/dp" + str(season) +"/") 
        except:
            print("Error copying stamps directory to desweb. Please investigate.")

    mytime = datetime.datetime.now().strftime('%Y%m%d_%H:%M:%S')
    print("END COPY TO desweb", mytime)
    mytime = datetime.datetime.now().strftime('%Y%m%d_%H:%M:%S')
    print("START new for d in dates TIME", mytime)
    for d in dats:
        c=c+1
        if c%1000==0:
            print(c)

        filename = d.split('\n')[0]
        datfile = os.path.join(path,filename)
        f = open(datfile,'r+')
        lines = f.readlines()
        f.close()

        hashost = False
        for myline in lines:
            if myline == '\n':
                continue
            splitline=myline.split()
            if splitline[0] == "HOSTGAL_NMATCH2:":
                hashost = True

        dat_df = get_dat_data(lines)
        md = get_metadata(lines, hashost)

        MJD_1,BAND_1,FIELD_1,FLUXCAL_1,FLUXCALERR_1,PHOTFLAG_1,PHOTPROB_1,ZPFLUX_1,PSF_1,SKYSIG_1,SKYSIG_T_1,GAIN_1,XPIX_1,YPIX_1,NITE_1,EXPNUM_1,CCDNUM_1,OBJID_1,RA_1,DEC_1,CAND_ID_1,DATAFILE_1,SN_ID_1,HOSTID_1,PHOTOZ_1,PHOTOZERR_1,SPECZ_1,SPECZERR_1,HOSTSEP_1,HOST_GMAG_1,HOST_RMAG_1,HOST_IMAG_1,HOST_ZMAG_1 = make_full_array(dat_df, md, datfile, c)
        
        RA += RA_1
        DEC += DEC_1
        CAND_ID += CAND_ID_1
        SN_ID += SN_ID_1
        HOSTID += HOSTID_1
        PHOTOZ += PHOTOZ_1
        PHOTOZERR += PHOTOZERR_1
        SPECZ += SPECZ_1
        SPECZERR += SPECZERR_1
        HOSTSEP += HOSTSEP_1
        HOST_GMAG += HOST_GMAG_1
        HOST_RMAG += HOST_RMAG_1
        HOST_IMAG += HOST_IMAG_1
        HOST_ZMAG += HOST_ZMAG_1
        MJD += MJD_1
        BAND += BAND_1
        FIELD += FIELD_1
        FLUXCAL += FLUXCAL_1
        FLUXCALERR += FLUXCALERR_1
        PHOTFLAG += PHOTFLAG_1
        PHOTPROB += PHOTPROB_1
        ZPFLUX += ZPFLUX_1
        PSF += PSF_1
        SKYSIG += SKYSIG_1
        SKYSIG_T += SKYSIG_T_1
        GAIN += GAIN_1
        XPIX += XPIX_1
        YPIX += YPIX_1
        NITE += NITE_1
        EXPNUM += EXPNUM_1
        CCDNUM += CCDNUM_1
        OBJID += OBJID_1
    print("len objid full", len(OBJID))

    mytime = datetime.datetime.now().strftime('%Y%m%d_%H:%M:%S')
    print("END new d in dats TIME", mytime)

    print('allgood = %d' % allgood)
    print()
    print(len(EXPNUM))
    HEX = []

    for h in EXPNUM:
        if len(masdf['hex'].loc[masdf['expnum']==h].values)>0:
            HEX.append(masdf['hex'].loc[masdf['expnum']==h].values[0])
        else:
            print('No hex in table for %d. Investigate.' % h)
    MJD,FIELD,FLUXCAL,FLUXCALERR,PHOTFLAG,PHOTPROB,ZPFLUX,PSF,SKYSIG,SKYSIG_T,\
        GAIN,XPIX,YPIX,NITE,EXPNUM,CCDNUM,OBJID,RA,DEC,CAND_ID,SN_ID = \
        np.asarray(MJD),np.asarray(FIELD),np.asarray(FLUXCAL),np.asarray(FLUXCALERR),\
        np.asarray(PHOTFLAG),np.asarray(PHOTPROB),np.asarray(ZPFLUX),np.asarray(PSF),\
        np.asarray(SKYSIG),np.asarray(SKYSIG_T),np.asarray(GAIN),np.asarray(XPIX),\
        np.asarray(YPIX),np.asarray(NITE),np.asarray(EXPNUM),np.asarray(CCDNUM),\
        np.asarray(OBJID),np.asarray(RA),np.asarray(DEC),np.asarray(CAND_ID),np.asarray(SN_ID)

    HOSTID,PHOTOZ,PHOTOZERR,SPECZ,SPECZERR,HOSTSEP,HOST_GMAG,HOST_RMAG,HOST_IMAG,\
        HOST_ZMAG = np.asarray(HOSTID),np.asarray(PHOTOZ),np.asarray(PHOTOZERR),\
        np.asarray(SPECZ),np.asarray(SPECZERR),np.asarray(HOSTSEP),np.asarray(HOST_GMAG),\
        np.asarray(HOST_RMAG),np.asarray(HOST_IMAG),np.asarray(HOST_ZMAG)

    print("check full array: len(CAND_ID)", len(CAND_ID))
    tbhdu1 = fits.BinTableHDU.from_columns(
        [fits.Column(name='cand_ID', format='K', array=CAND_ID.astype(float)),
         fits.Column(name='SNID', format='K', array=SN_ID.astype(float)),
         fits.Column(name='OBJID', format='K', array=OBJID.astype(float)),
         fits.Column(name='RA', format='E', array=RA.astype(float)),
         fits.Column(name='DEC', format='E', array=DEC.astype(float)),
         fits.Column(name='MJD', format='E', array=MJD.astype(float)),
         fits.Column(name='BAND', format='1A', array=BAND),
         fits.Column(name='EXPNUM', format='K', array=EXPNUM.astype(float)),
         fits.Column(name='CCDNUM', format='K', array=CCDNUM.astype(float)),
         fits.Column(name='NITE', format='K', array=NITE.astype(float)),
         fits.Column(name='HEX', format='8A', array=HEX),
         #fits.Column(name='FIELD', format='K', array=RA.astype(float)),
         fits.Column(name='FLUXCAL', format='E', array=FLUXCAL.astype(float)),
         fits.Column(name='FLUXCALERR', format='E', array=FLUXCALERR.astype(float)),
         fits.Column(name='PHOTFLAG', format='K', array=PHOTFLAG.astype(float)),
         fits.Column(name='PHOTPROB', format='E', array=PHOTPROB.astype(float)),
         fits.Column(name='ZPFLUX', format='E', array=ZPFLUX.astype(float)),
         fits.Column(name='PSF', format='E', array=PSF.astype(float)),
         fits.Column(name='SKYSIG', format='E', array=SKYSIG.astype(float)),
         fits.Column(name='SKYSIG_T', format='E', array=SKYSIG_T.astype(float)),
         fits.Column(name='GAIN', format='E', array=GAIN.astype(float)),
         fits.Column(name='XPIX', format='E', array=XPIX.astype(float)),
         fits.Column(name='YPIX', format='E', array=YPIX.astype(float)),
    
         fits.Column(name='HOSTID', format='K', array=HOSTID.astype(float)),
         fits.Column(name='PHOTOZ', format='E', array=PHOTOZ.astype(float)),
         fits.Column(name='PHOTOZERR', format='E', array=PHOTOZERR.astype(float)),
         fits.Column(name='SPECZ', format='E', array=SPECZ.astype(float)),
         fits.Column(name='SPECZERR', format='E', array=SPECZERR.astype(float)),
         fits.Column(name='HOSTSEP', format='E', unit='arcsec', array=HOSTSEP.astype(float)),
         fits.Column(name='HOST_GMAG', format='E', array=HOST_GMAG.astype(float)),
         fits.Column(name='HOST_RMAG', format='E', array=HOST_RMAG.astype(float)),
         fits.Column(name='HOST_IMAG', format='E', array=HOST_IMAG.astype(float)),
         fits.Column(name='HOST_ZMAG', format='E', array=HOST_ZMAG.astype(float)),
         fits.Column(name='DATAFILE', format='21A', array=DATAFILE)])

    tbhdu1.writeto(fitsname,clobber=True)

    print("number of candidates where all detections had ml_score>0.5 :",allgood)
    print("")

    status=True
    
    gc.collect()
    return fitsname,status,masterTableInfo

def makeplots(ccddf,master,truthplus,fitsname,expnums,mjdtrigger,ml_score_cut=0.,skip=False):
    
    season = os.environ.get('SEASON')
    season = str(season)

    rootdir = os.environ.get('ROOTDIR')
    rootdir = os.path.join(rootdir,'exp')

### get data
    if os.path.isfile(master):
        #mlist = Table.read(master)
        #masdf = mlist.to_pandas()
        mlist = fitsio.read(master)
        mlist = mlist.byteswap().newbyteorder()
        masdf = pd.DataFrame.from_records(mlist)
    else:
        skip = True
        print("No master list found with filename",master+'.')
        print("Plots requiring a master list (SNR, RA/DEC hex maps) will not be created.")

    df1 = truthplus

    outdir = os.path.join(os.environ.get('ROOTDIR2'),'plots')
    if not os.path.isdir(outdir):
        os.mkdir(outdir)

    #rtable = Table.read(fitsname)
    #rdf1 = rtable.to_pandas()
    rtable = fitsio.read(fitsname)
    rtable = rtable.byteswap().newbyteorder()
    rdf1 = pd.DataFrame.from_records(rtable)
    
### cut out rejects ###
    df = df1.loc[df1['REJECT'] == 0]
    rdf = rdf1.loc[rdf1['PHOTFLAG'].isin([4096,12288])]

### TEMPLATE FAILURES (temporary section?) ###
    ccdhexes = masdf['fullhex'].loc[masdf['epoch']==4].values
    ccdexp = masdf.loc[(masdf['epoch']==4) & (masdf['fullhex'].isin(ccdhexes))]

    tempfails,rafail,decfail = [],[],[]

    explist = list(ccdexp['expnum'].values)

    for t in sorted(explist):
        if (ccddf.loc[str(t)]==2).any():
            tempfails.append(t)
            rafail.append(masdf['RA'].loc[masdf['expnum']==t].values[0])
            decfail.append(masdf['DEC'].loc[masdf['expnum']==t].values[0])
    
    np.savetxt('event3_ccds.txt',np.c_[tempfails,rafail,decfail],fmt=['%d','%.6f','%.6f'],delimiter='\t',header='EXP\tRA\t\tDEC',comments='')

    notemp = masdf.loc[masdf['expnum'].isin(tempfails)]
    yestemp = masdf.loc[(masdf['expnum'].isin(explist)) & (~masdf['expnum'].isin(tempfails))]

    notemp.ix[notemp['RA'] > 180, 'RA'] = notemp.ix[notemp['RA'] > 180, 'RA'] - 360.
    yestemp.ix[yestemp['RA'] > 180, 'RA'] = yestemp.ix[yestemp['RA'] > 180, 'RA'] - 360.

    xmax = max([max(notemp['RA']),max(yestemp['RA'])])+2
    ymax = max([max(notemp['DEC']),max(yestemp['DEC'])])+2
    xmin = min([min(notemp['RA']),min(yestemp['RA'])])-2
    ymin = max([min(notemp['DEC']),min(yestemp['DEC'])])-2

    plt.xlim(xmin,xmax)
    plt.ylim(ymin,ymax)

    plt.scatter(notemp['RA'],notemp['DEC'],marker='H',c='r',s=200,label='failed')
    plt.scatter(yestemp['RA'],yestemp['DEC'],marker='H',c='b',s=200,label='succeeded')
    plt.title('Template failures - GW170104')
    plt.xlabel('RA')
    plt.ylabel('DEC')
    plt.legend()
    #plt.show()
    plt.savefig('templatefailures.png',dpi=200)
    
    plt.clf()
    
### EFFICIENCY ###

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
    plt.clf()

### ML_SCORE HISTOGRAM - FAKES ###

    plt.hist(df1['ML_SCORE'],bins=np.linspace(0.3,1,100))
    plt.title('ML_SCORE OF FAKES')
    plt.xlabel('ml_score')
    plt.ylabel('# of fakes')
    plt.savefig(os.path.join(outdir,'fakemltest_'+season+'.png'))
    plt.clf()

### PULL --> (MAG-TRUEMAG)/MAG_ERR -- FOR FAKES ### 

    magdiff = df['MAG']-df['TRUEMAG']
    pull = magdiff/df['MAGERR']
    
    bins = np.linspace(-3,3,100)
    print(bins)
    print(plt.hist(pull,bins=bins))
    plt.xlabel("Magnitude Pull (MAG-TRUEMAG)/MAGERR")
    plt.ylabel("Number of Objects")
    plt.title("Magnitude Pull for Fakes")
    plt.xlim(bins.min(),bins.max())

    plt.savefig(os.path.join(outdir,'pulltest_'+season+'.png'))
    plt.clf()

### NUMBER OF REAL CANDIDATES PER CCD ###

    bins = np.arange(1,64,1)
    
    for e in expnums:
        ccdcand = rdf['CCDNUM'].loc[rdf['EXPNUM'] == e]
        ccdhist, bin_edges = np.histogram(ccdcand, bins=bins)
        
        plt.xlim(0,63)
        plt.ylim(0,max(ccdhist)+1)
        plt.scatter(bins[:-1], ccdhist)
        plt.plot(bins[:-1], ccdhist, label=str(e))
        plt.grid(True)
    
    plt.xlabel('CCD')
    plt.ylabel('# of candidates')
    plt.title('# of Candidates per CCD')
    plt.legend(fontsize='small')
    plt.savefig(os.path.join(outdir,'ccdtest'+season+'.png'))
    plt.clf()

### SNR VS. HEX -- FAKES ###
    if not skip:
        masdf_ord = masdf
        masdf_ord.ix[masdf_ord['RA'] > 180, 'RA'] = masdf_ord.ix[masdf_ord['RA'] > 180, 'RA'] - 360.
        masdf_ord = masdf_ord.sort_values(by='RA')
        SNR = OD()
        maxepoch = max(masdf_ord['epoch'])
        for hx in masdf_ord['hex'].unique():
            SNR[hx]=np.array([-5.]*maxepoch)
            epexpdf = masdf_ord[['hex','epoch','expnum']].loc[masdf_ord['hex']==hx]
            for ep in epexpdf['epoch'].unique():
                snrexp = epexpdf['expnum'].loc[epexpdf['epoch']==ep].values[0]
                snrs = df['SNR'].loc[df['EXPNUM']==snrexp].values
                if len(snrs)==0:
                    SNR[hx][ep-1] = 0
                else:
                    meansnr = np.mean(snrs)
                    SNR[hx][ep-1] = meansnr

        SNRdf = pd.DataFrame.from_dict(SNR,orient='index')
        SNRdf = SNRdf.reset_index()
        cols = ['hex']

        epochs = np.array(range(maxepoch))+1
        for i in epochs:
            cols.append(str(i))
        SNRdf.columns = cols

        inds = np.array(SNRdf.index.values)+1

        plt.figure(figsize=(16,9))
        
        ax = plt.axes()
        ax.yaxis.grid(True)
        ax.xaxis.grid(True)

        major = MultipleLocator(5)
        majForm = FormatStrFormatter('%d')
        minor = MultipleLocator(1)
        ax.xaxis.set_major_locator(major)
        ax.xaxis.set_major_formatter(majForm)
        ax.xaxis.set_minor_locator(minor)

        for e in epochs:
            plt.plot(inds, SNRdf[str(e)], '-o', markeredgewidth=0, label='epoch '+str(e), antialiased=True)
        
        plt.legend(loc='upper left',fontsize='small')
        plt.axis([0, max(inds)+1, -10, max(SNRdf.max(numeric_only=True))+5])
        plt.title("Average SNR of MAG=20 fakes by hex index and epoch - GW170104")

        plt.xlabel('Hex Index')
        plt.ylabel('Mean SNR')

        plt.tight_layout()
        savefile = 'SNR_test.png'
        plt.savefig(os.path.join(outdir,savefile),dpi=400)

        plt.clf()

### RA/DEC MAPS ###
    
    radecdf = rdf
    if abs(max(radecdf['RA'])-min(radecdf['RA']))>180:
        for ira in range(len(radecdf['RA'])):
            if radecdf['RA'][ira]>180:
                radecdf['RA'][ira] = radecdf['RA'][ira]-360

    radecdf = radecdf.drop_duplicates('cand_ID')

    radecdf = radecdf.loc[radecdf['PHOTPROB'] > ml_score_cut]

    #plt.hist2d(radecdf['RA'],radecdf['DEC'],50)
    
    mapdir = os.path.join(outdir,'maps')
    if not os.path.isdir(mapdir):
        os.mkdir(mapdir)    

    hexex = []

    ### this loop gets the full set of first epoch exposures of each hex.
    ### if there are two (or more), it chooses the one with the best t_eff.
    for h in masdf['fullhex'].unique():
        exepteff = masdf[['expnum','epoch','t_eff']].loc[masdf['fullhex'] == h]
        cut = exepteff[['expnum','epoch','t_eff']].loc[exepteff['epoch']==1]
        if len(cut)>1:
            cut = cut.loc[cut['t_eff'] == cut['t_eff'].ix[cut['t_eff'].idxmax()]]
        hexex.append(cut['expnum'].values[0])

    radecdf = radecdf.loc[radecdf['EXPNUM'].isin(hexex)]

    ### overall map
    plt.figure(figsize=(16,9))
    plt.scatter(radecdf['RA'],radecdf['DEC'],c=radecdf['PHOTPROB'],edgecolor='',s=25)
    plt.xlim(min(radecdf['RA'])-0.2,max(radecdf['RA'])+0.2)
    plt.ylim(min(radecdf['DEC'])-0.2,max(radecdf['DEC'])+0.2)
    plt.clim(0,1)
    plt.colorbar().set_label('ml_score')
    plt.title('Candidate Sky Map')
    plt.xlabel('RA')
    plt.ylabel('DEC')
    plt.tight_layout()
    plt.savefig(os.path.join(outdir,'fullmap_'+season+'.png'))
    plt.clf()
    sys.exit()

    ### individual hex maps
    if not skip:
        for e in hexex:
            print(e)
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

            decs = np.concatenate((odf['dec1'].tolist(),odf['dec2'].tolist(),odf['dec3'].tolist(),odf['dec4'].tolist()),axis=0)

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
            plt.savefig(os.path.join(mapdir,'map_'+str(hexname)+'_'+str(e)+'.png'),dpi=200)
            plt.clf()

    ### LIGHTCURVES ###

    lcdir = os.path.join(outdir,'lightcurves')

    # generalize this later!
    band = 'i'

    numsnid = len(rdf['SNID'].unique())
    ctsnid = 0
    noct,yesct = 0,0

    rdf['cutflag'] = np.zeros(len(rdf))

    maxmjd = rdf['MJD'].max()

    for sn in rdf['SNID'].unique():
        #ctsnid += 1
        
        new = rdf[['MJD','FLUXCAL','FLUXCALERR','PHOTPROB']].loc[rdf['SNID']==sn]
        
        if all(i < ml_score_cut for i in new['PHOTPROB']):
            noct+=1
            continue
        
        yesct+=1
        
        if (noct+yesct) % 100 == 0:
            print(str(noct+yesct)+'/'+str(numsnid))

        rdf.loc[rdf['SNID']==sn,'cutflag'] = 1

        mjd = np.array(new['MJD'].tolist())
        flux = np.array(new['FLUXCAL'].tolist())
        fluxerr = np.array(new['FLUXCALERR'].tolist())
        ml_score = np.array(new['PHOTPROB'].tolist())

        plt.errorbar(mjd-mjdtrigger,flux,yerr=fluxerr,fmt='none',ecolor='k',zorder=0)
        plt.scatter(mjd-mjdtrigger,flux,c=ml_score,edgecolor='',s=40,zorder=1)
        plt.clim(0,1)
        plt.title('SNID '+str(sn)+' ('+band+')')
        plt.colorbar().set_label('ml_score')
        plt.xlim(0,int(maxmjd-mjdtrigger)+1)
        plt.xlabel('MJD - MJD(TRIGGER)')
        plt.ylabel('FLUX')
        plt.tight_layout()
        plt.savefig(os.path.join(lcdir,'SNID'+str(sn)+'.png'))
        plt.clf()
    return rdf,lcdir

def createhtml(fitsname,realdf,master,lcdir): 
    rootdir = os.environ.get('ROOTDIR')
    expdir = os.path.join(rootdir,'exp')
    season = os.environ.get('SEASON')
    skip = False

    if os.path.isfile(master):
        #mlist = Table.read(master)
        #masdf = mlist.to_pandas()
        mlist = fitsio.read(master)
        mlist = mlist.byteswap().newbyteorder()
        masdf = pd.DataFrame.from_records(mlist)
    else:
        skip = True
        print("No master list found with filename",master+'.')
        print("This step will run more slowly because it will require the use of glob.")
    
    spl = fitsname.split('.fits')
    newfits = spl[0]+'stamps.fits'

    if not os.path.isfile(newfits):

        rdf = realdf.reset_index(drop=True)

        ### GET STAMPS ###
        lenr = len(rdf)
        srcharray = ['' for x in range(lenr)]
        temparray = ['' for x in range(lenr)]
        diffarray = ['' for x in range(lenr)]
        aaa = 0
        aaalen = len(rdf['EXPNUM'].unique())
        #time1 = time.time()
        for e in sorted(rdf['EXPNUM'].unique()):
            aaa += 1
            bb = 0
            edf = rdf[['EXPNUM','NITE','CCDNUM','BAND','OBJID','HEX']].loc[rdf['EXPNUM'] == e]
            bblen = len(edf['CCDNUM'].unique())
            #time2 = time.time()
            for c in sorted(edf['CCDNUM'].unique()):
                bb += 1
                cdf = edf.loc[edf['CCDNUM'] == c]
                nite = str(cdf['NITE'].values[0])
                hhex = str(cdf['HEX'].values[0])
                exp = str(e)
                dp = 'dp'+str(season)
                band = str(cdf['BAND'].values[0])
                ccd = '%02d' % c
                stampname = 'stamps_'+nite+'_'+hhex+'_'+band+'_'+ccd
                stampstar = os.path.join(expdir,nite+'/'+exp+'/'+dp+'/'+band+'_'+ccd+'/'+stampname)
                #time3 = time.time()
                #gstamp = glob(stampstar)
                #time4 = time.time()
                #if len(gstamp)==1:
                if os.path.isdir(stampstar):
                    stampdir = stampstar
                    for i in list(cdf.index.values):
                        #time5 = time.time()
                        obj = str(int(cdf.ix[i,'OBJID']))
                        srch = os.path.join(stampdir,'srch'+obj+'.gif')
                        temp = os.path.join(stampdir,'temp'+obj+'.gif')
                        diff = os.path.join(stampdir,'diff'+obj+'.gif')
                        #time6 = time.time()
                    
                        srcharray[i] = srch
                    
                        temparray[i] = temp
                    
                        diffarray[i] = diff
    
        rdf['srchstamp'] = srcharray
        rdf['tempstamp'] = temparray
        rdf['diffstamp'] = diffarray

        rdf['cutflag'] = rdf['cutflag'].astype(int)

        newfile = fitsio.FITS(newfits,'rw')
        newfile.write(rdf.to_records(index=False),clobber=True)
        newfile.close()

        sdf = rdf.copy()

    else:    
        print('A combined .fits file for all real candidates containing stamp information already exists:')
        print("")
        print(newfits)
        print("")
        print('If you want to recreate the file, simply delete or rename the existing one.')

        #stable = Table.read(newfits)
        #sdf = stable.to_pandas()
        stable = fitsio.read(newfits)
        stable = stable.byteswap().newbyteorder()
        sdf = pd.DataFrame.from_records(stable)

    ### HTML CREATION ###
    
    sdf = sdf.loc[sdf['cutflag']==1]

    for c in sdf['SNID']:
        cdf = sdf.loc[sdf['SNID']==c]
        

