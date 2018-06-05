import os
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
dat=os.listdir('output_GW170814_dp408_20180326/makedatafiles')
print dat
dats=os.listdir('output_GW170814_dp408_20180326/makedatafiles')
 dats = [x for x in dats if '.dat' in x]
dats = [x for x in dats if '.dat' in x]
print dats
dats=os.listdir('output_GW170814_dp408_20180326/makedatafiles/LightCurvesReal')
dats = [x for x in dats if '.dat' in x]
print dats
print len(dats)
hostlist = []
    c = 0
    MJD,BAND,FIELD,FLUXCAL,FLUXCALERR,PHOTFLAG,PHOTPROB,ZPFLUX,PSF,SKYSIG,\
        SKYSIG_T,GAIN,XPIX,YPIX,NITE,EXPNUM,CCDNUM,OBJID = [],[],[],[],[],[],\
        [],[],[],[],[],[],[],[],[],[],[],[]
    RA,DEC,CAND_ID,DATAFILE,SN_ID = [],[],[],[],[]
    HOSTID,PHOTOZ,PHOTOZERR,SPECZ,SPECZERR,HOSTSEP,HOST_GMAG,HOST_RMAG,HOST_IMAG,\
        HOST_ZMAG = [],[],[],[],[],[],[],[],[],[]
    c=0
import postproc
import argparse
import ConfigParser
config = ConfigParser.ConfigParser()
if os.path.isfile('./postproc.ini'):
    inifile = config.read('./postproc.ini')[0]
## Read command line options
parser = argparse.ArgumentParser(description=__doc__, 
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
parser.add_argument('--expnums', metavar='e',type=int, nargs='+', help='List of Exposures')
parser.add_argument('--outputdir', metavar='d', type=str, help='Location of output files')
parser.add_argument('--season', help='Season number', type=int)
parser.add_argument('--triggerid', help='LIGO trigger ID (GW######)', type=str)
parser.add_argument('--ligoid', help='LIGO event ID (G######)', type=str)
parser.add_argument('--mjdtrigger', type=float, help='MJD of LIGO trigger')
parser.add_argument('--ups', type=bool, help='ups mode: True/False')
parser.add_argument('--checkonly', type=bool, help='only do the processing check')
args = parser.parse_args()
## Set ups mode: True/False
if args.ups == None:
    ups = config.getboolean('general','ups')
else:
    ups = args.ups
## If running in ups environment, replace the .ini file
if ups:
    cpath = os.environ["GWPOST_DIR"]
    inifile = config.read(os.path.join(cpath,"postproc.ini"))[0]
## Option to only do the check
if args.checkonly == None:
    checkonly = False
else:
    checkonly = True
## Set outdir
if args.outputdir == None:
    outdir = config.get('general','outdir')
else:
    outdir = args.outdir
## Set season
if args.season == None:
    season = config.get('general','season')
else:
    season = str(args.season)
## Set ligoid                                                                       
if args.ligoid == None:
    ligoid = config.get('general','ligoid')
else:
    ligoid = args.ligoid
## Set triggerid
if args.triggerid == None:
    triggerid = config.get('general','triggerid')
else:
    triggerid = args.triggerid
## Set triggermjd
if args.mjdtrigger == None:
    triggermjd = config.getfloat('general','triggermjd')
else:
    triggermjd = float(args.mjdtrigger)
## Get the list of exposures
if args.expnums == None:
    expnums = []
    indir = config.get('general','indir')
    expnums_listfile = config.get('general','exposures_listfile')
    expnums_listfile = os.path.join(indir,expnums_listfile)
    try:
        explist = open(expnums_listfile,'r')
        expnums1 = explist.readlines()
        for line in expnums1:
            expnums.append(line.split('\n')[0])
            expnums = map(int,expnums)
    except:
        print "WARNING: List of exposures file not found or empty."
        expnums = []
        #sys.exit(1)
else:
    expnums = args.expnums
## Get remaining configuration info from the .ini file 
rootdir = config.get('general','rootdir')
indir = config.get('general','indir')
expdir = os.path.join(rootdir,'exp')
forcedir = os.path.join(rootdir,'forcephoto')+'/images/dp'+season+'/*'
 
bands = config.get('general','bands')
if bands=='all':
    bands = None
else:
    bands = bands.split(',')
setupfile = config.get('general','env_setup_file')
triggerid = config.get('general','triggerid')
propid = config.get('general','propid')
mlscore_cut = config.getfloat('plots','mlscore_cut')
blacklist_file = config.get('masterlist', 'blacklist')
masterfile_1 = config.get('masterlist', 'filename_1')
masterfile_2 = config.get('masterlist', 'filename_2')
logfile = config.get('checkoutputs', 'logfile')
ccdfile = config.get('checkoutputs', 'ccdfile')
goodchecked = config.get('checkoutputs', 'goodfile')
steplist = config.get('checkoutputs', 'steplist')
ncore = config.getint('GWFORCE', 'ncore')
numepochs_min_1 = str(config.getint('GWFORCE', 'numepochs_min'))
numepochs_min_2 = str(config.getint('GWmakeDataFiles', 'numepochs_min'))
#numepochs_min = str(min(numepochs_min_1,numepochs_min_2))
writeDB = config.getboolean('GWFORCE', 'writeDB')
version_hostmatch = config.get('HOSTMATCH', 'version')
db = config.get('general', 'db')
schema = config.get('general', 'schema')
filename = config.get('truthtable', 'filename')
truthplusfile = config.get('truthtable', 'plusname')
format = config.get('GWmakeDataFiles', 'format')
two_nite_trigger = config.get('GWmakeDataFiles', '2nite_trigger')
outFile_stdoutreal = config.get('GWmakeDataFiles-real', 'outFile_stdout')
outDir_datareal = config.get('GWmakeDataFiles-real', 'outDir_data')
combined_fits = config.get('GWmakeDataFiles-real', 'combined_fits')
outFile_stdoutfake = config.get('GWmakeDataFiles-fake', 'outFile_stdout')
outDir_datafake = config.get('GWmakeDataFiles-fake', 'outDir_data')
fakeversion = config.get('GWmakeDataFiles-fake', 'version')

postproc.prep_environ(rootdir,indir,outdir,season,setupfile,version_hostmatch,db,schema)

fitsname = postproc.combinedatafiles(master,combined_fits,outDir_datareal)
