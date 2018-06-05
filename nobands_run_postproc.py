import os
import argparse
import ConfigParser
import sys
import postproc
import numpy as np
import pandas as pd

## Read config file
config = ConfigParser.ConfigParser()
if os.path.isfile('./postproc.ini'):
    inifile = config.read('./postproc.ini')[0]

## Read command line options
parser = argparse.ArgumentParser(description=__doc__, 
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
parser.add_argument('--expnums', metavar='e',type=int, nargs='+', help='List of Exposures')
parser.add_argument('--outputdir', metavar='d', type=str, help='Location of output files')
parser.add_argument('--season', help='Season number', type=int)
parser.add_argument('--triggerid', help='LIGO trigger ID', type=str)
parser.add_argument('--mjdtrigger', type=float, help='MJD of LIGO trigger')
parser.add_argument('--ups', type=bool, help='ups mode: True/False')
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

bands = config.get('general','bands')
bands = bands.split(',')

rootdir = config.get('general','rootdir')
indir = config.get('general','indir')
expdir = os.path.join(rootdir,'exp')
forcedir = os.path.join(rootdir,'forcephoto')+'/images/dp'+season+'/*'

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

ncore = config.get('GWFORCE', 'ncore')

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


## Make directory structure

if not os.path.isdir(outdir):
    os.mkdir(outdir) 

if not os.path.isdir(outdir + '/' + 'stamps'):
    os.mkdir(outdir + '/' + 'stamps')

if not os.path.isdir(outdir + '/' + 'plots'):
    os.mkdir(outdir + '/' + 'plots')

if not os.path.isdir(outdir + '/plots/' + 'lightcurves'):
    os.mkdir(outdir + '/plots/' + 'lightcurves')

outplots = outdir + '/' + 'plots'
outstamps = outdir + '/' + 'stamps'

#########
# STEP -1: Set up the environment
#########

print "Run STEP -1: Set up the environment"
postproc.prep_environ(rootdir,indir,outdir,season,setupfile,version_hostmatch,db,schema)
print

#########
# STEP 0: Create initial master list, check processing outputs
#########

print "Run STEP 0: Create initial master list, check processing outputs"
if len(expnums)>0:
    expniteband_df,master = postproc.masterlist(masterfile_1,blacklist_file,triggerid,propid,expnums)
else:
    print "No exposures specified by user. All exposures taken under trigger id "+str(triggerid)+" and prop id "+str(propid)+" will be used for the initial master list and the checkoutputs step."
    expniteband_df,master = postproc.masterlist(masterfile_1,blacklist_file,triggerid,propid)
print

### the current checkoutputs assumes .FAIL files are cleared out when a CCD is reprocessed
if len(expniteband_df)>0:
    expnums,a_blacklist,ccddf = postproc.checkoutputs(expniteband_df,logfile,ccdfile,goodchecked,steplist)
else:
    print "ERROR: No exposures provided, and no exposures found matching the trigger id and prop id provided in the .ini file:"
    print
    print "TRIGGER ID: "+str(triggerid)
    print "PROP ID: "+str(propid)
    print
    print "EXITING."
    print
    sys.exit()
print

#########
# STEP 1: Create final master list
#########

print "Run STEP 1: Create final master list"
expniteband_df,master = postproc.masterlist(masterfile_2,blacklist_file,triggerid,propid,expnums,a_blacklist)
print

expnums = expniteband_df['expnum'].tolist()
sys.exit()

#########
# STEP 2: Forcephoto
#########

print "Run STEP 2: Forcephoto"
postproc.forcephoto(ncore,numepochs_min_1,writeDB)
print

#########
# STEP 3: Hostmatch
#########

print "Run STEP 3: Hostmatch"
#import desHostMatch
#desHostMatch.main()
print

#########
# STEP 4: Make truth table
#########

if len(expnums)>0:
    print "Run STEP 4: Make truth table"
    truthplus = postproc.truthtable(expnums,filename,truthplusfile)
else:
    print "WARNING: List of exposures is empty. Skipping STEP 4."
print

#########
# STEP 5: Make datafiles
#########

print "Run STEP 5: Make datafiles"
#postproc.makedatafiles(format,numepochs_min_2,two_nite_trigger,outFile_stdoutreal,outDir_datareal)

if not fakeversion=='KBOMAG20ALLSKY':
#    postproc.makedatafiles(format,numepochs_min_2,two_nite_trigger,outFile_stdoutfake,outDir_datafake,fakeversion)
    one=1
else:
    print "No datafiles made for fakes because fakeversion=KBOMAG20ALLSKY."
print                                                                                    

print "Run STEP 5b: Combine real datafiles"
fitsname = postproc.combinedatafiles(master,combined_fits,outDir_datareal)
print

#########
# STEP 6: Make plots
#########

skip=True
print "Run STEP 6: Make plots"
print
realdf,lcdir = postproc.makeplots(ccddf,master,truthplus,fitsname,expnums,triggermjd,mlscore_cut,skip)
print

#########
# STEP 7: Make htmls/webpage
#########

sys.exit()
print "Run STEP 7: Make htmls/webpage"
print "This is not yet implemented. Coming soon..."
postproc.createhtml(fitsname,realdf,master,lcdir)
print


