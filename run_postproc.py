import os
from glob import glob
import tarfile
import argparse
import ConfigParser
import sys
import updateStatus
import postproc
import makePlots
import numpy as np
import pandas as pd
import findHostGala
import WholeHTML
import datetime
import time


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
parser.add_argument('--schema', type=str, default='gw', help='Schema used')
args = parser.parse_args()

season=str(args.season)

## Read config file                                                            
config = ConfigParser.ConfigParser()
if os.path.isfile('./postproc_'+str(season)+'.ini'):
    inifile = config.read('./postproc_'+str(season)+'.ini')[0]

## Set ups mode: True/False
if args.ups == None:
    ups = config.getboolean('general','ups')
else:
    ups = args.ups

## If running in ups environment, replace the .ini file
if ups:
    cpath = os.environ["GWPOST_DIR"]
    inifile = config.read(os.path.join(cpath,"postproc_"+str(season)+".ini"))[0]

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
#This line should not be necessary
if args.season == None:
    season = config.get('general','season')
else:
    season = str(args.season)

########                                                                                   
# Set Season and Time                                                                      
####### 

thisTime = time.strftime("%Y%m%d.%H%M")
thisTime=thisTime.replace('.','_')
seasonTime=open('seasonTime'+str(season)+'.txt','w+')
seasonTime.write(season)
seasonTime.write('\n')
seasonTime.write(thisTime)
seasonTime.close()
print('seasonTime'+season+'.txt was made.')


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

## Make directory structure

if not os.path.isdir(outdir):
    os.makedirs(outdir) 

if not os.path.isdir(outdir + '/' + 'stamps'):
    os.mkdir(outdir + '/' + 'stamps')

if not os.path.isdir(outdir + '/' + 'plots'):
    os.mkdir(outdir + '/' + 'plots')

if not os.path.isdir(outdir + '/plots/' + 'lightcurves'):
    os.mkdir(outdir + '/plots/' + 'lightcurves')

outplots = outdir + '/' + 'plots'
outstamps = outdir + '/' + 'stamps'

########
# Initialize Status
########
statusList=[False,False,False,False,False,False,False,False,False,'incomplete']
update=updateStatus.updateStatus(statusList,season)
print(update)
#sys.exit('debugggin')

#########
# STEP -1: Set up the environment
#########

print "Run STEP -1: Set up the environment"
status=postproc.prep_environ(rootdir,indir,outdir,season,setupfile,version_hostmatch,db,schema)
print
print(status,'status-1')
if status !=None:
    statusList[0]=status
else:
    statusList[0]=False
print('step-1 status:',status)
update=updateStatus.updateStatus(statusList,season)
print(update)
print('statusList',statusList)

#########
# STEP 0: Create initial master list, check processing outputs
#########

print "Run STEP 0: Create initial master list, check processing outputs"
if len(expnums)>0:
    expniteband_df,master,sta = postproc.masterlist(masterfile_1,blacklist_file,ligoid,propid,bands,expnums)

else:
    print "No exposures specified by user. All exposures taken under LIGO ID "+str(ligoid)+" / event ID "+str(triggerid)+" and prop ID "+str(propid)+" will be used for the initial master list and the checkoutputs step."
    expniteband_df,master,sta = postproc.masterlist(masterfile_1,blacklist_file,ligoid,propid,bands)
print

if sta == None:
    sta=False

### the current checkoutputs assumes .FAIL files are cleared out when a CCD is reprocessed
if len(expniteband_df)>0:
    print("expniteband_df >0 true")
    expnums,a_blacklist,ccddf,tus = postproc.checkoutputs(expniteband_df,logfile,ccdfile,goodchecked,steplist)

else:
    tus=False
    print "ERROR: No exposures provided, and no exposures found matching the trigger id and prop id provided in the .ini file:"
    print
    print "TRIGGER ID: "+str(triggerid)
    print "PROP ID: "+str(propid)
    print
    print "EXITING."
    print
    sys.exit()
print
print('step 0 status', sta+tus)
if sta+tus==0 or sta+tus == 1:
    statusList[1]=False
else:
    statusList[1]=True

if checkonly:
    print( 'You gave the --checkonly option. Stopping Now.')
    sys.exit(0)

update=updateStatus.updateStatus(statusList,season)
print(update)
print('statusList',statusList)

#########
# STEP 1: Create final master list
#########

print "Run STEP 1: Create final master list"

expniteband_df,master,status = postproc.masterlist(masterfile_2,blacklist_file,ligoid,propid,bands,expnums,a_blacklist)
print

if status !=None:
    statusList[2]=status
else:
    statusList[2]=False
print('step 1 staus:', status)
update=updateStatus.updateStatus(statusList,season)
print(update)
print('statusList',statusList)
expnums = expniteband_df['expnum'].tolist()

#########
# STEP 2: Forcephoto
#########

print "Run STEP 2: Forcephoto"
postproc.forcephoto(season,ncore,numepochs_min_1,writeDB)
print
####Status update at a different time

#########
# STEP 3: Hostmatch
#########

#print "Run STEP 3: Hostmatch"
#import desHostMatch
#print('We are *RUNNING HOSTMATCH!*')
#desHostMatch.main(season)
#print
#print('That... was not worth the hype.')
#print
#print('Now making a galaxy match dictionary!')
#irksome='/data/des40.b/data/nsherman/postprocBig/outputs/hostmatch/databaseLocation.txt'
#irritation=open(irksome,'r')
#path=irritation.read()
#irritation.close()
#print(path)
##sys.exit('Ladies and gentlemen! We are debugging.')
#snidDict=findHostGala.findHostGala(path)
#print(list(snidDict.keys()))
#
#prestat=open('hostmatchstatus.txt','r')
#stat=prestat.read()
#if stat==True:
#    status=True
#else:
#    status=False
#statusList[4]=status
#
snidDict={}
status=False
print('step 3 status', status)

update=updateStatus.updateStatus(statusList,season)
print(update)
print('statusList',statusList)

#########
# STEP 4: Make truth table
#########

if len(expnums)>0:
    print "Run STEP 4: Make truth table"
    truthplus,status = postproc.truthtable(season,expnums,filename,truthplusfile)

else:
    status=False
    print "WARNING: List of exposures is empty. Skipping STEP 4."
print

if status == None:
    status=False
print('stet 4 status',status)
statusList[5]=status
update=updateStatus.updateStatus(statusList,season)
print(update)
print('statusList',statusList)

#########
# STEP 5: Make datafiles
#########

print "Run STEP 5: Make datafiles"
postproc.makedatafiles(season,format,numepochs_min_2,two_nite_trigger,outFile_stdoutreal,outDir_datareal,ncore)

if not fakeversion=='KBOMAG20ALLSKY':
    postproc.makedatafiles(season,format,numepochs_min_2,two_nite_trigger,outFile_stdoutfake,outDir_datafake,ncore,fakeversion)
    one=1
else:
    print "No datafiles made for fakes because fakeversion=KBOMAG20ALLSKY."
print                                                                                    
print "Run STEP 5b: Combine real datafiles"
fitsname,status,masterTableInfo = postproc.combinedatafiles(season,master,combined_fits,outDir_datareal,snidDict, args.schema)


print
#if masterTableInfo != None:
#    print(type(list(masterTableInfo.keys())[0]))
#
if status == None:
    status=False
print('step 5 status',status)
statusList[6]=status
update=updateStatus.updateStatus(statusList,season)
print(update)
print('statusList',statusList)

if status == False:
    runStatus='complete'
    statusList[9]=runStatus
    update=updateStatus.updateStatus(statusList,season)
    print(update)
    sys.exit()

#########
# STEP 6: Make plots
#########


skip=True
print "Run STEP 6: Make plots"
print
stat6,MLScoreFake,RADEC=makePlots.MakeDaPlots(season,ccddf,master,truthplus,fitsname,expnums,triggermjd,mlscore_cut,skip)
#print(Words)
print('It is possible this has run.')
#statusList.append(status)
    #print("Sorry, son. Step 6 doesn't quite work right here, so I'll just give you the associated .fits and .gifs for these exposures.They will be in your outdir directory and hard to miss.")

if stat6==None:
    stat6=False
print('step 6 status', stat6)
statusList[7]=stat6
update=updateStatus.updateStatus(statusList,season)
print(update)
print('statusList',statusList)

#########
# STEP 7: Make htmls/webpage
#########
status=False
#sys.exit()
print "Run STEP 7: Make htmls/webpage"
print "This is not Awesomely implemented. More coming soon..."
#postproc.createhtml(fitsname,realdf,master,lcdir)
print
print("HAHAH! Tricked you! The htmls were actually created in Step 5! Bwahaha!")
print('Also, here is a (possibly) working master HTML, from which you can access evvvverythiiiing.')
word=WholeHTML.WholeHTML(MLScoreFake,RADEC,season,masterTableInfo)
print(word)
if word == "Functional":
    statusNew=True
    statusList[8]=statusNew
else:
    statusList[8]=status
print('step 7 status', status)

update=updateStatus.updateStatus(statusList,season)
print(update)
print('statusList',statusList)


runStatus='complete'
statusList[9]=runStatus
update=updateStatus.updateStatus(statusList,season)
print(update)
