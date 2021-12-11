import os
from glob import glob
import tarfile
import argparse
import configparser
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
import run_checker
#import cProfile

from makePlots import EmptyPlotError

## Read command line options
parser = argparse.ArgumentParser(description=__doc__, 
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
parser.add_argument('--expnums', metavar='e',type=int, nargs='+', help='List of Exposures')
parser.add_argument('--outputdir', metavar='d', type=str, help='Location of output files')
parser.add_argument('--season', help='Season number', type=int)
parser.add_argument('--mjdtrigger', type=float, help='MJD of LIGO trigger')
parser.add_argument('--ups', type=bool, help='ups mode: True/False')
parser.add_argument('--checkonly', type=bool, help='only do the processing check')
parser.add_argument('--schema', type=str, default='gw', help='Schema used')
parser.add_argument('--post', action='store_true', help='Push htmls to website - only use for production runs')
parser.add_argument('--SKIPTO', default=0, type=int, help='If you are rerunning and want to skip to a specific step. Appropriate steps include: STEP 1: Create final master list, STEP 2: Forcephoto, STEP 3: Hostmatch,  STEP 4: Make truth table, STEP 5: Make datafiles, (steps 6 and 7 not implemented yet)')
parser.add_argument('--skip_lightcurves', action='store_true', help='Skip lightcurve generation to speed things up')
args = parser.parse_args()

if args.SKIPTO > 7 or args.SKIPTO < 0:
    raise ValueError('NOT VALID SKIP TO ARGUMENT')
    
season=str(args.season)

mytime = datetime.datetime.now().strftime('%Y%m%d_%H:%M:%S')
print("START TIME", mytime)

## Read config file                                                            
config = configparser.ConfigParser()
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
    outdir = args.outputdir

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
maxnite = time.strftime("%Y%m%d") #ag add for forcephoto maxnite
thisTime=thisTime.replace('.','_')
seasonTime=open('seasonTime'+str(season)+'.txt','w+')
seasonTime.write(season)
seasonTime.write('\n')
seasonTime.write(thisTime)
seasonTime.close()
print('seasonTime'+season+'.txt was made.')



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
            expnums = list(map(int,expnums)) #Made 'list' since 'map' object has no append attribute.
    except:
        print("WARNING: List of exposures file not found or empty.")
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
    #bands = map(str.strip, bands) #If bands is supposed to be an array (e.g. ['i','z']), this is unnecessary

setupfile = config.get('general','env_setup_file')

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

GoodSNIDs = config.get('general','GoodSNIDs')

MLcutoff = config.get('plots','mlscore_cut')

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


#############################
# Make directories to hold  #
# plots and htmls for ease. #
#############################
os.system('mkdir '+str(outdir)+'/pngs')
os.system('mkdir '+str(outdir)+'/htmls')

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

print("Run STEP -1: Set up the environment")
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


if args.SKIPTO > 0:

    expniteband_df =pd.read_csv(outdir+'/masterlist/expniteband_df.csv')
    master = pd.read_csv(outdir+'/masterlist/'+masterfile_1, encoding='latin-1') #Added encoding

    with open(outdir+'/checkoutputs/step0_expnums.txt', 'r') as f:
        expnums = [int(x.strip()) for x in f.readlines()]
    if os.path.exists(outdir+'/checkoutputs/step0_a_blacklist.txt'):
        if os.path.getsize(outdir+'/checkoutputs/step0_a_blacklist.txt') > 0:
            with open(outdir+'/checkoutputs/step0_a_blacklist.txt', 'r') as f:
                a_blacklist = [int(x.strip()) for x in f.readlines()]
        else:
            a_blacklist = []
    else:
        raise OSError("step 0 a_blacklist file missing")
#    except:
#        raise OSError("/masterlist/expniteband_df.csv or /masterlist/+masterfile_1 or checkoutputs/step0_expnums.txt or checkoutputs/step0_a_blacklist.txt does not exist. Try reruning with step 0")
    
else:
    print("Run STEP 0: Create initial master list, check processing outputs")
    if len(expnums)>0:
        expniteband_df,master,sta = postproc.masterlist(masterfile_1,blacklist_file,propid,bands,expnums)
        expniteband_df.to_csv(outdir+'/masterlist/expniteband_df.csv')
    else:
        raise RuntimeError("Need an exposures.list file or exposures in argument")
    
    if len(expniteband_df)>0:
        print("expniteband_df >0 true")
        expnums,a_blacklist,ccddf,tus = postproc.checkoutputs(expniteband_df,logfile,ccdfile,goodchecked,steplist)
        with open(outdir+'/checkoutputs/step0_expnums.txt', 'w+') as f:
            f.writelines([str(x)+'\n' for x in expnums])
        with open(outdir+'/checkoutputs/step0_a_blacklist.txt', 'w+') as f:
            f.writelines([str(x)+'\n' for x in a_blacklist])

    else:
        raise ValueError('expniteband_df is empty. Check log file for "MASTERLIST QUERY" for the failed db query.')

if checkonly:
    print( 'You gave the --checkonly option. Stopping Now.')
    sys.exit(0)

#########
# STEP 1: Create final master list
#########

print("Run STEP 1: Create final master list")

expniteband_df,master,status = postproc.masterlist(masterfile_2,blacklist_file,propid,bands,expnums,a_blacklist)
print("")

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

if args.SKIPTO <= 2:
    print("Run STEP 2: Forcephoto")
    postproc.forcephoto(season,maxnite,ncore,numepochs_min_1,writeDB) #ag added maxnite
    print("")
else:
    print("USING SKIPTO FLAG, SKIPPING TO STEP ", args.SKIPTO)

#########
# STEP 3: Hostmatch
#########

if args.SKIPTO <= 3:
    print("Run STEP 3: Hostmatch")
    import desHostMatch
    desHostMatch.main(season)
else:
    print("USING SKIPTO FLAG, SKIPPING TO STEP ", args.SKIPTO)

prestat=open('hostmatchstatus.txt','r')
stat=prestat.read()
if stat=='0':
    status=True
else:
    status=False
statusList[4]=status

print('step 3 status', status)

update=updateStatus.updateStatus(statusList,season)
print(update)
print('statusList',statusList)

#########
# STEP 4: Make truth table
#########

if args.SKIPTO > 4:
    if os.path.exists(outdir+'/truthtable'+str(season)+'/'+truthplusfile):
        truthplus = pd.read_csv(outdir+'/truthtable'+str(season)+'/'+truthplusfile, delim_whitespace=True)
        status = True
    else:
        raise OSError("No truth table files found. Try running step 4")

elif len(expnums)>0:
    print("Run STEP 4: Make truth table")
    truthplus,status = postproc.truthtable(season,expnums,filename,truthplusfile)

else:
    status=False
    print("WARNING: List of exposures is empty. Skipping STEP 4.")
print("")

if status == None:
    status=False
print('step 4 status',status)
statusList[5]=status
update=updateStatus.updateStatus(statusList,season)
print(update)
print('statusList',statusList)

#########
# STEP 5: Make datafiles
#########

if args.SKIPTO <= 5:
    print("Run STEP 5: Make datafiles")
    postproc.makedatafiles(season,format,numepochs_min_2,two_nite_trigger,outFile_stdoutreal,outDir_datareal,ncore)

    if not fakeversion=='KBOMAG20ALLSKY':
        postproc.makedatafiles(season,format,numepochs_min_2,two_nite_trigger,outFile_stdoutfake,outDir_datafake,ncore,fakeversion)
        one=1
    else:
        print("No datafiles made for fakes because fakeversion=KBOMAG20ALLSKY.")
        print("")                                                                                    
        print("Run STEP 5b: Combine real datafiles")
    mytime = datetime.datetime.now().strftime('%Y%m%d_%H:%M:%S')
    print("START 5B TIME", mytime)

    if os.path.isfile(outdir+'/makedatafiles/'+combined_fits):
        print("REMOVING COMBINED DATA FILE")
        os.system('rm '+outdir+'/makedatafiles/'+combined_fits)

    fitsname,status,masterTableInfo = postproc.combinedatafiles(season,master,combined_fits,outdir, outDir_datareal, args.schema,triggermjd, GoodSNIDs, args.skip_lightcurves, args.post, MLcutoff)
    np.save(outdir+'/mastertableinfo.npy', masterTableInfo)
    
else:
    print("STEP 5")
    print("USING SKIPTO FLAG, SKIPPING TO STEP ", args.SKIPTO)
    print("STEP 5B")
    fitsname = outdir+'/makedatafiles/'+combined_fits
    status = False
    masterTableInfo = np.load(outdir+'/mastertableinfo.npy')

print("")

if status == None:
    status=False
print('step 5 status',status)
statusList[6]=status
update=updateStatus.updateStatus(statusList,season)
print(update)
print('statusList',statusList)


mytime = datetime.datetime.now().strftime('%Y%m%d_%H:%M:%S')
print("END 5B TIME", mytime)

#########
# STEP 6: Make plots
#########


skip=True
print("Run STEP 6: Make plots")
print("")

#make fake ml and ra dec map plots
try:
    _, MLScoreFake, RADEC = makePlots.MakeDaPlots(
        season,master,truthplus,fitsname,expnums,triggermjd,outdir,mlscore_cut,skip)
except EmptyPlotError as err:
    print(err.message)
    print("Step 6 failed due to empty candidate dfs. Ending PostProc.")
    mytime = datetime.datetime.now().strftime('%Y%m%d_%H:%M:%S')
    print("END END TIME", mytime)
    sys.exit()

#########
# STEP 7: Make htmls/webpage
#########
status=False
#sys.exit()
print("Run STEP 7: Make htmls/webpage")

word=WholeHTML.WholeHTML(MLScoreFake,RADEC,season,masterTableInfo, outdir)

if word == "Functional":
    statusNew=True
    statusList[8]=statusNew
else:
    statusList[8]=status
print('step 7 status', status)

update=updateStatus.updateStatus(statusList,season)
print(update)
print('statusList',statusList)


##################################
# Move plots and htmls into      #
# directories established above  #
##################################
#os.system('mv candidate*.html '+str(outdir)+'/htmls/')
#os.system('mv *.png '+str(outdir)+'/pngs/')

#os.system('mkdir '+str(outdir)+'/gifs/')
#os.system('mv *.gif '+str(outdir)+'/gifs/')

#os.system('mv GiantTarList.txt '+str(outdir)+'/')

runStatus='complete'
statusList[9]=runStatus
update=updateStatus.updateStatus(statusList,season)
print(update)

mytime = datetime.datetime.now().strftime('%Y%m%d_%H:%M:%S')
print("END END TIME", mytime)
