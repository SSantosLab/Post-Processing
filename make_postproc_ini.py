import ConfigParser
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--season', type=int, help="season #, as determined by main-injector.")
parser.add_argument('--triggerid', type=str, help="ligo trigger id e.g. G######")
parser.add_argument('--ligoid', type=str, help="ligo id e.g. GW######")
parser.add_argument('--recycler_mjd', type=float, help="recycler mjd")
parser.add_argument('--propid', type=str, default='2017B-0110', help="propid 20##B-####")
args = parser.parse_args()

config = ConfigParser.RawConfigParser()

config.add_section('general')
config.set('general', 'season', args.season) #416/417 
config.set('general', 'ligoid', args.ligoid) #GW170814, possibly in yaml?
config.set('general', 'triggerid', args.triggerid) #G297595 in yaml
config.set('general', 'propid', args.propid) #2017B-0110 not in yaml
config.set('general', 'triggermjd', args.recycler_mjd) #57979.437 in yaml
config.set('general', 'ups', 'False')
config.set('general', 'env_setup_file', './diffimg_setup.sh')
config.set('general', 'rootdir', '/pnfs/des/persistent/gw')
config.set('general', 'outdir', '/data/des41.a/data/desgw/postproc/'+str(args.season)+'/outputs')
config.set('general', 'indir', './')
config.set('general', 'db', 'destest')
config.set('general', 'schema', 'marcelle')
config.set('general', 'exposures_listfile', '/data/des40.b/data/kherner/GW170814_postproc/postproc/exposures_ZDtest.list') #/data/des40.b/data/kherner/GW170817_postproc/postproc/exposures_GW170817_night1.list
#exposures_ZDtest.list ; # exposures_GW170814_teff_cut.list ; #if you have this, store it in indir indicated above
config.set('general', 'bands', 'i') # all if using all bands, list or single band if making selection (ex: i,z or i)

config.add_section('plots')
config.set('plots', 'mlscore_cut', '0.7')

config.add_section('masterlist')
config.set('masterlist', 'blacklist', 'blacklist.txt') #if you have this, store it in indir indicated above
config.set('masterlist', 'filename_1', 'MasterExposureList_prelim.fits')
config.set('masterlist', 'filename_2', 'MasterExposureList.fits')

config.add_section('checkoutputs')
config.set('checkoutputs', 'logfile','checkoutputs.log')
config.set('checkoutputs', 'ccdfile', 'checkoutputs.csv')
config.set('checkoutputs', 'goodfile', 'goodchecked.list')
config.set('checkoutputs', 'steplist', 'steplist.txt') #store this in indir indicated above (needed)

config.add_section('GWFORCE')
config.set('GWFORCE', 'numepochs_min','0')
config.set('GWFORCE', 'ncore','8')
config.set('GWFORCE','writeDB','True')

config.add_section('HOSTMATCH')
config.set('HOSTMATCH', 'version', 'v1.0.1')

config.add_section('truthtable')
config.set('truthtable', 'filename','fakes_truth.tab')
config.set('truthtable', 'plusname','truthplus.tab')

config.add_section('GWmakeDataFiles')
config.set('GWmakeDataFiles','format','snana')
config.set('GWmakeDataFiles','numepochs_min','0')
config.set('GWmakeDataFiles','2nite_trigger','null')

config.add_section('GWmakeDataFiles-fake')
config.set('GWmakeDataFiles-fake', 'outFile_stdout','makeDataFiles_fake.stdout')
config.set('GWmakeDataFiles-fake', 'outDir_data','LightCurvesFake')
config.set('GWmakeDataFiles-fake', 'version','FIXMAGGW170814noHost')

config.add_section('GWmakeDataFiles-real')
config.set('GWmakeDataFiles-real', 'outFile_stdout','makeDataFiles_real.stdout')
config.set('GWmakeDataFiles-real', 'outDir_data', 'LightCurvesReal')
config.set('GWmakeDataFiles-real', 'combined_fits','datafiles_combined.fits')

with open('test_makeini.ini', 'wb') as configfile:
    config.write(configfile)
