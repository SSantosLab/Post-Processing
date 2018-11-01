import configparser
import argparse


parser = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
parser.add_argument('--season' ,help='Season Number!',type=int)
parser.add_argument('--expList',help='List of Exposures for event',type=str)
args = parser.parse_args()
season=str(args.season)
expList=args.expList

config = configparser.ConfigParser()
config.read('postproc_'+season+'.ini')

#config['general']['exposures_listfile']=expList
config.set('general', 'exposures_listfile', expList)

with open('postproc_'+season+'.ini', 'w') as configfile:
    config.write(configfile)
