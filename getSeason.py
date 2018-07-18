import argparse

parser = argparse.ArgumentParser(description=__doc__,formatter_class=argparse.RawDescriptionHelpFormatter)
parser.add_argument('--ini', help='.ini file name', type=str)
args=parser.parse_args()
ini=args.ini

def getSeason(iniFile):
    #print(iniFile)
    ini=str(iniFile)
    season=ini.split('.')[0].split('_')[1]
    print(season)
    return int(season)

season=str(getSeason(ini))
s=open('getSeason.txt','w+')
s.write(season)
s.close()
