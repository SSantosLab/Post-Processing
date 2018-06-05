import sys
import argparse

parser = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
parser.add_argument('-f','--expfile', metavar='FILE', type=str, help='Path to list of exposures.')
parser.add_argument('-t','--timecut', metavar='HOURS', type=float, help='Time value, in hours before current time. This value will determine the cutoff--i.e. you will not delete .FAIL files created after that time.')
parser.add_argument('-s','--season', metavar='SEASON', type=int, help='season number')
parser.add_argument('-d','--delete', metavar='True/False', help='If False, code will simply find .FAIL files satisfying criteria. If True, they will be deleted as well.\n(default=False)',default=False)
parser.add_argument('-l','--listfile', metavar='LISTFILE', type=str, help='Filename to output list of .FAIL files to',default = 'fails.list')
args = parser.parse_args()

expfile = args.expfile
timecut = args.timecut
season = args.season
delete = args.delete
listfile = args.listfile

if args.delete=='True':
    args.delete=True
else:
    args.delete=False

print
if not timecut or not season or not expfile:
    parser.print_help()
    sys.exit()
else:
    print args
    print
    import pandas as pd
    import os
    import datetime
    import numpy as np
    import easyaccess as ea

f = open(expfile,'r')
exps = f.read().splitlines()
f.close()
exps = [int(e) for e in exps]

print 'EXPOSURES GIVEN:',exps
exps = str(tuple(exps))

connection = ea.connect('desoper')
cursor = connection.cursor()

query = 'select expnum, band, nite from exposure where expnum in '+exps

df = connection.query_to_pandas(query)

df = df.sort_values(by='EXPNUM')
df = df.reset_index(drop=True)
#print df 

#sys.exit()

base = '/pnfs/des/persistent/gw/exp'

timeagocut_s = timecut*60*60

chips = range(1,63)
chips.remove(2),chips.remove(31),chips.remove(61)

season = str(season)

allfail = []

length = len(df)

lf=open(listfile,'w+')

for ind in range(len(df)):
    expnum = str(df.get_value(ind,'EXPNUM'))
    nite = str(df.get_value(ind,'NITE'))
    band = str(df.get_value(ind,'BAND'))
    path = os.path.join(base,nite)
    path = os.path.join(path,expnum)
    path = os.path.join(path,'dp'+season)
    print
    print str(ind+1)+'/'+str(length)+' - '+path
    print
    for c in chips:
        c = '%02d' % c
        cpath = os.path.join(path,band+'_'+c)
        #print cpath
        if os.path.isdir(cpath):
            for f in os.listdir(cpath):
                fullpath = os.path.join(cpath,f)
                try:
                    timestamp = os.stat(fullpath).st_ctime
                except:
                    continue
                createtime = datetime.datetime.fromtimestamp(timestamp)
                now = datetime.datetime.now()
                delta = now - createtime
                #m, s = divmod(delta.total_seconds(), 60)
                #h, m = divmod(m, 60)
                if delta.total_seconds() > (timeagocut_s) and '.FAIL' in f:
                    #print "%d:%02d:%02d" % (h, m, s)
                    #print fullpath
                    allfail.append(fullpath)
                    lf.write(fullpath+'\n')
                    if delete:
                        os.remove(fullpath)
    lf.flush()
    os.fsync(lf)

lf.close()
#np.savetxt(listfile,allfail,fmt='%s')
