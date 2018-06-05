import numpy as np
import pandas as pd
import easyaccess as ea
from glob import glob
import os
import sys
from collections import Counter
import fnmatch

base = '/pnfs/des/persistent/gw/exp'

ls = '/data/des41.b/data/rbutler/sb/bench/PostProcessing/event2_trim.list'
biglist = '/data/des41.b/data/rbutler/desgw/event2test/exposures.list'
colnames = ['expnum','nite','mjd_obs','radeg','decdeg','band','exptime','propid','obstype','object']


#df = pd.read_csv(biglist,delim_whitespace=True)#,header=0,names=colnames)

#expnum = np.genfromtxt(biglist,delimiter=[6],dtype=['int'],skip_header=1,usecols=(0))

#print expnum[:10]
#print nite[:10]

#print df[:10]
#print df.ix[:10,'band']
#print list(df)

#sys.exit()

if os.path.exists(ls):
    f = open(ls,'r')
    exps = f.readlines()
    f.close()
    exps = map(lambda s: int(s.strip()), exps)
else:
    exps = [497767,498267,498268]

exps = exps[:]
#exps = [506423,506424,506425,506426,506427,506428,506429,506430,506431,506432]
#exps = [506430]
season = 208
bands = 'i'

if bands=='all':
    bands = None
else:
    bands = bands.split(',')

print exps

connection = ea.connect('desoper')
cursor = connection.cursor()

if len(exps)>1:
    exps = str(tuple(exps))
    query = 'select expnum, band, nite from exposure where expnum in '+exps
else:
    query = 'select expnum, band, nite from exposure where expnum='+str(exps[0])

df = connection.query_to_pandas(query)

df = df.loc[df['BAND'].isin(bands)]
df = df.sort_values(by='EXPNUM')
df = df.reset_index(drop=True)

length = len(df)
bad = []
total = 0
notemp = 0
fznotfound = 0
starnotfound = 0
other = 0

chips = range(1,63)
chips.remove(2),chips.remove(31),chips.remove(61)

for ind in range(len(df)):
    expnum = str(df.get_value(ind,'EXPNUM'))
    nite = str(df.get_value(ind,'NITE'))
    band = str(df.get_value(ind,'BAND'))
    path = os.path.join(base,nite)
    path = os.path.join(path,expnum)
    path = os.path.join(path,'dp'+str(season))
    print
    print str(ind+1)+'/'+str(length)+' - '+path
    print
    for c in chips:
        c = '%02d' % c
        cpath = os.path.join(path,band+'_'+c)
        #print cpath
        if os.path.isdir(cpath):
            ffail = 'RUN03_expose_mkWStemplate.FAIL'
            allfiles = os.listdir(cpath)
            for fp in allfiles:
                if fnmatch.fnmatch(fp,'WSTemplate_*_GW*GWV1*.stdout'):
                    stdout = fp
                    if ffail in allfiles:
                    #if 1==1:
                        f = open(os.path.join(cpath,fp))
                        lines = f.readlines()
                        f.close()
                        #print expnum,c,lines[-1]
                        if '***FATAL' in lines[-1]:
                            total+=1
                            if '[.fz] not found' in lines[-1]:
                                fznotfound+=1
                                #print 'fz',os.path.join(cpath,stdout)
                            elif '*/ not found' in lines[-1]:
                                print expnum,c
                                for lin in lines:
                                    if 'DEBUG: exposure' in lin:
                                        
                                starnotfound+=1
                                #print '*/',os.path.join(cpath,stdout)
                            elif 'No good templates.' in lines[-1]:
                                print expnum,c
                                notemp+=1
                                #print 'NGT',os.path.join(cpath,stdout)
                            else:
                                for i in lines[-3:]:
                                    print i
                                other+=1


# for ind in range(len(df)):
#     expnum = str(df.get_value(ind,'EXPNUM'))
#     nite = str(df.get_value(ind,'NITE'))
#     band = str(df.get_value(ind,'BAND'))
#     path = os.path.join(base,nite)
#     path = os.path.join(path,expnum)
#     path = os.path.join(path,'dp'+str(season))
#     print
#     print str(ind+1)+'/'+str(length)+' - '+path
#     print
#     for c in chips:
#         c = '%02d' % c
#         cpath = os.path.join(path,band+'_'+c)
#         #print cpath
#         if os.path.isdir(cpath):
#             globout = glob(os.path.join(cpath,'WSTemplate_*_GW*GWV1*.stdout'))
#             pathfail = os.path.join(cpath,'RUN03_expose_mkWStemplate.FAIL')
#             if len(globout)==1 and os.path.isfile(pathfail):
#                 #print globout[0]
#                 f = open(globout[0],'r')
#                 lines = f.readlines()
#                 f.close()
#                 #print lines[-1]
#                 if '***FATAL' in lines[-1]:
#                     total+=1
#                     if '[.fz] not found' in lines[-1]:
#                         fznotfound+=1
#                         print 'fz',globout[0]
#                         #for g in lines[-1:]:
#                             #print g
#                             #print '-'*45
#                     elif '*/ not found' in lines[-1]:
#                         starnotfound+=1
#                         print '*/',globout[0]
#                     elif 'No good templates.' in lines[-1]:
#                         notemp+=1
#                     else:
#                         other+=1
#                     #print e, c
#                     #print lines[-1]
#                     #for l in lines[-3:]:
#                     #    print l
#                     #print
#                     #bad.append(int(lines[-12].split('_')[1]))
#                     #bad.append(lines[-12])

# for e in exps:
#     print str(exps.index(e)+1)+'/'+str(length)+' - '+str(e)
#     path = os.path.join(base,str(e))
#     path = os.path.join(path,'dp'+str(season))
#     for c in range(1,63):
#         ccd = '%02d' % c
#         globout = glob(path+'/*'+ccd+'/WSTemplate_*_GW*GWV1*.stdout')
#         globfail = glob(path+'/*'+ccd+'/RUN03_expose_mkWStemplate.FAIL') 
#         #print globout
#         if len(globout)==1 and len(globfail)==1:
#             #print globout[0]
#             f = open(globout[0],'r')
#             lines = f.readlines()
#             f.close()
#             #print lines[-1]
#             if '***FATAL' in lines[-1]:
#                 total+=1
#                 if 'not found' in lines[-1]:
#                     notfound+=1
#                     print globout[0]
#                     for g in lines[-1:]:
#                         print g
#                     print '-'*45
#                 elif 'No good templates.' in lines[-1]:
#                     notemp+=1
#                 else:
#                     other+=1
#                 #print e, c
#                 #print lines[-1]
#                 #for l in lines[-3:]:
#                 #    print l
#                 #print
#                 #bad.append(int(lines[-12].split('_')[1]))
#                 #bad.append(lines[-12])
            
print
print '---'
print

print total
print '[.fz] not found: %d, %.1f%%' % (fznotfound,((float(fznotfound)/float(total))*100))
print '...*/ not found: %d, %.1f%%' % (starnotfound,((float(starnotfound)/float(total))*100))
print 'no good templates: %d, %.1f%%' % (notemp,((float(notemp)/float(total))*100))
print 'other: %d, %.1f%%' % (other,((float(other)/float(total))*100))

sys.exit()

counts = Counter(bad)

dcounts = dict(counts)
#print dcounts

for i in dcounts:
    print i,':',dcounts[i]

total = len(bad)
print 
print 'total :',total 
