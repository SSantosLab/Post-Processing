import sys
import os

try:
    pathlist = sys.argv[1]
except:
    print 'Provide path to file containing list of paths to files you want to remove.'
    sys.exit()

f = open(pathlist,'r')
fails = f.read().splitlines()
f.close()

length = len(fails)

for i in fails:
    try:
        os.remove(i)
    except:
        findex = fails.index(i)+1
        if (findex % 100 == 0) or (findex == length):
            print str(findex)+'/'+str(length)
        continue
    findex = fails.index(i)+1
    if (findex % 100 == 0) or (findex == length):
        print str(findex)+'/'+str(length)
