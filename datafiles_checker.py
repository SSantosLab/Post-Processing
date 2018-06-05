path = './RealLCurves_01_20_17_new/'

listfile = path + 'RealLCurves_01_20_17_new.LIST'

ls = open(listfile,'r+')
dats = ls.readlines()
ls.close()

hostlist = []
c = 0

for d in dats:
    c = c+1
    if c % 10000 == 0:
        print c
    datfile = path + d.split('\n')[0]
    
    f = open(datfile,'r+')
    lines = f.readlines()
    f.close()
    
    number = lines[15].split()[1]
    
    if number!='-888':
        if c % 1000 == 0:
            print number
        hostlist.append(d) 
        
print 'len(hostlist):',len(hostlist)