import numpy as np
import os.path
import os
import sys
import timeit
import platform
import shutil
from glob import glob

start_time = timeit.default_timer()

# red rectangle = reject
# magenta circle = accepted real
# cyan circle = accepted fake
# green circle = "good" accepted real

ds9r = 12 #radius of circular region in pixels
ds9b = 20 #height/width of box region in pixels

###
configfile = sys.argv[-1]
config = open(configfile)
line1 = config.readline().split()
line2 = config.readline().split()
line3 = config.readline().split()
line4 = config.readline().split()
line5 = config.readline().split()
line6 = config.readline().split()
line7 = config.readline().split()
expnums = eval(line1[1]) #
rootdir = line2[1]       #
outdir = line3[1]        # 
cand_list = line4[1]     # if none to be used, make filename 'none'
fake_list = line5[1]     # if none to be used, make filename 'none'
season = int(line6[1])
ccds = line7[1]    # if all, make this 'all'
###

if ccds=='all':
    ccds = range(1,63)
    ccds = np.asarray(ccds)
else:
    ccds = eval(ccds)
    
#print ccds

if cand_list!='none':
    SNID,candRA,candDEC = np.genfromtxt(cand_list, delimiter=',',skip_header=1,\
        usecols=(0,1,2),unpack=True)

for exp in expnums:
    for index in ccds:
        number = str("%02d" % (index))
        chip = number
        part1 = rootdir + '*/'
        part2 = '/dp' + str(season) + '/*'
        part3 = '/*+fakeSN_filterObj.out'
        filename = part1 + str(exp) + part2 + chip + part3
        globfil = glob(filename)
        if len(globfil)>0 and os.path.isfile(globfil[0]):
            if (platform.node()=='des41.fnal.gov')==True:
                lineglobfil = open(globfil[0])
            else:
                try:
                    shutil.copy(globfil[0],".")
                    lineglobfil = open(globfil[0].split('/')[10])
                except IOError:
                    if os.path.isfile(globfil[0].split('/')[10]):
                        os.remove(globfil[0].split('/')[10])
                    print "Did you run export LD_PRELOAD=/usr/lib64/libpdcap.so.1",\
                    "before this code? If yes, some other problem occurred. If no, run it and try again." 
                    continue
            line1 = lineglobfil.readline()
            line2 = lineglobfil.readline()
            lineglobfil.close()
        
            ds9name = outdir+'ds9regions_dp'+str(season)+'_'+str(exp)+'_'+line1.split()[1]+'_'+str(number)+'.reg'
            
            ds9file = open(ds9name, 'w+')
            
            ds9file.write('global font="helvetica 10 bold"\n')
            
            if (platform.node()=='des41.fnal.gov')==False:
                ID,MJD2,CCDNUM2,reject,RA,DEC,x,y,SN_FAKEID = np.genfromtxt(\
                    globfil[0].split('/')[10], delimiter=' ',skip_header=18, skip_footer=5,\
                    usecols=(1,3,5,6,8,9,12,13,45),unpack=True)
                os.remove(globfil[0].split('/')[10])
            else:
                ID,MJD2,CCDNUM2,reject,RA,DEC,x,y,SN_FAKEID = np.genfromtxt(\
                    globfil[0], delimiter=' ',skip_header=18, skip_footer=5,\
                    usecols=(1,3,5,6,8,9,12,13,45),unpack=True)
            
            #x = np.around(x)
            #y = np.around(y)
            
            number2 = str("%02d" % (index))
            
            for i in range(len(ID)):
                if reject[i]==0:
                    if SN_FAKEID[i]==0:
                        if cand_list!='none':
                            flag = 'False'
                            for cra in range(len(candRA)):
                                if RA[i]>=(candRA[cra]-.0005) and RA[i]<=(candRA[cra]+.0005):
                                    if DEC[i]>=(candDEC[cra]-.0005) and DEC[i]<=(candDEC[cra]+.0005):
                                        flag = 'True'
                                        candid = SNID[cra]
                                        #print candid, RA[i]
                            if flag=='True':
                                ds9file.write('circle(')
                                ds9file.write(str(x[i])+','),ds9file.write(str(y[i])+',')
                                ds9file.write(str(ds9r)+')')
                                ds9file.write(' # color = green ')
                                ds9file.write('text={'),ds9file.write(str(int(candid))),ds9file.write('}\n') 
                            else:
                                ds9file.write('circle(')
                                ds9file.write(str(x[i])+','),ds9file.write(str(y[i])+',')
                                ds9file.write(str(ds9r)+')')
                                ds9file.write(' # color = magenta ')
                                ds9file.write('text={'),ds9file.write(str(int(ID[i]))),ds9file.write('}\n')           
		        
  		        else:
  		            ds9file.write('circle(')
                            ds9file.write(str(x[i])+','),ds9file.write(str(y[i])+',')
                            ds9file.write(str(ds9r)+')')
                            ds9file.write(' # color = magenta ')
                            ds9file.write('text={'),ds9file.write(str(int(ID[i]))),ds9file.write('}\n')
############################################################################
###############################    FAKES    ################################
############################################################################
                
                    else: 
                        ds9file.write('circle(')
                        ds9file.write(str(x[i])+','),ds9file.write(str(y[i])+',')
                        ds9file.write(str(ds9r)+')')
                        ds9file.write(' # color = cyan ')
                        ds9file.write('text={'),ds9file.write(str(int(SN_FAKEID[i]))),ds9file.write('}\n')
                else:
                    ds9file.write('box(')
                    ds9file.write(str(x[i])+','),ds9file.write(str(y[i])+',')
                    ds9file.write(str(ds9b)+','),ds9file.write(str(ds9b)+',')
                    ds9file.write('0)')
                    ds9file.write(' # color = red ')
                    ds9file.write('text={'),ds9file.write(str(int(ID[i]))),ds9file.write('}\n')
        
            ds9file.close()    

elapsed = timeit.default_timer() - start_time
print round(elapsed,3)
