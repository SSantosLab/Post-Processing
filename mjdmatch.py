import numpy as np
import csv
from astropy.io import fits 
import sys

### exp .csv file should be ordered by exposure number
### mjd .csv file should be ordered by exposure number
### counts .csv file should be ordered by RA

### this should have no duplicates
expnum1,ra1,dec1= np.genfromtxt('execute_exp_01_23_17.csv',delimiter=',',skip_header=1,\
    usecols=(0,1,2),unpack=True)
    
date1,hex1= np.genfromtxt('execute_exp_01_23_17.csv',dtype='string',delimiter=',',skip_header=1,\
    usecols=(10,11),unpack=True)
    
mjd2,expnum2,ra2,dec2= np.genfromtxt('execute_mjd_01_23_17.csv',delimiter=',',skip_header=1,\
    usecols=(0,2,3,4),unpack=True)
    
date2,hex2= np.genfromtxt('execute_mjd_01_23_17.csv',dtype='string',delimiter=',',skip_header=1,\
    usecols=(1,6),unpack=True)

count3,ra3,dec3 = np.genfromtxt('execute_count_01_23_17.csv',delimiter=',',skip_header=1,\
    usecols=(0,1,2),unpack=True)
    
hex3 = np.genfromtxt('execute_count_01_23_17.csv',dtype='string',delimiter=',',skip_header=1,\
    usecols=(3),unpack=True)
    
blacklist = np.genfromtxt('blacklist.txt',usecols=(0),unpack=True)
#print ra2

print expnum1[0],ra1[0],dec1[0]
print expnum2[0],ra2[0],dec2[0]



#a=[0,1,2,3,4,5,6,7]
#a = np.asarray(a)
#print a,type(a)
#a = a.tolist()
#print a
#
#print type(mjd2)
#
#print len(expnum1)
#print len(mjd2)

#mjd2 = mjd2.tolist()

#print type(mjd2)

print len(ra2),"len(ra2) before"
print len(ra1),"len(ra1) before"

#mjd2,date2,expnum2,hex2,ra2,dec2=mjd2.tolist(),date2.tolist(),\
#    expnum2.tolist(),hex2.tolist(),ra2.tolist(),dec2.tolist()

mjdt,datet,expnumt,hext,rat,dect=[],[],[],[],[],[]

for i in range(len(mjd2)):
    if i==0 and expnum2[i] not in blacklist:
        mjdt.append(mjd2[i]),datet.append(date2[i]),expnumt.append(expnum2[i]),\
            hext.append(hex2[i]),rat.append(ra2[i]),dect.append(dec2[i])
    elif i>=1 and ra2[i]!=ra2[i-1] and expnum2[i] not in blacklist:
        mjdt.append(mjd2[i]),datet.append(date2[i]),expnumt.append(expnum2[i]),\
            hext.append(hex2[i]),rat.append(ra2[i]),dect.append(dec2[i])
    else:
        print expnum2[i]

mjd2,date2,expnum2,hex2,ra2,dec2=mjdt,datet,expnumt,hext,rat,dect
#print mjd2 
#print date2
#print expnum2,
#print hex2
#print ra2
#print dec2
mjdt,datet,expnumt,hext,rat,dect=[],[],[],[],[],[]

for i in range(len(ra1)):
    if i==0 and expnum1[i] not in blacklist:
        datet.append(date1[i]),expnumt.append(expnum1[i]),\
            hext.append(hex1[i]),rat.append(ra1[i]),dect.append(dec1[i])
    elif i>=1 and ra1[i]!=ra1[i-1] and expnum1[i] not in blacklist:
        datet.append(date1[i]),expnumt.append(expnum1[i]),\
            hext.append(hex1[i]),rat.append(ra1[i]),dect.append(dec1[i])
    else:
        print expnum1[i]

date1,expnum1,hex1,ra1,dec1=datet,expnumt,hext,rat,dect
print len(ra1),"len(ra1)"
#mjd2,date2,expnum2,hex2,ra2,dec2=np.asarray(mjd2),np.asarray(date2),\
#    np.asarray(expnum2),np.asarray(hex2),np.asarray(ra2),np.asarray(dec2)        
                        
print len(ra2), "len(ra2)"
print
print "in mjd list but not in exposure list:"
for j in range(len(expnum2)):
    if expnum2[j] not in expnum1:
        print expnum2[j],'|',date2[j],'|',hex2[j],'|',ra2[j]
print '--------'
print "vice versa:"
for j in range(len(expnum1)):
    if expnum1[j] not in expnum2:
        print expnum1[j],'|',date1[j],'|',hex1[j],'|',ra1[j]
        
print
print len(set(hex1))
print len(set(hex2))

c, d, a, b = zip(*sorted(zip(ra2,expnum2,hex2,date2)))

for x in range(len(a)):
    #print d[x], a[x],b[x],c[x]
    pass
print '----------------------------------'  
xlist = []
for y in range(len(hex2)):
    if hex2[y] not in hex3:
        print y,hex2[y]
        xlist.append(y)
        pass
print 
for z in hex3:
    if z not in set(hex2):
        #print z
        pass

mjdt,datet,expnumt,hext,rat,dect=[],[],[],[],[],[]

for i in range(len(mjd2)):
    if i not in xlist:
        mjdt.append(mjd2[i]),datet.append(date2[i]),expnumt.append(expnum2[i]),\
            hext.append(hex2[i]),rat.append(ra2[i]),dect.append(dec2[i])
    else:
        print expnum2[i], hex2[i]

mjd2,date2,expnum2,hex2,ra2,dec2=mjdt,datet,expnumt,hext,rat,dect

print len(mjd2),len(date2),len(expnum2),len(hex2),len(ra2),len(dec2)
print len(set(hex2))

print '----------------'

ra2,dec2,expnum2,hex2,date2,mjd2 = zip(*sorted(zip(ra2,dec2,expnum2,hex2,date2,mjd2)))

print ra2[0],dec2[0],expnum2[0],hex2[0],date2[0],mjd2[0]



for el in range(len(expnum2)):
    if el<10:
        print expnum2[el],'|',date2[el],'|',hex2[el],'|',ra2[el]
        
epoch = []

noDupes = []
[noDupes.append(i) for i in ra2 if not noDupes.count(i)]
for x in noDupes:
    ep = 0
    for y in range(len(ra2)):
        if x==ra2[y]:
            ep = ep + 1
            epoch.append(ep)
print             
print len(epoch)
#print epoch


#ep1 = [1]*len(ra_all)
#ep2 = [2]*len(ra_all)
#ep3 = [3]*len(ra_all)
expnum_ep1,expnum_ep2,expnum_ep3,expnum_ep4 = [],[],[],[]
mjd_ep1,mjd_ep2,mjd_ep3,mjd_ep4 = [],[],[],[]
date_ep1,date_ep2,date_ep3,date_ep4 = [],[],[],[]

foo = open('exposurelist4.txt','w+')
for xx in range(len(sorted(expnum2))):
    foo.write(str(int(sorted(expnum2)[xx])))
    if xx!=(len(sorted(expnum2))-1):
        foo.write(',')
    
foo.close

ra_,expnum_,dec_,hex_,date_,mjd_ = zip(*sorted(zip(ra2,expnum2,dec2,hex2,date2,mjd2)))
print 
print expnum_[0],ra_[0],dec_[0],hex_[0],date_[0],mjd_[0]

#print ra_
noDupes = []
[noDupes.append(i) for i in ra_ if not noDupes.count(i)]
ra_all = noDupes
print 
#print ra_all
dec_all = []
noDupes = []
[noDupes.append(i) for i in hex_ if not noDupes.count(i)]
hex_all = noDupes
print ra_all[0],hex_all[0]
print '^^^^^^^^^^^^^^'
###########################CORRECT TO HERE######################################

#fook = open('exposureinfo.txt','w+')
#fook.write('exposure\tra\tdec\n')
#for yy in range(len(expnum_)):
#    fook.write(str(int(expnum_[yy]))),fook.write('\t')
#    fook.write(str(ra_[yy])),fook.write('\t')
#    fook.write(str(dec_[yy])),
#    if yy!=(len(expnum_)-1):
#        fook.write('\n')  
#        
#fook.close() 

expnum_ep4,mjd_ep4 = np.zeros(85),np.zeros(85)
date_ep4 = np.chararray(85,itemsize=10)
date_ep4[:] = '0'

print 'HERE', len(ra2), len(ra_all)
for k in range(len(epoch)):
    if epoch[k]==1:
        expnum_ep1.append(expnum2[k])
        mjd_ep1.append(mjd2[k])
        date_ep1.append(date2[k])
        dec_all.append(dec2[k])
    if epoch[k]==2:
        expnum_ep2.append(expnum2[k])
        mjd_ep2.append(mjd2[k])
        date_ep2.append(date2[k])       
    if epoch[k]==3:
        expnum_ep3.append(expnum2[k])
        mjd_ep3.append(mjd2[k])
        date_ep3.append(date2[k])
    if epoch[k]==4:
        #expnum_ep4.append(expnum2[k])
        #mjd_ep4.append(mjd2[k])
        #date_ep4.append(date2[k])        
        for ka in range(len(ra_all)):
            if ra2[k]==ra_all[ka]:
                expnum_ep4[ka] = expnum2[k]
                mjd_ep4[ka] = mjd2[k]
                date_ep4[ka] = date2[k]
            #else:
            #    expnum_ep4.append(0)
            #    mjd_ep4.append(0)
            #    date_ep4.append(0)                
print 
print ra_all[0],expnum_ep1[0],expnum_ep2[0]
print 
#print dec_all
print len(ra_all),len(dec_all),len(hex_all)
print len(expnum_ep1),len(expnum_ep2),len(expnum_ep3)
print len(mjd_ep1),len(mjd_ep2),len(mjd_ep3)
print len(date_ep1),len(date_ep2),len(date_ep3)

print expnum_ep1[0],ra_all[0],dec_all[0]

ra_all = np.round(ra_all,decimals=6)
dec_all = np.round(dec_all,decimals=5)

for gg in range(len(ra_all)):
    if ra_all[gg]>300:
        ra_all[gg]=ra_all[gg]-360

#with open('MasterExposureList4.txt', 'w') as f:
#    writer = csv.writer(f, delimiter='\t')
#    f.write("hex\tra\tdec\texpnum_ep1\tmjd_ep1\tdate_ep1\texpnum_ep2\tmjd_ep2\tdate_ep2\texpnum_ep3\tmjd_ep3\tdate_ep3\n")
#    writer.writerows(zip(hex_all,ra_all,dec_all,expnum_ep1,mjd_ep1,date_ep1,\
#        expnum_ep2,mjd_ep2,date_ep2,expnum_ep3,mjd_ep3,date_ep3))
  
#ra1,dec1,snr1,exp1 = np.genfromtxt('epoch1.txt',usecols=(0,1,2,3),\
#    unpack=True)
#
#ra2,dec2,snr2,exp2 = np.genfromtxt('epoch2.txt',usecols=(0,1,2,3),\
#    unpack=True)
#
#ra3,dec3,snr3,exp3 = np.genfromtxt('epoch3.txt',usecols=(0,1,2,3),\
#    unpack=True)
#    
#ra1,dec1,snr1,exp1 = zip(*sorted(zip(ra1,dec1,snr1,exp1)))
#ra2,dec2,snr2,exp2 = zip(*sorted(zip(ra2,dec2,snr2,exp2)))
#ra3,dec3,snr3,exp3 = zip(*sorted(zip(ra3,dec3,snr3,exp3)))

rownum = range(1,86)
                              
tbhdu1 = fits.BinTableHDU.from_columns(
[#fits.Column(name='row', format='K', array=rownum),
fits.Column(name='hex', format='A69', array=hex_all),
fits.Column(name='RA', format='E', array=ra_all),
fits.Column(name='DEC', format='E', array=dec_all),
fits.Column(name='exp_ep1', format='K', array=expnum_ep1),
fits.Column(name='mjd_ep1',format='K',array=mjd_ep1),
fits.Column(name='date_ep1',format='A10',array=date_ep1),
#fits.Column(name='SNRfakes_ep1',format='E',array=snr1),
fits.Column(name='exp_ep2', format='K', array=expnum_ep2),
fits.Column(name='mjd_ep2',format='K',array=mjd_ep2),
fits.Column(name='date_ep2',format='A10',array=date_ep2),
#fits.Column(name='SNRfakes_ep2',format='E',array=snr2),
fits.Column(name='exp_ep3', format='K', array=expnum_ep3),
fits.Column(name='mjd_ep3',format='K',array=mjd_ep3),
fits.Column(name='date_ep3',format='A10',array=date_ep3),
#fits.Column(name='SNRfakes_ep3',format='E',array=snr3)
fits.Column(name='exp_ep4', format='K', array=expnum_ep4),
fits.Column(name='mjd_ep4',format='K',array=mjd_ep4),
fits.Column(name='date_ep4',format='A10',array=date_ep4)
])

final = 'MasterExposureList4Epochs_new4.fits'
tbhdu1.writeto(final,clobber=True)

