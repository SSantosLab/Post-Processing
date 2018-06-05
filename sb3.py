import numpy as np
from astropy.io import fits
import os.path
import os
import shutil
import timeit
import datetime
import sys
import platform
from glob import glob
from joblib import Parallel, delayed

cpus = 16 #number of cores to be used

first = 1 #CCD
last = 62 #CCD

start_time = timeit.default_timer()
elapsed0 = 0
elapsed00 = 0  
            
#INPUT
configfile = sys.argv[1]
config = open(configfile)
line1 = config.readline().split()
line2 = config.readline().split()
line3 = config.readline().split()
line4 = config.readline().split()
line5 = config.readline().split()
line6 = config.readline().split()
expnums = eval(line1[1]) #
rootdir = line2[1]       #
outdir = line3[1]        # 
cand_list = line4[1]     # if none to be used, make filename 'none'
fake_list = line5[1]     # if none to be used, make filename 'none'
season = int(line6[1])   #

try:
    rejarg = sys.argv[2]
except IndexError:
    rejarg = 'none'

scale = 0.2623
                                                                 
def bigloop(index):
    global rootdir,outdir,cand_list,fake_list,season
    #print "1 :", timeit.default_timer() - start_time
    rows = []
    number = str("%02d" % (index))
    chip = number
    part1 = rootdir + '*/'
    part2 = '/dp' + str(season) + '/*'
    part3 = '/*+fakeSN_filterObj.out'
    filename = part1 + str(exp) + part2 + chip + part3
    globfil = glob(filename)
    
    part4 = '/*+fakeSN_autoScan.out'
    autofilename = part1 + str(exp) + part2 + chip + part4
    autoglobfil = glob(autofilename)
    
    if len(globfil)>0 and os.path.isfile(globfil[0]) and len(autoglobfil)>0:
        if (platform.node()=='des41.fnal.gov')==True:
            lineglobfil = open(globfil[0])
        else:
            try:
                shutil.copy(globfil[0],".")
                shutil.copy(autoglobfil[0],".")
                lineglobfil = open(globfil[0].split('/')[10])
            except IOError:
                if os.path.isfile(globfil[0].split('/')[10]):
                    os.remove(globfil[0].split('/')[10])
                if os.path.isfile(autoglobfil[0].split('/')[10]):
                    os.remove(autoglobfil[0].split('/')[10])
                print "Did you run export LD_PRELOAD=/usr/lib64/libpdcap.so.1",\
                "before this code? If yes, some other problem occurred. If no, run it and try again." 
                return
                
        line1 = lineglobfil.readline()
        line2 = lineglobfil.readline()
        lineglobfil.close()
        
        #print line1
        #print line2
        if (platform.node()=='des41.fnal.gov')==False:
            ID,MJD2,CCDNUM2,reject,RA,DEC,MAG,MAGERR,x,y,FLUX,FLUXERR,SN_FAKEID = np.genfromtxt(\
                globfil[0].split('/')[10], delimiter=' ',skip_header=18, skip_footer=5,\
                usecols=(1,3,5,6,8,9,10,11,12,13,16,17,45),unpack=True)
            os.remove(globfil[0].split('/')[10])
            MLID,SCORE = np.genfromtxt(autoglobfil[0].split('/')[10],skip_header=3,\
                usecols=(1,2),unpack=True)
            os.remove(autoglobfil[0].split('/')[10])
        else:
            ID,MJD2,CCDNUM2,reject,RA,DEC,MAG,MAGERR,x,y,FLUX,FLUXERR,SN_FAKEID = np.genfromtxt(\
                globfil[0], delimiter=' ',skip_header=18, skip_footer=5,\
                usecols=(1,3,5,6,8,9,10,11,12,13,16,17,45),unpack=True)
            MLID,SCORE = np.genfromtxt(autoglobfil[0],skip_header=3,\
                usecols=(1,2),unpack=True)            
        
        x = np.around(x)
        y = np.around(y)
        #print "2 :", timeit.default_timer() - start_time
        
        number2 = str("%02d" % (index))
            
        part1a = rootdir + '/*'
        part2a = '/dp' + str(season) + '/*'
        part3a = '/*template.fits'
        image = part1a + '/' + str(exp) + part2a + chip + part3a
        imagefil = glob(image)
        if len(imagefil)==0:
            return
        if (platform.node()=='des41.fnal.gov')==True:
            imagefil = imagefil[0]
        else:
            shutil.copy(imagefil[0],".")
            imagefil = imagefil[0].split('/')[10]
            
        fits1 = fits.open(imagefil)
        
        data1 = fits1[0].data
        
        scale = 0.2623 # arcsec/pixel; from ds9; header says .27         
        
        template_zp = 31.1982 
        filterObj_zp = 31.4
        
        avg_bg = 0.
            
        a = 0
        b = 0
        c = 0 
        d = 0

        for p in range(len(ID)):
            if reject[p]==0.:
                b=b+1
        
        print ' '
        print 'total candidates =',len(ID), "for chip", number2
        print 'non-rejects =',b
        
        for q in range(len(ID)):
            if SN_FAKEID[q]!=0.:
                c=c+1
            
        print 'fakes =',c

        for i in range(len(ID)):
            if reject[i]==0:
		if SN_FAKEID[i]==0:
		    #print "3a :", timeit.default_timer() - start_time
                    # image coordinates
                    det1 = Detections(0,-99,-1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,-123,0)
                    
                    x_low, x_up, y_low, y_up = 0,0,0,0
                    
                    x_low, y_up = x[i]-20, y[i]+20 
                    x_up, y_low = x[i]+20, y[i]-20 
                        
                    ###
                    
                    #coordinate adjustment for python starting at 0
                    x_low, x_up, y_low, y_up = int(x_low-1), int(x_up-1),\
                        int(y_low-1), int(y_up-1)
                    
                    ###
                    
                    ### be aware: in these cuts, the x-coordinate comes SECOND ###
                    
                    box = data1[y_low:y_up,x_low:x_up]
                    box = box.ravel()
                    
                    # subtract out average background
                    box = box - avg_bg
                    
                    row, col= np.indices((4096,2048)) 
                    
                    new_row = row - y[i]
                    new_col = col - x[i]
                    
                    row_x = new_row[y_low:y_up,x_low:x_up]
                    col_y = new_col[y_low:y_up,x_low:x_up]
                    
                    row_x1 = row_x.ravel()
                    col_y1 = col_y.ravel()
                        
                    radius = scale*np.sqrt(col_y1**2+row_x1**2)
                    
                    ### loop that specifies area used in calculations ###
                    
                    z_radcut = 2.    #radius of circle around center
                    #z_isocut = 1400.   #pixel intensity lower cutoff
                    
                    z_circle = []
                    error = []

                    for i2 in range(len(box)):
                        if ((radius[i2] <= z_radcut)): # and 
                        #((box[i]+avg_bg) >=z_isocut)):
                            z_circle.append(box[i2])
                    
                    #since the gain in this particular image's header is ~0.98, I just used
                    # Poisson square root error in each pixel, and then averaged those 
                    # errors later.
                    for i3 in range(len(box)):
                        if ((radius[i3] <= z_radcut)):
                            poisson = np.sqrt(abs(box[i3]))
                            error.append(poisson)
                        len(z_circle)
                    ######################################################
                    
                    ### surface brightness and other calculations ###
                    
                    med = np.median(z_circle)
                    errmean = np.mean(error)
                    err.append(errmean)
                    stdev = np.std(z_circle)
                    std.append(stdev)
                    
                    #set flat or negative background to 30 mag/arcsec^2 SB
                    if med<=.207432394746636:
                        med = .207432394746636
                    
                    sb_med_circle = template_zp -2.5*np.log10(med/(scale**2))
                    
                    sn_g1 = 1.086*(errmean/med)
                    
                    p = med/(scale**2)

                    det1.sb= sb_med_circle
                    det1.sb_err = sn_g1
                                    
                    expband = line1.split()[1]
                    det1.rej = reject[i]
                    det1.band = line1.split()[1]
                    det1.nite = int(line2.split()[1])
                    det1.expnum = exp
                    det1.season = int(line2.split()[5])
                    det1.rad = z_radcut
                    det1.truemag = 0.
                    det1.trueflux = 0.
                    det1._id = ID[i]
                    det1.ra = RA[i]
                    det1.dec = DEC[i]
                    if FLUX[i]>0:
                        det1.realmag = filterObj_zp - 2.5*np.log10(FLUX[i])
                        det1.realmagerr = 2.5*np.log10(1+(FLUXERR[i]/FLUX[i]))
                    else:
                        det1.realmag = 30.
                        det1.realmagerr = 30.                        
                    det1.mag = MAG[i]+filterObj_zp
                    det1.magerr = MAGERR[i]
                    det1.flux = FLUX[i]
                    det1.fluxerr = FLUXERR[i]
                    det1.ccdnum = CCDNUM2[i]
                    det1.fakeid = SN_FAKEID[i] 
                    if cand_list!='none':
                        for cra in range(len(candRA)):
                            if RA[i]>=(candRA[cra]-.0005) and RA[i]<=(candRA[cra]+.0005):
                                for cdec in range(len(candDEC)):
                                    if DEC[i]>=(candDEC[cdec]-.0005) and DEC[i]<=(candDEC[cdec]+.0005) and cra==cdec:
                                        det1.snid = SNID[cdec]
                    for ml in range(len(MLID)):
                        if MLID[ml]==ID[i]:
                            score=SCORE[ml]
                    det1.ml_score = score
                    detections.append(det1)
                    #print "3 :", timeit.default_timer() - start_time
############################################################################
###############################    FAKES    ################################
############################################################################
                
                else: 
                    if fake_list=='none':
                        expband = line1.split()[1]
                        det1 = Detections(0,-99,-1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,-123,0)
                        d=d+1
                        # image coordinates
                        x_low, x_up, y_low, y_up = 0,0,0,0
                        
                        x_low, y_up = x[i]-20, y[i]+20 
                        x_up, y_low = x[i]+20, y[i]-20 
                        
                        ###
                        
                        #coordinate adjustment for python starting at 0
                        x_low, x_up, y_low, y_up = int(x_low-1), int(x_up-1),\
                            int(y_low-1), int(y_up-1)
    
                        ###
                        
                        ### be aware: in these cuts, the x-coordinate comes SECOND ###
                        
                        box = data1[y_low:y_up,x_low:x_up]
                        box = box.ravel()
                        
                        # subtract out average background
                        box = box - avg_bg
                        
                        row, col= np.indices((4096,2048)) 
                        
                        new_row = row - y[i]
                        new_col = col - x[i]
                        
                        row_x = new_row[y_low:y_up,x_low:x_up]
                        col_y = new_col[y_low:y_up,x_low:x_up]
                        
                        row_x1 = row_x.ravel()
                        col_y1 = col_y.ravel()
                            
                        radius = scale*np.sqrt(col_y1**2+row_x1**2)
                        
                        ### loop that specifies area used in calculations ###
                        
                        radcut = 2.    #radius of circle around center
                        #isocut = 1400.   #pixel intensity lower cutoff
                        
                        circle = []
                        error = []
                        
                        for i2 in range(len(box)):
                            if ((radius[i2] <= radcut)): # and 
                            #((box[i]+avg_bg) >=isocut)):
                                circle.append(box[i2])
                        
                        #since the gain in this particular image's header is ~0.98, I just used
                        # Poisson square root error in each pixel, and then averaged those 
                        # errors later.
                        for i3 in range(len(box)):
                            if ((radius[i3] <= radcut)):
                                poisson = np.sqrt(abs(box[i3]))
                                error.append(poisson)
                            len(circle)
                        ######################################################
    
                        ### surface brightness and other calculations ###
    
                        med = np.median(circle)
                        errmean = np.mean(error)
                        err.append(errmean)
                        stdev = np.std(circle)
                        std.append(stdev)
                        det1.season = int(line2.split()[5])
    
                        #set flat background to 30 mag/arcsec^2
                        if med<=.207432394746636:
                            med = .207432394746636
    
                        sb_med_circle = template_zp -2.5*np.log10(med/(scale**2))
                        
                        sn_g1 = 1.086*(errmean/med)
                        
                        p = med/(scale**2)
    
                        det1.sb = sb_med_circle
                        det1.sb_err = sn_g1
    
                        det1.rad = radcut
                        det1.rej = reject[i]
                        det1._id = ID[i]
                        det1.ra = RA[i]
                        det1.dec = DEC[i]
                        if FLUX[i]>0:
                            det1.realmag = filterObj_zp - 2.5*np.log10(FLUX[i])
                            det1.realmagerr = 2.5*np.log10(1+(FLUXERR[i]/FLUX[i]))
                        else:
                            det1.realmag = 30.
                            det1.realmagerr = 30. 
                        det1.mag = MAG[i]+filterObj_zp
                        det1.magerr = MAGERR[i]
                        det1.flux = FLUX[i]
                        det1.fluxerr = FLUXERR[i]
                        det1.ccdnum = CCDNUM2[i]
                        det1.fakeid = SN_FAKEID[i] 
                        det1.band = line1.split()[1]
                        det1.nite = int(line2.split()[1]) 
                        det1.expnum = exp
                        for ml in range(len(MLID)):
                            if MLID[ml]==ID[i]:
                                score=SCORE[ml]
                        det1.ml_score = score
                        detections.append(det1) 
                        
                    else:                        
                        expband = line1.split()[1]
                        det1 = Detections(0,-99,-1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,-123,0)
                        d=d+1      

                        for j in range(len(FAKEID)):
    
                            if (SN_FAKEID[i] == FAKEID[j]) and (CCDNUM2[i] == CCDNUM1[j]) \
                                and (MJD2[i] <= (MJD1[j]+.002)) and (MJD2[i] >= (MJD1[j]-.002))\
                                and (BAND[j] == line1.split()[1]):
                                    a = a+1
                                    if SN_FAKEID[i] not in fakelist:
                                        #print int(SN_FAKEID[i]), index
                                        det1.band = BAND[j]
                                        det1.truemag = TRUEMAG[j]
                                        det1.trueflux = TRUEFLUX[j]
                                        det1.nite = NITE[j]
                                        det1.expnum = EXPNUM[j]
                                    fakelist.append(SN_FAKEID[i])
    
                        # image coordinates
                        x_low, x_up, y_low, y_up = 0,0,0,0
                        
                        x_low, y_up = x[i]-20, y[i]+20 
                        x_up, y_low = x[i]+20, y[i]-20 
                        
                        ###
                        
                        #coordinate adjustment for python starting at 0
                        x_low, x_up, y_low, y_up = int(x_low-1), int(x_up-1),\
                            int(y_low-1), int(y_up-1)
    
                        ###
                        
                        ### be aware: in these cuts, the x-coordinate comes SECOND ###
                        
                        box = data1[y_low:y_up,x_low:x_up]
                        box = box.ravel()
                        
                        # subtract out average background
                        box = box - avg_bg
                        
                        row, col= np.indices((4096,2048)) 
                        
                        new_row = row - y[i]
                        new_col = col - x[i]
                        
                        row_x = new_row[y_low:y_up,x_low:x_up]
                        col_y = new_col[y_low:y_up,x_low:x_up]
                        
                        row_x1 = row_x.ravel()
                        col_y1 = col_y.ravel()
                            
                        radius = scale*np.sqrt(col_y1**2+row_x1**2)
                        
                        ### loop that specifies area used in calculations ###
                        
                        radcut = 2.    #radius of circle around center
                        #isocut = 1400.   #pixel intensity lower cutoff
                        
                        circle = []
                        error = []
                        
                        for i2 in range(len(box)):
                            if ((radius[i2] <= radcut)): # and 
                            #((box[i]+avg_bg) >=isocut)):
                                circle.append(box[i2])
                        
                        #since the gain in this particular image's header is ~0.98, I just used
                        # Poisson square root error in each pixel, and then averaged those 
                        # errors later.
                        for i3 in range(len(box)):
                            if ((radius[i3] <= radcut)):
                                poisson = np.sqrt(abs(box[i3]))
                                error.append(poisson)
                            len(circle)
                        ######################################################
    
                        ### surface brightness and other calculations ###
    
                        med = np.median(circle)
                        errmean = np.mean(error)
                        err.append(errmean)
                        stdev = np.std(circle)
                        std.append(stdev)
                        det1.season = int(line2.split()[5])
    
                        #set flat background to 30 mag/arcsec^2
                        if med<=.207432394746636:
                            med = .207432394746636
    
                        sb_med_circle = template_zp -2.5*np.log10(med/(scale**2))
                        
                        sn_g1 = 1.086*(errmean/med)
                        
                        p = med/(scale**2)
    
                        det1.sb = sb_med_circle
                        det1.sb_err = sn_g1
    
                        det1.rad = radcut
                        det1.rej = reject[i]
                        det1._id = ID[i]
                        det1.ra = RA[i]
                        det1.dec = DEC[i]
                        if FLUX[i]>0:
                            det1.realmag = filterObj_zp - 2.5*np.log10(FLUX[i])
                            det1.realmagerr = 2.5*np.log10(1+(FLUXERR[i]/FLUX[i]))
                        else:
                            det1.realmag = 30.
                            det1.realmagerr = 30. 
                        det1.mag = MAG[i]+filterObj_zp
                        det1.magerr = MAGERR[i]
                        det1.flux = FLUX[i]
                        det1.fluxerr = FLUXERR[i]
                        det1.ccdnum = CCDNUM2[i]
                        det1.fakeid = SN_FAKEID[i] 
                        for ml in range(len(MLID)):
                            if MLID[ml]==ID[i]:
                                score=SCORE[ml]
                        det1.ml_score = score 
                        detections.append(det1) 
            elif rejarg=='none':                
                expband = line1.split()[1]
                det1 = Detections(0,-99,-1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,-123,0)
                det1.sb = 0.
                det1.sb_err = 0.
                det1.band = line1.split()[1]
                det1.nite = int(line2.split()[1])
                det1.expnum = exp
                det1.rej = reject[i]
                det1.season = int(line2.split()[5])
                det1.rad = 0.
                det1.truemag = 0.
                det1.trueflux = 0.
                det1._id = ID[i]
                det1.ra = RA[i]
                det1.dec = DEC[i]
                if FLUX[i]>0:
                    det1.realmag = filterObj_zp - 2.5*np.log10(FLUX[i])
                    det1.realmagerr = 2.5*np.log10(1+(FLUXERR[i]/FLUX[i]))
                else:
                    det1.realmag = 30.
                    det1.realmagerr = 30. 
                det1.mag = MAG[i]+filterObj_zp
                det1.magerr = MAGERR[i]
                det1.flux = FLUX[i]
                det1.fluxerr = FLUXERR[i]
                det1.ccdnum = CCDNUM2[i]
                det1.fakeid = SN_FAKEID[i] 
                det1.ml_score = -0.1           
                detections.append(det1)
                #print "3b :", timeit.default_timer() - start_time
            else:
		#print "3a :", timeit.default_timer() - start_time
                # image coordinates
                det1 = Detections(0,-99,-1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,-123,0)
                
                x_low, x_up, y_low, y_up = 0,0,0,0
                
                x_low, y_up = x[i]-20, y[i]+20 
                x_up, y_low = x[i]+20, y[i]-20 
                    
                ###
                
                #coordinate adjustment for python starting at 0
                x_low, x_up, y_low, y_up = int(x_low-1), int(x_up-1),\
                    int(y_low-1), int(y_up-1)
                
                ###
                
                ### be aware: in these cuts, the x-coordinate comes SECOND ###
                
                box = data1[y_low:y_up,x_low:x_up]
                box = box.ravel()
                
                # subtract out average background
                box = box - avg_bg
                
                row, col= np.indices((4096,2048)) 
                
                new_row = row - y[i]
                new_col = col - x[i]
                
                row_x = new_row[y_low:y_up,x_low:x_up]
                col_y = new_col[y_low:y_up,x_low:x_up]
                
                row_x1 = row_x.ravel()
                col_y1 = col_y.ravel()
                    
                radius = scale*np.sqrt(col_y1**2+row_x1**2)
                
                ### loop that specifies area used in calculations ###
                
                z_radcut = 2.    #radius of circle around center
                #z_isocut = 1400.   #pixel intensity lower cutoff
                
                z_circle = []
                error = []

                for i2 in range(len(box)):
                    if ((radius[i2] <= z_radcut)): # and 
                    #((box[i]+avg_bg) >=z_isocut)):
                        z_circle.append(box[i2])
                
                #since the gain in this particular image's header is ~0.98, I just used
                # Poisson square root error in each pixel, and then averaged those 
                # errors later.
                for i3 in range(len(box)):
                    if ((radius[i3] <= z_radcut)):
                        poisson = np.sqrt(abs(box[i3]))
                        error.append(poisson)
                    len(z_circle)
                ######################################################
                
                ### surface brightness and other calculations ###
                
                med = np.median(z_circle)
                errmean = np.mean(error)
                err.append(errmean)
                stdev = np.std(z_circle)
                std.append(stdev)
                
                #set flat or negative background to 30 mag/arcsec^2 SB
                if med<=.207432394746636:
                    med = .207432394746636
                
                sb_med_circle = template_zp -2.5*np.log10(med/(scale**2))
                
                sn_g1 = 1.086*(errmean/med)
                
                p = med/(scale**2)


                expband = line1.split()[1]
                det1 = Detections(0,-99,-1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,-123,0)
                det1.sb= sb_med_circle
                det1.sb_err = sn_g1
                det1.rej = reject[i]
                det1.band = line1.split()[1]
                det1.nite = int(line2.split()[1])
                det1.expnum = exp
                det1.season = season
                det1.rad = z_radcut
                det1.truemag = 0.
                det1.trueflux = 0.
                det1._id = ID[i]
                det1.ra = RA[i]
                det1.dec = DEC[i]
                if FLUX[i]>0:
                    det1.realmag = filterObj_zp - 2.5*np.log10(FLUX[i])
                    det1.realmagerr = 2.5*np.log10(1+(FLUXERR[i]/FLUX[i]))
                else:
                    det1.realmag = 30.
                    det1.realmagerr = 30. 
                det1.mag = MAG[i]+filterObj_zp
                det1.magerr = MAGERR[i]
                det1.flux = FLUX[i]
                det1.fluxerr = FLUXERR[i]
                det1.ccdnum = CCDNUM2[i]
                det1.fakeid = SN_FAKEID[i] 
                det1.ml_score = -0.1
                detections.append(det1)
                #print "3b :", timeit.default_timer() - start_time 
          
        fits1.close()
        if (platform.node()=='des41.fnal.gov')==False:
            os.remove(imagefil)
        #print 'a =',a
        print 'number of non-reject fakes',number2+':',d  
        
        ind = 0    
        for detection in detections:
            if detections[ind].ccdnum==index:
                rows.append(Detections.getattributes(detection)) 
            ind=ind+1
        
        #print len(rows), index, len(ID) 
        newrows = np.array(rows)
        
        tbhdu1 = fits.BinTableHDU.from_columns(
        [fits.Column(name='ID', format='K', array=newrows[:,0].astype(float)),
        fits.Column(name='candidate_ID', format='K', array=newrows[:,1].astype(float)),
        fits.Column(name='reject', format='K', array=newrows[:,2].astype(float)),
        fits.Column(name='RA', format='E', array=newrows[:,3].astype(float)),
        fits.Column(name='DEC', format='E', array=newrows[:,4].astype(float)),
        fits.Column(name='radius', format='E', array=newrows[:,5].astype(float)),
        fits.Column(name='band', format='1A', array=newrows[:,6]),
        fits.Column(name='realmag', format='E', array=newrows[:,7].astype(float)),
        fits.Column(name='realmag_err', format='E', array=newrows[:,8].astype(float)),        
        fits.Column(name='mag', format='E', array=newrows[:,9].astype(float)),
        fits.Column(name='mag_err', format='E', array=newrows[:,10].astype(float)),
        fits.Column(name='flux', format='E', array=newrows[:,11].astype(float)),
        fits.Column(name='flux_err', format='E', array=newrows[:,12].astype(float)),
        fits.Column(name='true_mag', format='E', array=newrows[:,13].astype(float)),
        fits.Column(name='true_flux', format='E', array=newrows[:,14].astype(float)),
        fits.Column(name='NITE', format='K', array=newrows[:,15].astype(float)),
        fits.Column(name='EXPNUM', format='K', array=newrows[:,16].astype(float)),
        fits.Column(name='CCDNUM', format='K', array=newrows[:,17].astype(float)),
    #    fits.Column(name='CHIP', format='K', array=chips),
        fits.Column(name='SEASON', format='K', array=newrows[:,18].astype(float)),
        fits.Column(name='SB', format='E', array=newrows[:,19].astype(float)),
        fits.Column(name='SB_err', format='E', array=newrows[:,20].astype(float)),
        fits.Column(name='ML_score',format='E', array=newrows[:,21].astype(float)),
        fits.Column(name='fakeID', format='K', array=newrows[:,22].astype(float))])
        
        final1 = outdir
        final2 = '.fits'
        final = final1 + str(exp) + '_' + str(index) + final2
        tbhdu1.writeto(final)
        
        newrows = []
        rows = []
                
        global elapsed0
        elapsed1 = timeit.default_timer() - start_time - elapsed0
        elapsed0 = timeit.default_timer() - start_time
        
        m, s = divmod(elapsed0, 60)
        h, m = divmod(m, 60)
        
        print 'time elapsed:',round(elapsed1,2),'seconds for chip',number2
        print '             ',"%d:%02d:%02d" % (h, m, s), 'overall'
        
        return expband

areas = []
numcands = []
candidatelist = cand_list

if cand_list!='none':
    SNID,candRA,candDEC = np.genfromtxt(candidatelist, delimiter=',',skip_header=1,\
        usecols=(0,1,2),unpack=True)

now = datetime.datetime.now()

if fake_list!='none':
    table1 = fake_list

    FAKEID,EXPNUM,CCDNUM1,TRUEMAG,TRUEFLUX,FLUXCOUNT,NITE,MJD1 = \
        np.genfromtxt(table1, delimiter=' ',\
        skip_header=1,usecols=(0,1,2,3,4,5,7,8),unpack=True)

    BAND = np.genfromtxt(table1,dtype = 'string',delimiter=' ',
        skip_header=1,usecols=(6,),unpack=True)
        
    MJD1 = np.around(MJD1,decimals=3)
                                                

hourminute = "%02d%02d" %(now.hour,now.minute)
logfile = outdir+'sblog_'+str(datetime.datetime.now().date())+'_'+hourminute+'.txt'    
logf = open(logfile,'w+')
logf.write('Exposures processed starting at ')
logf.write(now.strftime("%I:%M%p on %B %d, %Y"))
logf.write(':\n')
for ie in range(len(expnums)):
    if ie!=len(expnums)-1:
        logf.write(str(expnums[ie]))
        logf.write(', ')
    else:
        logf.write(str(expnums[ie]))
        logf.write('\n')

for exp in expnums:

    expband = ''

    fakelist = []
    sb = []
    sb_err = []
    std = []
    rad = []
    err = []      
    
    class Detections:
        det_count = 0
        
        def __init__(self,_id,snid,rej,ra,dec,rad,band,realmag,realmagerr,mag,magerr,flux,fluxerr,truemag,\
            trueflux,nite,expnum,ccdnum,season,sb,sb_err,ml_score,fakeid):
            self._id = _id #ID[i]
            self.snid = snid #candidate id
            self.rej = rej #if nonzero, detection is a reject
            self.ra = ra #RA[i]
            self.dec = dec #DEC[i]
            self.rad = rad #
            self.band = band
            self.realmag = realmag
            self.realmagerr = realmagerr
            self.mag = mag
            self.magerr = magerr
            self.flux = flux
            self.fluxerr = fluxerr
            self.truemag = truemag
            self.trueflux = trueflux
            self.nite = nite
            self.expnum = expnum
            self.ccdnum = ccdnum
            self.season = season
            self.sb = sb
            self.sb_err = sb_err
            self.ml_score = ml_score
            self.fakeid = fakeid
            Detections.det_count += 1
            
        def getattributes(self):
            newarray = [self._id,self.snid,self.rej,self.ra,self.dec,self.rad,self.band,\
                self.realmag,self.realmagerr,self.mag,self.magerr,self.flux,self.fluxerr,\
                self.truemag,self.trueflux,self.nite,self.expnum,self.ccdnum,\
                self.season,self.sb,self.sb_err,self.ml_score,self.fakeid]
            return newarray        
      
    f = 0
    g = 0
    
    detections = []   
    rows = []   
    
    func = Parallel(n_jobs=cpus)(delayed(bigloop)(index) for index in range(first,last+1))       
    func
    
    for fval in range(len(func)):
        if func[fval]!=None:
            expband = func[fval]
            break
    #print 'expband',expband
    
    tnames = outdir+str(exp)+'_*.fits'
    tglob = glob(tnames)
    print ' '
    logf.write('\n')
    print '----- exposure',str(exp),'-----\n'
    logf.write('----- exposure '),logf.write(str(exp)),logf.write(' -----\n')
    print "band:",expband
    logf.write('band: '), logf.write(str(expband)),logf.write('\n')
    print "CCDs processed:",len(tglob)
    logf.write("CCDs processed: "),logf.write(str(len(tglob))),logf.write('\n')
    if len(tglob)==0:
        print ' '
        print 'NO DATA'
        print ' '
        print '---------------------------'
        logf.write('\n'),logf.write('NO DATA\n')
        logf.write('---------------------------\n')
        continue
    if len(tglob)==1:
        filenamenew = outdir + 'final_' + str(exp) + '.fits'
        os.rename(tglob[0],filenamenew)
        
    elif len(tglob)==2:
        t1 = fits.open(tglob[0])
        t2 = fits.open(tglob[1])
        
        nrows1 = t1[1].data.shape[0]
        nrows2 = t2[1].data.shape[0]
        nrows = nrows1 + nrows2
        hdu = fits.BinTableHDU.from_columns(t1[1].columns, nrows=nrows)
        
        for colname in t1[1].columns.names:
            hdu.data[colname][nrows1:] = t2[1].data[colname]
            
        filenamenew = outdir + str(exp)+ '_final' + '.fits'
        
        hdu.writeto(filenamenew, clobber=True)

        t1.close()
        t2.close()
        os.remove(tglob[0])
        os.remove(tglob[1])        
        
    else:    
        t1 = fits.open(tglob[0])
        t2 = fits.open(tglob[1])
        
        nrows1 = t1[1].data.shape[0]
        nrows2 = t2[1].data.shape[0]
        nrows = nrows1 + nrows2
        hdu = fits.BinTableHDU.from_columns(t1[1].columns, nrows=nrows)
        
        for colname in t1[1].columns.names:
            hdu.data[colname][nrows1:] = t2[1].data[colname]
        
        hdu.writeto(outdir + str(exp)+'_final_0.fits', clobber=True)  
        
        a,t=0,0
        #print "*",round((timeit.default_timer()-start_time),3)
    
        for tab in range(len(tglob)):
            t = tab+2
            if t>(len(tglob)-1):
                break
            t1 = fits.open(str(exp)+ '_final_' + str(a) + '.fits')
            t2 = fits.open(tglob[t])
            nrows1 = t1[1].data.shape[0]
            nrows2 = t2[1].data.shape[0]
            nrows = nrows1 + nrows2
            hdu = fits.BinTableHDU.from_columns(t1[1].columns, nrows=nrows)
            for colname1 in t1[1].columns.names:
                hdu.data[colname1][nrows1:] = t2[1].data[colname1]
            a = a+1
            filename = outdir + str(exp)+ '_final_' + str(a) + '.fits'
            #print "1", filename
            hdu.writeto(filename, clobber=True) 
            #print "2", filename
            t1.close()
            t2.close()
        
        filenamenew = outdir + 'final_' + str(exp) + '.fits'
        #print "3", filename 
        os.rename(filename,filenamenew)
    
        for aa in range(a):
            old = outdir + str(exp)+ '_final_' + str(aa) + '.fits'
            #print old
            os.remove(old) 
    
    if len(tglob)!=2:
        oldglob=glob(outdir+str(exp)+'_*.fits')
        
        for old2 in oldglob:
            #print old2
            os.remove(old2)
    
    #print "**",round((timeit.default_timer()-start_time),3)
    
    pixtotal = 0 
        
    for index2 in range(first,last+1):
        numberpix = str("%02d" % (index2))
        part1c = rootdir + '2*/'
        part2c = '/dp' + str(season) + '/*'
        part3c = '/*_combined_*'
        part4c = '+fakeSN_diff.fits'
        diffimage = part1c + str(exp) + part2c + numberpix + part3c + numberpix + part4c
        diffglob = glob(diffimage)  
        if len(diffglob)>0:
            if (platform.node()=='des41.fnal.gov')==True:
                diffimagefil = diffglob[0]
            else:
                shutil.copy(diffglob[0],".")
                diffimagefil = diffglob[0].split('/')[10]
            
            difffits = fits.open(diffimagefil)
            
            diffdata = difffits[0].data
            diffdata = diffdata - (1e-30)
            
            nonzero = np.count_nonzero(diffdata)
            #print nonzero, index2
            
            pixtotal = pixtotal + nonzero
            difffits.close()
            if (platform.node()=='des41.fnal.gov')==False:
                os.remove(diffglob[0].split('/')[10])
            #print pixtotal, timeit.default_timer()-start_time
    #print "***",round((timeit.default_timer()-start_time),3)        
    pixarea = float(pixtotal)*((scale/3600.)**2.)
    print ' '
    logf.write('\n')

    print 'total area for exposure =',round(pixarea,4),'square degrees'
    logf.write('total area for exposure = ')
    logf.write(str(round(pixarea,4))),logf.write(' square degrees\n')
    
    if cand_list!='none':        
        newfile = fits.open(filenamenew)
        tbdata = newfile[1].data
        candidates = tbdata.field(1)
        
        counter = 0
        
        for candidate in range(len(candidates)):
            if candidates[candidate]>0:
                counter = counter + 1
        #print "****",round((timeit.default_timer()-start_time),3)
        print 'total good candidates (real) =',counter
        logf.write('total good candidates (real) = '),logf.write(str(counter))
        logf.write('\n')
        
        areas.append(pixarea)
        numcands.append(float(counter))    
        goodrate = float(counter)/pixarea
        
        print 'good candidates (real) per square degree =',round(goodrate,1)
        logf.write('good candidates (real) per square degree = ')
        logf.write(str(round(goodrate,1))),logf.write('\n')
        newfile.close()
    
    else:
        newfile = fits.open(filenamenew)
        tbdata = newfile[1].data
        rejz = tbdata.field(2)
        fidz = tbdata.field(22)
        
        counter = 0
        
        for candidate in range(len(rejz)):
            if rejz[candidate]==0 and fidz[candidate]==0:
                counter = counter + 1
        #print "****",round((timeit.default_timer()-start_time),3)
        print 'total accepted detections (real) =',counter
        logf.write('total accepted detections (real) = '),logf.write(str(counter))
        logf.write('\n')
        
        areas.append(pixarea)
        numcands.append(float(counter))
        goodrate = float(counter)/pixarea
        
        print 'accepted detections (real) per square degree =',round(goodrate,1)
        logf.write('accepted detections (real) per square degree = ')
        logf.write(str(round(goodrate,1))),logf.write('\n')
        newfile.close()
       
    elapsed01 = timeit.default_timer() - start_time - elapsed00
    elapsed00 = timeit.default_timer() - start_time
    
    m0, s0 = divmod(elapsed01, 60)
    h0, m0 = divmod(m0, 60)
    
    print ' '
    logf.write('\n')
    print "%d:%02d:%02d" % (h0, m0, s0),'elapsed'
    logf.write("%d:%02d:%02d" % (h0, m0, s0)),logf.write(' elapsed\n')
    print '---------------------------'
    logf.write('---------------------------\n')
        
overallarea = np.sum(areas)
overallcands = np.sum(numcands)
overallrate = overallcands/overallarea
elapsed = timeit.default_timer() - start_time
mt, st = divmod(elapsed, 60)
ht, mt = divmod(mt, 60)
print ' ' 
logf.write('\n')
logf.write('*****\n')
logf.write('total area: '),logf.write(str(round(overallarea,3))),logf.write(' square degrees')
logf.write('\n')
if cand_list!='none':
    logf.write('total good candidates: '),logf.write(str(int(overallcands))),logf.write('\n')
else:
    logf.write('total accepted detections: '),logf.write(str(int(overallcands))),logf.write('\n')
logf.write('total rate: '),logf.write(str(round(overallrate,1))),logf.write(' per square degree')
logf.write('\n')
print 'time elapsed:',"%d:%02d:%02d" % (ht, mt, st), "total"
logf.write('total time elapsed: '),logf.write("%d:%02d:%02d" % (ht, mt, st))
now2 = datetime.datetime.now()
logf.write('\n')
logf.write("completed at "),logf.write(now2.strftime("%I:%M%p")),logf.write('\n')
logf.write("machine: "),logf.write(platform.node().split('.')[0]),logf.write('\n')
logf.write('*****\n')
print '---'
logf.write('\n'),logf.write('END')

logf.close() 
