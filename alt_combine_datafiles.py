import numpy as np
import sys
from astropy.io import fits
import timeit

start_time = timeit.default_timer()

path = './RealLCurves_01_30_17/'

listfile = path + 'RealLCurves_01_30_17.LIST'

ls = open(listfile,'r+')
dats = ls.readlines()
ls.close()

hostlist = []
c = 0

MJD,BAND,FIELD,FLUXCAL,FLUXCALERR,PHOTFLAG,PHOTPROB,ZPFLUX,PSF,SKYSIG,\
        SKYSIG_T,GAIN,XPIX,YPIX,NITE,EXPNUM,CCDNUM,OBJID = [],[],[],[],[],[],\
        [],[],[],[],[],[],[],[],[],[],[],[]
        
RA,DEC,CAND_ID,DATAFILE,SN_ID = [],[],[],[],[]
HOSTID,PHOTOZ,PHOTOZERR,SPECZ,SPECZERR,HOSTSEP,HOST_GMAG,HOST_RMAG,HOST_IMAG,\
    HOST_ZMAG = [],[],[],[],[],[],[],[],[],[]

c=0
allgood=0
for d in dats:
    c=c+1
    if c%1000==0:
        print c
        #break
    filename = d.split('\n')[0]
    datfile = path + filename
    f = open(datfile,'r+')
    lines = f.readlines()
    f.close()
    
    snid = lines[1].split()[1]
    raval = lines[8].split()[1]
    decval = lines[9].split()[1] 
    host_id = lines[15].split()[1]
    photo_z = lines[16].split()[1]
    photo_zerr = lines[16].split()[3]
    spec_z = lines[17].split()[1]
    spec_zerr = lines[17].split()[3] 
    host_sep = lines[18].split()[1]
    h_gmag = lines[19].split()[1]
    h_rmag = lines[19].split()[2]
    h_imag = lines[19].split()[3]
    h_zmag = lines[19].split()[4]
    
    mjd,band,field,fluxcal,fluxcalerr,photflag,photprob,zpflux,psf,skysig,\
        skysig_t,gain,xpix,ypix,nite,expnum,ccdnum,objid = np.genfromtxt(datfile,\
        skip_header=53,usecols=(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18),unpack=True)
        
    band = np.genfromtxt(datfile,dtype='string',skip_header=53,usecols=(2,),unpack=True)    
        
    if all(x==12288 for x in photflag):
        allgood=allgood+1
        
    n = len(mjd)
    
    ra = np.empty(n)
    ra.fill(raval)
    dec = np.empty(n)
    dec.fill(decval)
    cand = np.empty(n)
    cand.fill(c)
    sn_id = np.empty(n)
    sn_id.fill(snid)
    hostid = np.empty(n)
    hostid.fill(host_id)
    photoz=np.empty(n)
    photoz.fill(photo_z)
    photozerr=np.empty(n)
    photozerr.fill(photo_zerr)
    specz=np.empty(n)
    specz.fill(spec_z)
    speczerr=np.empty(n)
    speczerr.fill(spec_zerr)
    hostsep=np.empty(n)
    hostsep.fill(host_sep)
    hgmag=np.empty(n)
    hgmag.fill(h_gmag)
    hrmag=np.empty(n)
    hrmag.fill(h_rmag)
    himag=np.empty(n)
    himag.fill(h_imag)
    hzmag=np.empty(n)
    hzmag.fill(h_zmag)
    
    for j in range(n):
        DATAFILE.append(filename)
    
    for k in range(n):
        RA.append(ra[k])
        DEC.append(dec[k])
        CAND_ID.append(cand[k])
        SN_ID.append(sn_id[k])
        
        HOSTID.append(hostid[k])
        PHOTOZ.append(photoz[k])
        PHOTOZERR.append(photozerr[k])
        SPECZ.append(specz[k])
        SPECZERR.append(speczerr[k])
        HOSTSEP.append(hostsep[k])
        HOST_GMAG.append(hgmag[k])
        HOST_RMAG.append(hrmag[k])
        HOST_IMAG.append(himag[k])
        HOST_ZMAG.append(hzmag[k])
        
        MJD.append(mjd[k])
        BAND.append(band[k])
        FIELD.append(field[k])
        FLUXCAL.append(fluxcal[k])
        FLUXCALERR.append(fluxcalerr[k])
        PHOTFLAG.append(photflag[k])
        PHOTPROB.append(photprob[k])
        ZPFLUX.append(zpflux[k])
        PSF.append(psf[k])
        SKYSIG.append(skysig[k])
        SKYSIG_T.append(skysig_t[k])
        GAIN.append(gain[k])
        XPIX.append(xpix[k])
        YPIX.append(ypix[k])
        NITE.append(nite[k])
        EXPNUM.append(expnum[k])
        CCDNUM.append(ccdnum[k])
        OBJID.append(objid[k])
    
print len(RA)
print len(PHOTFLAG)

MJD,FIELD,FLUXCAL,FLUXCALERR,PHOTFLAG,PHOTPROB,ZPFLUX,PSF,SKYSIG,SKYSIG_T,\
    GAIN,XPIX,YPIX,NITE,EXPNUM,CCDNUM,OBJID,RA,DEC,CAND_ID,SN_ID = \
    np.asarray(MJD),np.asarray(FIELD),np.asarray(FLUXCAL),np.asarray(FLUXCALERR),\
    np.asarray(PHOTFLAG),np.asarray(PHOTPROB),np.asarray(ZPFLUX),np.asarray(PSF),\
    np.asarray(SKYSIG),np.asarray(SKYSIG_T),np.asarray(GAIN),np.asarray(XPIX),\
    np.asarray(YPIX),np.asarray(NITE),np.asarray(EXPNUM),np.asarray(CCDNUM),\
    np.asarray(OBJID),np.asarray(RA),np.asarray(DEC),np.asarray(CAND_ID),np.asarray(SN_ID)
    
HOSTID,PHOTOZ,PHOTOZERR,SPECZ,SPECZERR,HOSTSEP,HOST_GMAG,HOST_RMAG,HOST_IMAG,\
    HOST_ZMAG = np.asarray(HOSTID),np.asarray(PHOTOZ),np.asarray(PHOTOZERR),\
    np.asarray(SPECZ),np.asarray(SPECZERR),np.asarray(HOSTSEP),np.asarray(HOST_GMAG),\
    np.asarray(HOST_RMAG),np.asarray(HOST_IMAG),np.asarray(HOST_ZMAG)

tbhdu1 = fits.BinTableHDU.from_columns(
        [fits.Column(name='cand_ID', format='K', array=CAND_ID.astype(float)),
        fits.Column(name='SNID', format='K', array=SN_ID.astype(float)),
        fits.Column(name='RA', format='E', array=RA.astype(float)),
        fits.Column(name='DEC', format='E', array=DEC.astype(float)),
        fits.Column(name='MJD', format='E', array=MJD.astype(float)),
        fits.Column(name='BAND', format='1A', array=BAND),
        fits.Column(name='FIELD', format='K', array=RA.astype(float)),
        fits.Column(name='FLUXCAL', format='E', array=FLUXCAL.astype(float)),
        fits.Column(name='FLUXCALERR', format='E', array=FLUXCALERR.astype(float)),
        fits.Column(name='PHOTFLAG', format='K', array=PHOTFLAG.astype(float)),
        fits.Column(name='PHOTPROB', format='E', array=PHOTPROB.astype(float)),
        fits.Column(name='ZPFLUX', format='E', array=ZPFLUX.astype(float)),
        fits.Column(name='PSF', format='E', array=PSF.astype(float)),
        fits.Column(name='SKYSIG', format='E', array=SKYSIG.astype(float)),
        fits.Column(name='SKYSIG_T', format='E', array=SKYSIG_T.astype(float)),
        fits.Column(name='GAIN', format='E', array=GAIN.astype(float)),
        fits.Column(name='XPIX', format='E', array=XPIX.astype(float)),
        fits.Column(name='YPIX', format='E', array=YPIX.astype(float)),
        fits.Column(name='NITE', format='K', array=NITE.astype(float)),
        fits.Column(name='EXPNUM', format='K', array=EXPNUM.astype(float)),
        fits.Column(name='CCDNUM', format='K', array=CCDNUM.astype(float)),
        
        fits.Column(name='HOSTID', format='K', array=HOSTID.astype(float)),
        fits.Column(name='PHOTOZ', format='E', array=PHOTOZ.astype(float)),
        fits.Column(name='PHOTOZERR', format='E', array=PHOTOZERR.astype(float)),
        fits.Column(name='SPECZ', format='E', array=SPECZ.astype(float)),
        fits.Column(name='SPECZERR', format='E', array=SPECZERR.astype(float)),
        fits.Column(name='HOSTSEP', format='E', unit='arcsec', array=HOSTSEP.astype(float)),
        fits.Column(name='HOST_GMAG', format='E', array=HOST_GMAG.astype(float)),
        fits.Column(name='HOST_RMAG', format='E', array=HOST_RMAG.astype(float)),
        fits.Column(name='HOST_IMAG', format='E', array=HOST_IMAG.astype(float)),
        fits.Column(name='HOST_ZMAG', format='E', array=HOST_ZMAG.astype(float)),
        fits.Column(name='DATAFILE', format='21A', array=DATAFILE)])
        
tbhdu1.writeto('dattestloop_host.fits',clobber=True)
        
print "number of candidates where all detections had ml_score>0.5 :",allgood
print
time = timeit.default_timer() - start_time
print "time:",time