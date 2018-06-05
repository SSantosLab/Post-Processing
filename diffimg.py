import numpy as np
import glob

def mag_limit(exptime=90.0,teff=1.0,sigma=10,band='i'):
	zp = 22.5
	if band == 'z': zp = 21.8
	return zp + 1.25 * np.log10(teff * exptime/90.0) + 2.5 * np.log10(10/sigma)

def mag(flux):
	f = flux
	f[f<=0] = 10**-9
	return 27.5 - 2.5 * np.log10(f)

def mag1(flux):
	f = max(flux,10**-9)
	return 27.5 - 2.5 * np.log10(f)

def magerr(flux,fluxerr):
	snr = flux/fluxerr
	return 2.5 * np.log10(1+1./snr)

def absMag(mag,dist_in_Mpc):
	return mag + 5 - 5 * np.log10(float(dist_in_Mpc)*10**6)

def decay_time(mjd_mag_list,dmag=2.,dt=1.0,mag_lim=24.0):
	m = sorted(mjd_mag_list,key=lambda mjd_mag_list: mjd_mag_list[0])
	rate = 0. # (mag/day rate)
	if len(m) == 0 : return -999.
	if len(m) == 1 : rate = (mag_lim  - m[0][1])/dt 
	if len(m) >  1 : rate = (m[-1][1] - m[0][1])/(m[-1][0]-m[0][0])
	rate = max(rate,0.001)
	return  np.absolute(dmag/rate)  

def is_decaying(f1,f2,f1err,f2err,sig):
	if (f2-f1) < (-1.0 * sig * np.sqrt(f1err**2+f2err**2)) :
		return True
	else:
		return False

def read_fakes_input(filename):
	f_fakeid = np.zeros(0,dtype=int)
	f_expnum = np.zeros(0,dtype=int)
	f_ccdnum = np.zeros(0,dtype=int)
	f_truemag = np.zeros(0,dtype=float)
	f_trueflux = np.zeros(0,dtype=float)
	f_flux = np.zeros(0,dtype=float)
	f_band = np.zeros(0,dtype=str)
	f_nite = np.zeros(0,dtype=int)
	f_mjd = np.zeros(0,dtype=float)
	with open(filename,"r") as f:
		n=0;	nskip=1
		for line in f:
			values = line.split() ; n+=1
			if n>nskip:
				f_fakeid = np.append(f_fakeid,int(values[0]))
				f_expnum = np.append(f_expnum,int(values[1]))
				f_ccdnum = np.append(f_ccdnum,int(values[2]))
				f_truemag = np.append(f_truemag,float(values[3]))
				f_trueflux = np.append(f_trueflux,float(values[4]))
				f_flux = np.append(f_flux,float(values[5]))
				f_band = np.append(f_band,values[6])
				f_nite = np.append(f_nite,int(values[7]))
				f_mjd = np.append(f_mjd,float(values[8]))
	return f_fakeid,f_expnum,f_ccdnum,f_truemag,f_trueflux,f_flux,f_band,f_nite,f_mjd 

def read_data_files(path='.',filenamepattern='*.dat'):

        # Generic top level info  
        FAKE = np.zeros(0,dtype=int) # 0 = not a fake
        RA = np.zeros(0,dtype=float)
        DEC = np.zeros(0,dtype=float)

        # DES info section 
        SNID = np.zeros(0,dtype=int)
        CCDNUM = np.zeros(0,dtype=int)
        ANGSEP = np.zeros(0,dtype=float) # arcsec
	NumEpochs = np.zeros(0,dtype=int)
	CandType = np.zeros(0,dtype=int)
        NumEpochsml = np.zeros(0,dtype=int)
	LatestNiteml = np.zeros(0,dtype=int)
        # FAKE info (not present if candidate is real)
        FAKEID = np.zeros(0,dtype=int)
        FAKEGALID = np.zeros(0,dtype=int)
        FAKERA = np.zeros(0,dtype=float)
        FAKEDEC = np.zeros(0,dtype=float)
        FAKEANGSEP = np.zeros(0,dtype=float) 
        FAKEZ = np.zeros(0,dtype=float)       # redshift
        FAKEPEAKMJD = np.zeros(0,dtype=float)
        FAKEHOSTSEP = np.zeros(0,dtype=float) # arcsec
    
        # Light curve data section
        MJD = np.zeros(0,dtype=float)
        BAND = np.zeros(0,dtype=str)
        FIELD = np.zeros(0,dtype=str)
        FLUXCAL = np.zeros(0,dtype=float)
        FLUXCALERR = np.zeros(0,dtype=float)
        PHOTFLAG = np.zeros(0,dtype=int)      # >= 4096 is good
        PHOTPROB = np.zeros(0,dtype=float)    # ML score
        ZPFLUX = np.zeros(0,dtype=float)
        PSF = np.zeros(0,dtype=float)
        SKYSIG = np.zeros(0,dtype=float)
        SKYSIGT = np.zeros(0,dtype=float)
        GAIN = np.zeros(0,dtype=float) 
        XPIX = np.zeros(0,dtype=float)
        YPIX = np.zeros(0,dtype=float)   
        SIMMAG = np.zeros(0,dtype=float) # this coll exists only on files for fakes
        OBSNITE = np.zeros(0,dtype=int)
	OBJID = np.zeros(0,dtype=int)
	EXPNUM  = np.zeros(0,dtype=int)
	OBSCCDNUM  = np.zeros(0,dtype=int)
	MASKFRAC = np.zeros(0,dtype=float)

        for filename in glob.glob(path+'/'+filenamepattern):
            print filename
            thisFAKE = -9
            thisRA = -999.0
            thisDEC = -999.0
            thisSNID = 0
            thisCCDNUM = 0
            thisANGSEP = -999.0
            thisFAKEID = 0
            thisFAKEGALID = 0
            thisFAKERA = -999.0
            thisFAKEDEC = -999.0
            thisFAKEANGSEP = -999.0
            thisFAKEZ = -999.0
            thisFAKEPEAKMJD = -999.0
            thisFAKEHOSTSEP = -999.0
	    thisCandType = -9
            thisNumEpochs = -99
            thisNumEpochsml= -99
            NOBS = 0
	    thisOBSNITE = -999
	    thisOBJID = -999
	    thisEXPNUM  = -999
	    thisOBSCCDNUM  = -999
	    thisMASKFRAC = -100
            is_data_section = False
            f = open(filename,"r")
            while not is_data_section:
                line = f.readline().split()
                if len(line) > 0:
                    if line[0] == 'OBS:': is_data_section = True
                    if line[0] == 'FAKE:': thisFAKE = int(line[1])
                    if line[0] == 'RA:': thisRA = float(line[1])
                    if line[0] == 'DECL:': thisDEC = float(line[1])
                    if line[0] == 'PRIVATE(DES_snid):': thisSNID = int(line[1])
		    if line[0] == 'PRIVATE(DES_cand_type):': thisCandType = int(line[1])
                    if line[0] == 'PRIVATE(DES_ccdnum):': thisCCDNUM = int(line[1])
		    if line[0] == 'PRIVATE(DES_numepochs):': thisNumEpochs = int(line[1])
		    if line[0] == 'PRIVATE(DES_numepochs_ml):': thisNumEpochsml = int(line[1])
                    if line[0] == 'PRIVATE(DES_angsep_trigger):': thisANGSEP = float(line[1])
		    if line[0] == 'PRIVATE(DES_latest_nite_ml):': thisLatestNiteml = int(line[1])
		    if line[0] == 'PRIVATE(DES_fake_id):': thisFAKEID = int(line[1])
                    if line[0] == 'PRIVATE(DES_fake_galid):': thisFAKEGALID = int(line[1])
                    if line[0] == 'PRIVATE(DES_fake_ra)': thisFAKERA = float(line[1])
                    if line[0] == 'PRIVATE(DES_fake_dec):': thisFAKEDEC = float(line[1])
                    if line[0] == 'PRIVATE(DES_fake_angsep):': thisFAKEANGSEP = float(line[1])
                    if line[0] == 'PRIVATE(DES_fake_z):': thisFAKEZ = float(line[1])
                    if line[0] == 'PRIVATE(DES_fake_peakmjd):': thisFAKEPEAKMJD = float(line[1])
                    if line[0] == 'PRIVATE(DES_fake_hostsep):': thisFAKEHOSTSEP = float(line[1])
                    if line[0] == 'NOBS:': NOBS = int(line[1])        
            FAKE = np.append(FAKE,np.full(NOBS,thisFAKE))
            RA = np.append(RA,np.full(NOBS,thisRA))
            DEC = np.append(DEC,np.full(NOBS,thisDEC))
            SNID = np.append(SNID,np.full(NOBS,thisSNID))
	    CandType = np.append(CandType,np.full(NOBS,thisCandType))
            CCDNUM = np.append(CCDNUM,np.full(NOBS,thisCCDNUM))
	    NumEpochs = np.append(NumEpochs,np.full(NOBS,thisNumEpochs))
            NumEpochsml = np.append(NumEpochsml,np.full(NOBS,thisNumEpochsml))
	    LatestNiteml = np.append(LatestNiteml,np.full(NOBS,thisLatestNiteml))
	    FAKEID = np.append(FAKEID,np.full(NOBS,thisFAKEID))
            FAKEGALID = np.append(FAKEGALID,np.full(NOBS,thisFAKEGALID))
            FAKERA = np.append(FAKERA,np.full(NOBS,thisFAKERA))
            FAKEDEC = np.append(FAKEDEC,np.full(NOBS,thisFAKEDEC))
            FAKEANGSEP = np.append(FAKEANGSEP,np.full(NOBS,thisFAKEANGSEP))
            FAKEZ = np.append(FAKEZ,np.full(NOBS,thisFAKEZ))
            FAKEPEAKMJD = np.append(FAKEPEAKMJD,np.full(NOBS,thisFAKEPEAKMJD))
            FAKEHOSTSEP = np.append(FAKEHOSTSEP,np.full(NOBS,thisFAKEHOSTSEP))
	    print line 
	    while is_data_section:
                MJD = np.append(MJD,float(line[1]))
                BAND = np.append(BAND,line[2])
                FIELD = np.append(FIELD,line[3])
                FLUXCAL = np.append(FLUXCAL,float(line[4]))
                FLUXCALERR = np.append(FLUXCALERR,float(line[5]))
                PHOTFLAG = np.append(PHOTFLAG,int(line[6]))
                PHOTPROB = np.append(PHOTPROB,float(line[7]))
                ZPFLUX = np.append(ZPFLUX,float(line[8]))
                PSF = np.append(PSF,float(line[9]))
                SKYSIG = np.append(SKYSIG,float(line[10]))
                SKYSIGT = np.append(SKYSIGT,float(line[11]))
                GAIN = np.append(GAIN,float(line[12]))
                XPIX = np.append(XPIX,float(line[13]))
                YPIX = np.append(YPIX,float(line[14]))
		OBSNITE = np.append(OBSNITE,float(line[15]))
		EXPNUM = np.append(EXPNUM,float(line[16]))
		OBSCCDNUM = np.append(OBSCCDNUM,float(line[17]))
		OBJID = np.append(OBJID,int(line[18]))
		if thisFAKE == 1 : 
			SIMMAG = np.append(SIMMAG,float(line[19]))
			MASKFRAC = np.append(MASKFRAC,float(line[20]))
		else:
			SIMMAG = np.append(SIMMAG,0.0)
			MASKFRAC = np.append(MASKFRAC, 0.0)
#                NITE = np.append(NITE,int(line[-1]))
                line = f.readline().split()
		if len(line) == 0: 
                    is_data_section = False
                elif line[0] != 'OBS:': 
                    is_data_section = False
            f.close()

	MAG = mag(FLUXCAL)
	MAGERR = magerr(FLUXCAL,FLUXCALERR)

        return FAKE,RA,DEC,SNID,CandType,CCDNUM,NumEpochs,NumEpochsml,LatestNiteml,FAKEID,\
            FAKEGALID,FAKERA,FAKEDEC,FAKEANGSEP,FAKEZ,\
            FAKEPEAKMJD,FAKEHOSTSEP,MJD,BAND,FIELD,FLUXCAL,\
            FLUXCALERR,PHOTFLAG,PHOTPROB,ZPFLUX,PSF,SKYSIG,\
            SKYSIGT,GAIN,XPIX,YPIX,OBSNITE,EXPNUM,OBSCCDNUM,\
	    OBJID,SIMMAG,MASKFRAC,MAG,MAGERR

class DataSet:

	def __init__(self,path='.',filenamepattern='*.dat',label='sample'): 
		
		self.data = np.core.records.fromarrays(read_data_files(path,filenamepattern),names="FAKE,RA,DEC,SNID,CandType,CCDNUM,NumEpochs,NumEpochsml,LatestNiteml,FAKEID,FAKEGALID,FAKERA,FAKEDEC,FAKEANGSEP,FAKEZ,FAKEPEAKMJD,FAKEHOSTSEP,MJD,BAND,FIELD,FLUXCAL,FLUXCALERR,PHOTFLAG,PHOTPROB,ZPFLUX,PSF,SKYSIG,SKYSIGT,GAIN,XPIX,YPIX,OBSNITE,EXPNUM,OBSCCDNUM,OBJID,SIMMAG,MASKFRAC,MAG,MAGERR")

		self.label = label
			
	def get_fakes_input(self,filename,get_all=True,good_expnum_list=np.array([475986,476340,476341,476342,476343,476344,476345,476346,476348,476350,476351,476353,482881,482882,482886,482888,482891,482892,482894,482897,482901,482902,482903,476978,476979,476981,476983,476991,477002,477004,477011,476347,476349,476352,475905,475906,475907,475911,475912,475913,475917,475918,475919,475920,475921,475922,475923,475924,475925,475926,475927,475928,475929,475930,475931,475932,475933,475934,475935,475936,475937,475941,475942,475943,475950,475951,475952,475953,475954,475955,475959,475960,475961,482865,482866,482867,482868,482869,482870,482871,482872,482873,482874,482875,482876,482877,482878,482879,482880,482883,482884,482885,482887,482889,482890,482893,482895,482896,482898,482899,482900,482904,482905,482906,482907,482908,482909,482910,482911,482912,482913,482914,482915,482916,482917,482918,482919,482920,482921,482922,482923,482924,476967,476969,476972,476973,476974,476975,476977,476980,477014,477022,477023,477024,476966,476968,476970,476971,476976,476982,476984,477005,476985,476986,476987,476992,476988,476989,476990,476993,476998,476994,476995,476996,477001,477006,476997,476999,477018,477000,477003,477007,477008,477010,477009,477012,477013,477019,477015,477016,477017,477020,477021,477025])):

          # note: to create this file I ran a query using easyaccess
          # select snfake_id,expnum,ccdnum,truemag,truefluxcnt,fluxcnt,band,nite,mjd from snfakeimg where season=106 ;
		self.fakes_input = np.core.records.fromarrays(read_fakes_input(filename),names="SNFAKE_ID,EXPNUM,CCDNUM,TRUEMAG,TRUEFLUXCNT,FLUXCNT,BAND,NITE,MJD")

		if not get_all:
			 self.fakes_input = self.fakes_input[np.in1d(self.fakes_input.EXPNUM,good_expnum_list)]

	# def set_mask_old(self,PHOTFLAG_min=0.,require_i=False,require_z=False,require_posflux=False,mask_list=[['none',0.,0.],]):
	# 	self.QC_mask  = (self.data.PHOTPROB >= PHOTPROB_min)
	# 	self.QC_mask &= (self.data.PHOTFLAG == 4096)
	# 	self.OBSI_mask = self.data.BAND == 'i'
	# 	self.OBSZ_mask = self.data.BAND == 'z'
	# 	self.NONZEROFLUX_mask = (self.data.FLUXCAL + self.data.FLUXCALERR) > 0
	# 	mask = self.QC_mask
	# 	if require_i : mask &= self.OBSI_mask
	# 	if require_z : mask &= self.OBSZ_mask
	# 	if require_posflux : mask &= self.NONZEROFLUX_mask		
	# 	for item in mask_list:
	# 		if not item[0] == 'none' :
	# 			mask &= (self.data[item[0]] >= item[1]) & (self.data[item[0]] <= item[2])
	# 	return self.data[mask]

	def set_mask(self,PHOTFLAG_bit=0,require_i=False,require_z=False,require_posflux=False,mask_list=[['none',0.,0.],]):
		self.QC_mask  = (self.data.PHOTFLAG & 1016 ==0)
		self.QC_mask &= (self.data.PHOTFLAG & PHOTFLAG_bit == PHOTFLAG_bit)
		self.OBSI_mask = self.data.BAND == 'i'
		self.OBSZ_mask = self.data.BAND == 'z'
		self.NONZEROFLUX_mask = (self.data.FLUXCAL + self.data.FLUXCALERR) > 0
		mask = self.QC_mask
		if require_i : mask &= self.OBSI_mask
		if require_z : mask &= self.OBSZ_mask
		if require_posflux : mask &= self.NONZEROFLUX_mask		
		for item in mask_list:
			if not item[0] == 'none' :
				mask &= (self.data[item[0]] >= item[1]) & (self.data[item[0]] <= item[2])
		return self.data[mask]

class LightCurve:

	def __init__(self,times,idata,zdata,label):

		self.data  = np.core.records.fromarrays([times,idata,zdata],names="TIME,IMAG,ZMAG")
		self.label = label

	def peak(self):

		self.peakIMAG = 99.
		self.peakITIME = -99.
		self.peakZMAG = 99.
		self.peakZTIME = -99.

		if len(self.data.IMAG) > 0:
			self.peakIMAG = min(self.data.IMAG)
			self.peakITIME = self.data.TIME[np.where(self.data.IMAG == self.peakIMAG)][0]

		if len(self.data.ZMAG) > 0:
			self.peakZMAG = min(self.data.ZMAG)
			self.peakZTIME = self.data.TIME[np.where(self.data.ZMAG == self.peakZMAG)][0]

		return self.peakITIME, self.peakIMAG, self.peakZTIME, self.peakZMAG 
		
	def decayTime(self):
		
		dmag = 2.0

		self.decayITIME = -99.
		self.decayZTIME = -99.

		ti, peaki, tz, peakz = self.peak()

 		if (ti > 0):
			mask  = self.data.IMAG > (peaki+dmag)
			mask &= self.data.TIME > ti
			t_arr = self.data.TIME[mask]
			if len(t_arr)>0 : 
				self.decayITIME = t_arr[0]
			else:
				self.decayITIME = decay_time(zip(self.data.TIME,self.data.IMAG))

		if (tz > 0):
			mask  = self.data.ZMAG > (peakz+dmag)
			mask &= self.data.TIME > tz
			t_arr = self.data.TIME[mask]
			if len(t_arr)>0 : 
				self.decayZTIME = t_arr[0]
			else:
				self.decayZTIME = decay_time(zip(self.data.TIME,self.data.ZMAG))

		return self.decayITIME, self.decayZTIME

# decay_time(mjd_mag_list,dmag=2.,dtmin=1.0,mag_lim=24.0,magerr=0.02):

##########


class Candidate:
	
	# initiate with an empty records array
	pool = np.recarray((0,),dtype=[('id',float),('ra',float),('dec',float),('peakmag_i',float),('magerrp_i',float),('simmagp_i',float),('peakmag_z',float),('magerrp_z',float),('simmagp_z',float),('absmag_near_i',float),('absmag_far_i',float),('nepochs_i',float),('dmag12_i',float),('dmag23_i',float),('dt_i',float),('nsiglast_i',float),('absmag_near_z',float),('absmag_far_z',float),('nepochs_z',float),('dmag12_z',float),('dmag23_z',float),('dt_z',float),('nsiglast_z',float),('color',float),('max_stage',float)])

	fake_mask = np.zeros(0,dtype=bool)

	def __init__(self,cand_id=None,ra=None,dec=None,
		    peakmag_i=None,magerrp_i=None,simmagp_i=None,
		    peakmag_z=None,magerrp_z=None,simmagp_z=None,
		    absmag_near_i=None,absmag_far_i=None,nepochs_i=None,dmag12_i=None,dmag23_i=None,dt_i=None,nsiglast_i=None,
		    absmag_near_z=None,absmag_far_z=None,nepochs_z=None,dmag12_z=None,dmag23_z=None,dt_z=None,nsiglast_z=None,color=None,max_stage=None):
		
		# set the records array for this candidate instance
		self.record = np.core.records.fromrecords([(cand_id,ra,dec,
							    peakmag_i,magerrp_i,simmagp_i,
							    peakmag_z,magerrp_z,simmagp_z,
							    absmag_near_i,absmag_far_i,nepochs_i,dmag12_i,dmag23_i,dt_i,nsiglast_i,
							    absmag_near_z,absmag_far_z,nepochs_z,dmag12_z,dmag23_z,dt_z,nsiglast_z,color,max_stage)],
							   names = "id,ra,dec,peakmag_i,magerrp_i,simmagp_i,peakmag_z,magerrp_z,simmagp_z,absmag_near_i,absmag_far_i,nepochs_i,dmag12_i,dmag23_i,dt_i,nsiglast_i,absmag_near_z,absmag_far_z,nepochs_z,dmag12_z,dmag23_z,dt_z,nsiglast_z,color,max_stage")

			# update pool of candidates
		Candidate.pool = np.core.records.fromrecords(Candidate.pool.tolist() + self.record.tolist(),
							     names = "id,ra,dec,peakmag_i,magerrp_i,simmagp_i,peakmag_z,magerrp_z,simmagp_z,absmag_near_i,absmag_far_i,nepochs_i,dmag12_i,dmag23_i,dt_i,nsiglast_i,absmag_near_z,absmag_far_z,nepochs_z,dmag12_z,dmag23_z,dt_z,nsiglast_z,color,max_stage")			
			# by default, candidates are assumed to be real

		if len(Candidate.pool) > 0:		
			Candidate.fake_mask = np.append(Candidate.fake_mask,False)



	def set_fake_mask(self,index=-1):
		Candidate.fake_mask[index]=True
		return Candidate.fake_mask

	def is_elected(self,require_i=False,require_z=False,nepochs_min=0,require_decay=False,last_epoch_max_significance=0,require_color=False,color_min=-99.):

		is_ok = True
		
		if (require_i): 
			is_ok &= True if self.record.peakmag_i < 50           else False
			is_ok &= True if self.record.nepochs_i >= nepochs_min else False 
			if (require_decay): 
				is_ok &= True if self.record.dt_i < 100 else False
			if (last_epoch_max_significance>0):
				is_ok &= True if self.record.nsiglast_i < last_epoch_max_significance else False 
			
		if (require_z): 
			is_ok &= True if self.record.peakmag_z < 50           else False
			is_ok &= True if self.record.nepochs_z >= nepochs_min else False 
			if (require_decay): 
				is_ok &= True if self.record.dt_z < 100 else False
			if (last_epoch_max_significance>0):
				is_ok &= True if self.record.nsiglast_z < last_epoch_max_significance else False 
			
		if (require_color):
			is_ok &= True if self.record.color > color_min else False

		return is_ok

###

def generic_transients(n=100):
	ClassicalNovae = np.zeros((3*n,2),dtype=float)  
	CoreCollapse = np.zeros((n,2),dtype=float)
	Ia = np.zeros((3*n,2),dtype=float)
	LuminousSNe = np.zeros((n,2),dtype=float)
	LuminousRedNovae = np.zeros((n,2),dtype=float)
	dotIa = np.zeros((n,2),dtype=float)
	RelativisticExplosions = np.zeros((3*n,2),dtype=float)
#	IntermediateRedTrans = np.zeros((n,2),dtype=float)
#	CaRichTransients = np.zeros((n,2),dtype=float)

	x=10**(1.+np.random.standard_normal(n))
	y=(1.*x - 10)*np.random.standard_normal(n)
	ClassicalNovae = zip(x,y)

	for i in range(n):
#		ClassicalNovae[i][0]=10**np.random.uniform(np.log10(1.),np.log10(10.))
#		ClassicalNovae[i][1]=np.random.uniform(-8.,-10.)
#		ClassicalNovae[n+i][0]=10**np.random.uniform(np.log10(10.),np.log10(30.))
#		ClassicalNovae[n+i][1]=np.random.uniform(-6.5,-9)
#		ClassicalNovae[2*n+i][0]=10**np.random.uniform(np.log10(30.),np.log10(100.))
#		ClassicalNovae[2*n+i][1]=np.random.uniform(-6.,-7.5)

		CoreCollapse[i][0]=10**np.random.uniform(np.log10(20.),np.log10(300.))
		CoreCollapse[i][1]=np.random.uniform(-14,-21)
		Ia[i][0]=10**np.random.uniform(np.log10(30.),np.log10(40.))
		Ia[i][1]=np.random.uniform(-17,-18)
		Ia[n+i][0]=10**np.random.uniform(np.log10(40.),np.log10(50.))
		Ia[n+i][1]=np.random.uniform(-18,-19)
		Ia[2*n+i][0]=10**np.random.uniform(np.log10(50.),np.log10(60.))
		Ia[2*n+i][1]=np.random.uniform(-19,-20)
		LuminousSNe[i][0]=10**np.random.uniform(np.log10(50.),np.log10(400.))
		LuminousSNe[i][1]=np.random.uniform(-19,-23)
		LuminousRedNovae[i][0]=10**np.random.uniform(np.log10(20.),np.log10(60.))
		LuminousRedNovae[i][1]=np.random.uniform(-10,-14)
		dotIa[i][0]=10**np.random.uniform(np.log10(0.7),np.log10(20))
		dotIa[i][1]=np.random.uniform(-15,-17)
		RelativisticExplosions[i][0] = 10**np.random.uniform(np.log10(0.01),np.log10(0.08))
		RelativisticExplosions[i][1] = np.random.uniform(-28,-29)
		RelativisticExplosions[n+i][0] = 10**np.random.uniform(np.log10(0.08),np.log10(0.2))
		RelativisticExplosions[n+i][1] = np.random.uniform(-27,-28.5)
		RelativisticExplosions[2*n+i][0] = 10**np.random.uniform(np.log10(0.2),np.log10(0.6))
		RelativisticExplosions[2*n+i][1] = np.random.uniform(-26,-27.5)

	return ClassicalNovae,CoreCollapse,Ia,LuminousSNe,LuminousRedNovae,dotIa,RelativisticExplosions
	


##lst1, lst2 = zip(*zipped_list)			 
