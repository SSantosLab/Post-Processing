import os, tarfile
import warnings
warnings.filterwarnings("ignore", category=RuntimeWarning)
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import pickle
from tqdm import tqdm
from glob import glob

plt.rcParams['mathtext.fontset'] = 'stix'
plt.rcParams['font.family'] = 'STIXGeneral'

base_dir = '/data/des70.a/data/desgw/O4/S250223dk/Post-Processing/'
proc_day = '250225'
snid_filename = 'snids_250226.list'

def flux2mag(flux):
    m =-2.5*np.log10(flux)+27.5
    return m

def fluxerr2magerr(flux, fluxerror):
    dmdflux = -2.5/(np.log(10)*flux)
    Error = np.abs((dmdflux)*fluxerror)
    return Error

def plot_lc(snid, ml, flt, mjd, mag, magerr, outpath, ml_cut=0.7, maglim=(25, 15)):
    fig, ax = plt.subplots(figsize=(10, 4))
    g_mask = (ml > ml_cut) & (flt == 'g')
    r_mask = (ml > ml_cut) & (flt == 'r')
    i_mask = (ml > ml_cut) & (flt == 'i')
    z_mask = (ml > ml_cut) & (flt == 'z')
    ax.errorbar(mjd[g_mask], mag[g_mask], yerr=magerr[g_mask], fmt='.', markersize=3, capsize=2, color='C0', label='g band')
    ax.errorbar(mjd[r_mask], mag[r_mask], yerr=magerr[r_mask], fmt='.', markersize=3, capsize=2, color='C1', label='r band')
    ax.errorbar(mjd[i_mask], mag[i_mask], yerr=magerr[i_mask], fmt='.', markersize=3, capsize=2, color='C2', label='i band')
    ax.errorbar(mjd[z_mask], mag[z_mask], yerr=magerr[z_mask], fmt='.', markersize=3, capsize=2, color='C3', label='z band')
    ax.axvline(60729.47430556, color='r', linewidth=1, label='Beginning of Night')
    ax.axvline(60730.47430556, color='r', linewidth=1)
    ax.set_ylim(maglim)
    ax.set_xlim((60729.4, 60731.4))
    ax.set_title('2025-02-23/24')
    ax.set_xlabel('MJD [days]')
    ax.set_ylabel('Transient Magnitude Over Baseline')
    ax.legend()
    fig.suptitle('S250223dk SNID: '+snid, fontsize=14)
    plt.savefig(outpath)
    plt.close()

with open(base_dir+'data_dict_{}.pickle'.format(proc_day), 'rb') as handle:
    data_dict = pickle.load(handle)

snid_array_precut = []
data_ra_precut = []
data_dec_precut = []
data_has_host_precut = []
for key in data_dict:
    snid_array_precut = np.append(snid_array_precut, key)
    data_ra_precut = np.append(data_ra_precut, data_dict[key]['RA'])
    data_dec_precut = np.append(data_dec_precut, data_dict[key]['DEC'])
    if len(data_dict[key]['HOSTGAL_ID'])>0:
        data_has_host_precut = np.append(data_has_host_precut, True)
    else:
        data_has_host_precut = np.append(data_has_host_precut, False)

snid_list = np.loadtxt(snid_filename, dtype='str')
for snid in tqdm(snid_list):
    this_ml = np.array(data_dict[snid]['PHOTPROB'])
    this_ml_mask = this_ml >= 0.7
    if len(this_ml[this_ml_mask]) == 0:
        continue
    if len(glob(base_dir+'stamps_lc_20{}/'.format(proc_day)+snid+'/*.jpg')) > 0:
        continue
    
    this_mjd = np.array(data_dict[snid]['MJD'])
    this_nite = np.array(data_dict[snid]['NITE'])
    this_flt = np.array(data_dict[snid]['FLT'])
    this_exp = np.array(data_dict[snid]['EXPNUM'])
    this_ccd = np.array(data_dict[snid]['CCDNUM'])
    this_objid = np.array(data_dict[snid]['OBJID'])
    this_flux = np.array(data_dict[snid]['FLUXCAL'])
    this_fluxerr = np.array(data_dict[snid]['FLUXCALERR'])
    this_mag = flux2mag(this_flux)
    this_magerr = fluxerr2magerr(this_flux, this_fluxerr)

    if not os.path.exists(base_dir+'stamps_lc_20{}/'.format(proc_day)+snid):
        os.makedirs(base_dir+'stamps_lc_20{}/'.format(proc_day)+snid)
    
    for i in range(len(this_ml[this_ml_mask])):
        try:
            stamp_tarball = glob('/pnfs/des/persistent/gw/exp/{}/{}/dp1600/{}_{:02d}/stamps*/stamps*.tar.gz'.format(this_nite[this_ml_mask][i], this_exp[this_ml_mask][i], this_flt[this_ml_mask][i], this_ccd[this_ml_mask][i]))[0]
        except IndexError:
            with open('fail_objids.txt', 'a') as failfile:
                failfile.write('{} {} {} {} {} {}\n'.format(this_objid[this_ml_mask][i], this_nite[this_ml_mask][i], this_exp[this_ml_mask][i], this_flt[this_ml_mask][i], this_ccd[this_ml_mask][i], this_ml[this_ml_mask][i]))
            continue
        with tarfile.open(stamp_tarball, 'r:gz') as tf:
            objid_filename_end = '{}.gif'.format(this_objid[this_ml_mask][i])
            try:
                diffimg = tf.extractfile('diff'+objid_filename_end)
                srchimg = tf.extractfile('srch'+objid_filename_end)
                tempimg = tf.extractfile('temp'+objid_filename_end)
                with open(base_dir+'stamps_lc_20{}/'.format(proc_day)+snid+'/diff'+objid_filename_end, 'wb') as diffimg_outfile:
                    diffimg_outfile.write(diffimg.read())
                with open(base_dir+'stamps_lc_20{}/'.format(proc_day)+snid+'/srch'+objid_filename_end, 'wb') as srchimg_outfile:
                    srchimg_outfile.write(srchimg.read())
                with open(base_dir+'stamps_lc_20{}/'.format(proc_day)+snid+'/temp'+objid_filename_end, 'wb') as tempimg_outfile:
                    tempimg_outfile.write(tempimg.read())
            except KeyError:
                with open('fail_objids.txt', 'a') as failfile:
                    failfile.write('{} {} {} {} {} {}\n'.format(this_objid[this_ml_mask][i], this_nite[this_ml_mask][i], this_exp[this_ml_mask][i], this_flt[this_ml_mask][i], this_ccd[this_ml_mask][i], this_ml[this_ml_mask][i]))
    plot = plot_lc(snid, this_ml, this_flt, this_mjd, this_mag, this_magerr, base_dir+'stamps_lc_20{}/'.format(proc_day)+snid+'/lightcurve_'+snid+'.jpg')

#os.system("zip -r {}stamps_lc_20{}.zip {}stamps_lc_20{}/".format(base_dir, proc_day, base_dir, proc_day))
#os.system("rm -r {}stamps_lc_20{}/".format(base_dir, proc_day))
