# A code to extract features from light curves

import pandas as pd
import numpy as np
import sys
import os
import glob

from followup import followup
from events import event

#use CLI to specify directory and outfile
data_dir = str(sys.argv[1])
outfile = str(sys.argv[2])

#Check for proper file structure and alter if necessary
files = glob.glob(data_dir+'/*')
data_exists = False
for fil in files:
    if fil[-3:] not in ['IST, ist, ORE, ore, DME, dme']:
        dat_or_DAT = fil[-3:]
    if fil.find('data') != -1:
        data_exists = True
        break
if not data_exists:
    os.system('mkdir %s_copy' %data_dir)
    os.system('mkdir %s_copy/data' %data_dir)
    os.system('cp %s/*.%s %s_copy/data' %(data_dir, dat_or_DAT, data_dir))
    os.system('ls -1 %s_copy/data > %s_copy/%s.LIST' %(data_dir, data_dir, data_dir))
    os.system('mv %s %s_backup' %(data_dir, data_dir))
    os.system('mv %s_copy %s' %(data_dir, data_dir))
    


f = followup(data_dir)

#Begin feature extraction
features = ['FILENAME', 'NOBS', 'BANDS', 'RA', 'DEC', 'BRIGHTEST', 'RISE_z', 'RISE_i', 'RISE_r', 'MAX_r-i', 'MIN_r-i', 'MAX_i-z', 'MIN_i-z', 'MAX_SNR_z', 'MAX_SNR_i', 'MAX_SNR_r', 'CUT_1', 'CUT_2', 'CUT_3', 'CCD', 'MAG_r', 'MAG_i', 'MAG_z', 'EXPNUM_1', 'EXPNUM_2','EXPNUM_3', 'EXPNUM_4']
data = []

counter = 0.0
for e in f.events:
    counter += 1
    progress = counter / len(f.events) * 100
    sys.stdout.write('\rExtracting Features:  %.2f / 100.00' %progress)

    
    row = []
    df = e.phot_data

    #FILENAME
    row.append(e.filename)

    #NOBS
    row.append(df.shape[0])

    #BANDS
    all_bands = [x for x in np.unique(df['FLT'].values)]
    bands = ''
    for band in all_bands:
        bands += band
    row.append(bands)

    #RA
    row.append(e.ra)

    #DEC
    row.append(e.dec)

    #BRIGHTEST
    row.append(df[df['FLUXCAL'] == max(df['FLUXCAL'])]['FLT'].values[0])

    #RISE_z
    flux = df[df['FLT'] == 'z']['FLUXCAL'].values
    try: rise = flux[-1] - flux[0]
    except: rise = 0.0
    row.append(rise)

    #RISE_i
    flux = df[df['FLT'] == 'i']['FLUXCAL'].values
    try: rise = flux[-1] - flux[0]
    except: rise = 0.0
    row.append(rise)

    #RISE_r
    flux = df[df['FLT'] == 'r']['FLUXCAL'].values
    try: rise = flux[-1] - flux[0]
    except: rise = 0.0
    row.append(rise)

    #r-i
    flux_r = df[df['FLT'] == 'r']['FLUXCAL'].values
    flux_i = df[df['FLT'] == 'i']['FLUXCAL'].values
    try: diffs_1 = flux_r[0] - flux_i[0]
    except: diffs_1 = 0.0
    try: diffs_2 = flux_r[-1] - flux_i[-1]
    except: diffs_2 = 0.0
    max_diff = max([diffs_1, diffs_2])
    min_diff = min([diffs_1, diffs_2])
    row.append(max_diff)
    row.append(min_diff)

    #i-z
    flux_z = df[df['FLT'] == 'z']['FLUXCAL'].values
    flux_i = df[df['FLT'] == 'i']['FLUXCAL'].values
    try: diffs_1 = flux_i[0] - flux_z[0]
    except: diffs_1 = 0.0
    try: diffs_2 = flux_i[-1] - flux_z[-1]
    except: diffs_2 = 0.0
    max_diff = max([diffs_1, diffs_2])
    min_diff = min([diffs_1, diffs_2])
    row.append(max_diff)
    row.append(min_diff)

    #MAX_SNR
    df['SNR'] = df['FLUXCAL'] / df['FLUXCALERR']
    try: row.append(np.max(df[df['FLT'] == 'z']['SNR'].values))
    except: row.append(0.0)
    try: row.append(np.max(df[df['FLT'] == 'i']['SNR'].values))
    except: row.append(0.0)
    try: row.append(np.max(df[df['FLT'] == 'r']['SNR'].values))
    except: row.append(0.0)

    #'CUT_1', 'CUT_2', 'CUT_3', 'CCD', 'MAG_r', 'MAG_i', 'MAG_z', 'EXPNUM_1', 'EXPNUM_2','EXPNUM_3', 'EXPNUM_4'
    #CUT_1 - require a detection in two bands
    row.append(len(np.unique(e.phot_data['FLT'].values)) == 2)

    #CUT_2 - require all detections to have photprob > 0.7, give simulations an automatic pass for now
    try: phot_probs = np.array([float(x) for x in e.phot_data['PHOTPROB'].values])
    except: phot_probs = np.ones(e.phot_data.shape[0]) * 0.8
    row.append(np.min(phot_probs) >= 0.7)

    #CUT_3 - require a 3 sigma change in flux in at least one band
    r_flux = e.phot_data[e.phot_data['FLT'] == 'r']['FLUXCAL'].values
    r_ferr = e.phot_data[e.phot_data['FLT'] == 'r']['FLUXCALERR'].values
    i_flux = e.phot_data[e.phot_data['FLT'] == 'i']['FLUXCAL'].values
    i_ferr = e.phot_data[e.phot_data['FLT'] == 'i']['FLUXCALERR'].values
    z_flux = e.phot_data[e.phot_data['FLT'] == 'z']['FLUXCAL'].values
    z_ferr = e.phot_data[e.phot_data['FLT'] == 'z']['FLUXCALERR'].values
    if len(r_flux) != 0: r = np.absolute(r_flux[0] - r_flux[-1]) / r_ferr[0] > 2.0
    else: r = False
    if len(i_flux) != 0: i = np.absolute(i_flux[0] - i_flux[-1]) / i_ferr[0] > 2.0
    else: i = False
    if len(z_flux) != 0: z = np.absolute(z_flux[0] - z_flux[-1]) / z_ferr[0] > 2.0
    else: z = False
    row.append(r or i or z)

    #CCD
    row.append(e.getCCD())

    #MAG_r, MAG_i, MAG_z - Check if we need to use zeropoint in flux conversion
    try: 
        flux_r = float(e.phot_data[e.phot_data['FLT'] == 'r']['FLUXCAL'].values[-1])
        if flux_r > 0: mag_r = 27.5 - 2.5 * np.log10(flux_r)
        else: mag_r = 27.5
    except: mag_r = 99.9
    row.append(mag_r)
    try:
        flux_i = float(e.phot_data[e.phot_data['FLT'] == 'i']['FLUXCAL'].values[-1])
        if flux_i > 0: mag_i = 27.5 - 2.5 * np.log10(flux_i)
        else: mag_i = 27.5
    except: mag_i = 99.9
    row.append(mag_i)
    try:
        flux_z = float(e.phot_data[e.phot_data['FLT'] == 'z']['FLUXCAL'].values[-1])
        if flux_z > 0: mag_z = 27.5 - 2.5 * np.log10(flux_z)
        else: mag_z = 27.5
    except: mag_z = 99.9
    row.append(mag_z)

    #EXPNUM_1, EXPNUM_2, EXPNUM_3, EXPNUM_4
    try: expnums = e.phot_data['EXPNUM'].values
    except: expnums = [0, 0, 0, 0]
    if len(expnums) == 4:
        for expnum in expnums: row.append(expnum)
    elif len(expnums) == 3:
        for expnum in expnums: row.append(expnum)
        row.append(0)
    elif len(expnums) == 2:
        for expnum in expnums: row.append(expnum)
        row.append(0)
        row.append(0)
    elif len(expnums) == 1:
        for expnum in expnums: row.append(expnum)
        row.append(0)
        row.append(0)
        row.append(0)
    elif len(expnums) == 0:
        row.append(0)
        row.append(0)
        row.append(0)
        row.append(0)
    else:
        #should never get here
        print " "
        print e.filename, " has more epochs than expected"




    #collect features of light curve
    data.append(row)



    
                
print " Done!"

out_df = pd.DataFrame(data, columns=features)

out_df.to_csv(outfile)
