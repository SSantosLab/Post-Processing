# Define the properties of the event class

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import matplotlib as mpl
import matplotlib.image as mpimg

""" Set Plotting Parameters """
mpl.rcParams['mathtext.rm'] = 'Times New Roman'
mpl.rcParams['mathtext.it'] = 'Times New Roman:italic'
mpl.rcParams['mathtext.bf'] = 'Times New Roman:bold'
mpl.rc('font', family='serif', size=12)
mpl.rcParams['xtick.labelsize'] = 12
mpl.rcParams['ytick.labelsize'] = 12
mpl.rcParams['xtick.major.size'] = 5
mpl.rcParams['ytick.major.size'] = 5

class event:
    def __init__(self, lc_data_file):
        self.filename = lc_data_file
        self.phot_data = self.get_phot_data()
        self.snid = self.getSNID()
        self.ra, self.dec = self.getRADec()
        
    def get_phot_data(event):
        f = open(event.filename, 'r')
        file_info = f.readlines()
        f.close()
        columns = [x.split() for x in file_info if x[0:8] == 'VARLIST:'][0][1:]
        phot_data = [x.split()[1:] for x in file_info if x[0:4] == 'OBS:']
        df = pd.DataFrame(phot_data, columns=columns)
        #df = df[['MJD', 'FLT', 'PHOTFLAG', 'PHOTPROB', 'FLUXCAL', 'FLUXCALERR', 'PSF']]
        for index, row in df.iterrows():
            row['MJD'] = round(float(row['MJD']), 1)
            row['PHOTFLAG'] = int(row['PHOTFLAG'])
            #row['PHOTPROB'] = float(row['PHOTPROB'])
            row['FLUXCAL'] = float(row['FLUXCAL'])
            row['FLUXCALERR'] = float(row['FLUXCALERR'])
            row['PSF'] = float(row['PSF'])
            
        #filter out double observations
        #df = df.drop_duplicates(subset=['MJD', 'FLT'], keep='first')
        return df

    def getSNID(event):
        f = open(event.filename, 'r')
        file_info = f.readlines()
        f.close()
        snid = [x.split() for x in file_info if x[0:5] == 'SNID:'][0][1]
        return snid

    def getRADec(event):
        f = open(event.filename, 'r')
        file_info = f.readlines()
        f.close()
        ra = float([x.split() for x in file_info if x[0:3] == 'RA:'][0][1])
        dec = float([x.split() for x in file_info if x[0:3] == 'DEC'][0][1])
        return ra, dec

    def getClassificationInfo(self, f):
        df = pd.read_csv('%s/classifications.csv' %f.name)
        info = df.loc[df['CID'] == int(self.snid)]
        return info        

    def getCurrentMags(self):
        df = self.phot_data
        filters = np.unique(df['FLT'].values)
        results = []
        for flt in filters:
            df_by_flt = df[df['FLT'] == flt]
            last_mjd = np.max(df_by_flt['MJD'].values)
            last_flux_by_flt = float(df_by_flt[df_by_flt['MJD'] == last_mjd]['FLUXCAL'].values)
            if last_flux_by_flt > 0.0: last_mag_by_flt = round(27.5 - 2.5 * np.log10(last_flux_by_flt), 3)
            else: last_mag_by_flt = ' >27.5'
            results.append((flt, last_mjd, last_mag_by_flt))
        num_additional_rows_needed = 4 - len(results)
        i = 0
        while i < num_additional_rows_needed:
            results.append((' ', ' ', ' '))
            i += 1
        return results

    def getCCD(self):
        f = open(self.filename, 'r')
        file_info = f.readlines()
        f.close()
        try: ccd = [y for y in [x for x in file_info if x[0:len('PRIVATE(DES_ccdnum)')] == 'PRIVATE(DES_ccdnum)'][0].split(' ') if y != ''][1]
        except: ccd = '-1'
        return ccd

    
    def plotLC(self, f):
        df = self.phot_data
        maxflux = np.max(df['FLUXCAL'])

        # Helper function for plotting to order filters by wavelength
        def sorter(flt):
            if flt == 'g': return 1
            if flt == 'r': return 2
            if flt == 'i': return 3
            if flt == 'z': return 4

        def lc_color(flt):
            if flt == 'g': return 'blue'
            if flt == 'r': return 'green'
            if flt == 'i': return 'red'
            if flt == 'z': return 'black'

        #Shift MJDs relative to trigger
        df['MJD_shifted'] = df['MJD'] - f.trigger_mjd
        
        #Order fiters
        filters = np.unique(df['FLT'].values)
        orders = []
        for flt in filters: orders.append(sorter(flt))
        sorted_orders = np.argsort(np.asarray(orders))
        sorted_filters = filters[sorted_orders]

        #Plot light curve
        fig = plt.figure(figsize=(8,6), dpi=120)
        for flt in sorted_filters:
            plot_df = df[df['FLT'] == flt]
            plt.errorbar(plot_df['MJD_shifted'], plot_df['FLUXCAL'], yerr=plot_df['FLUXCALERR'], label=flt+r'-band', color=lc_color(flt))
        plt.xlabel('Days Since Neutrino Trigger (MJD = %.2f)' %f.trigger_mjd)
        plt.ylabel('Flux (Calibrated)')
        plt.title('SNID %s   (RA, Dec) = (%s$^\circ$, %s$^\circ$)' %(self.snid, self.ra, self.dec))
        plt.xlim(0., np.max(df['MJD_shifted'].values) + 2.)
        #plt.ylim(-80., 1.1 * maxflux)
        plt.legend()

        return fig

    def plotLC_stamps(self, f):
        df = self.phot_data
        maxflux = np.max(df['FLUXCAL'])
        
        # Helper function for plotting to order filters by wavelength
        def sorter(flt):
            if flt == 'g': return 1
            if flt == 'r': return 2
            if flt == 'i': return 3
            if flt == 'z': return 4

        def lc_color(flt):
            if flt == 'g': return 'blue'
            if flt == 'r': return 'green'
            if flt == 'i': return 'red'
            if flt == 'z': return 'black'

        #Shift MJDs relative to trigger    
        df['MJD_shifted'] = df['MJD'] - f.trigger_mjd

        #Order fiters
        filters = np.unique(df['FLT'].values)
        orders = []
        for flt in filters: orders.append(sorter(flt))
        sorted_orders = np.argsort(np.asarray(orders))
        sorted_filters = filters[sorted_orders]

        #Obtain stamps
        im_file = '%s/stamps/%s/stamps.png' %(f.name, self.snid)
        
        #Make Plot
        fig, (ax1, ax2) = plt.subplots(1, 2, gridspec_kw = {'width_ratios':[1, 3]}, figsize=(18,6))
        
        #Plot light curve
        for flt in sorted_filters:
            plot_df = df[df['FLT'] == flt]
            #ax1.errorbar(plot_df['MJD_shifted'], plot_df['FLUXCAL'], yerr=plot_df['FLUXCALERR'], label=flt+r'-band', color=lc_color(flt))
            ax1.plot(plot_df['MJD_shifted'], plot_df['FLUXCAL'], label=flt+r'-band', color=lc_color(flt))
	ax1.set_xlabel('Days Since Neutrino Trigger (MJD = %.2f)' %f.trigger_mjd)
	ax1.set_ylabel('Flux (Calibrated)')
	ax1.set_title('SNID %s   (RA, Dec) = (%s$^\circ$, %s$^\circ$)' %(self.snid, self.ra, self.dec))
        ax1.set_xlim(0., np.max(df['MJD_shifted'].values) + 2.)
	#ax1.set_ylim(-80., 1.1 * maxflux)
	ax1.legend()

        #Plot stamps
        ax2.imshow(mpimg.imread(im_file))
        ax2.axes.get_xaxis().set_visible(False)
        ax2.axes.get_yaxis().set_visible(False)
        
	return fig

