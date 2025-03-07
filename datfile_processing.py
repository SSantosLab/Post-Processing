import glob
import pickle

proc_day = '250225'
lightcurve_dats = glob.glob('/data/des70.a/data/desgw/O4/S250223dk/Post-Processing/out_20{}/makedatafiles/LightCurvesReal/*.dat'.format(proc_day))

dat_file_dict = {} 
var_list_keys = ['RA', 'DEC', 'MJD','FLT','FIELD','FLUXCAL','FLUXCALERR','PHOTFLAG','PHOTPROB','ZPFLUX','PSF','SKYSIG','SKYSIG_T','GAIN','XPIX','YPIX','NITE','EXPNUM','CCDNUM','OBJID']
#Keys = SNIDS; keys to dictionary where that dictionary's keys are the headers of the dat file and the entries are a list of the data

def parse_dat(dat_file_dict, dat):
    datfile = open(dat)
    dat_info = datfile.readlines()
    datfile.close()
    has_host = False
    host_num = 0
    hostgal_id = []
    hostgal_ra = []
    hostgal_dec = []
    hostgal_sep = []
    hostgal_ddlr = []
    hostgal_pz = []
    hostgal_pze = []
    hostgal_sz = []
    hostgal_sze = []
    snid = ''
    data_dict = {}
    for line in dat_info:
        info = line.split()
        if len(info) == 0:
            continue
        if info[0] == 'SNID:':
            snid = info[1]
        elif info[0] == 'RA:':
            ra = float(info[1])
        elif info[0] == 'DECL:':
            dec = float(info[1])
        elif info[0] == 'HOSTGAL_NMATCH:':
            has_host = False
            host_num = int(info[1])
        elif info[0] == 'VARLIST:':
            data_dict = {k:[] for k in var_list_keys}
            data_dict['RA'] = ra
            data_dict['DEC'] = dec
        elif info[0] == 'OBS:':
            data = info[1:]
            for key, datum in zip(var_list_keys[2:], data):
                if key in ['FLT', 'FIELD', 'NITE', 'OBJID']:
                    data_dict[key].append(str(datum))
                elif key in ['PHOTFLAG', 'EXPNUM', 'CCDNUM']:
                    data_dict[key].append(int(datum))
                else:
                    data_dict[key].append(float(datum))
        elif host_num > 0:
            for i in range(host_num):
                if i==0: num_str = ''
                else: num_str = str(i+1)
                if info[0] == 'HOSTGAL'+num_str+'_OBJID:':
                    hostgal_id.append(str(info[1]))
                elif info[0] == 'HOSTGAL'+num_str+'_RA:':
                    hostgal_ra.append(float(info[1]))
                elif info[0] == 'HOSTGAL'+num_str+'_DEC:':
                    hostgal_dec.append(float(info[1]))
                elif info[0] == 'HOSTGAL'+num_str+'_SNSEP:':
                    hostgal_sep.append(float(info[1]))
                elif info[0] == 'HOSTGAL'+num_str+'_DDLR:':
                    hostgal_ddlr.append(float(info[1]))
                elif info[0] == 'HOSTGAL'+num_str+'_PHOTOZ:':
                    hostgal_pz.append(float(info[1]))
                    hostgal_pze.append(float(info[3]))
                elif info[0] == 'HOSTGAL'+num_str+'_SPECZ:':
                    hostgal_sz.append(float(info[1]))
                    hostgal_sze.append(float(info[3]))
        else:
            continue
    data_dict['HOSTGAL_ID'] = hostgal_id
    data_dict['HOSTGAL_RA'] = hostgal_ra
    data_dict['HOSTGAL_DEC'] = hostgal_dec
    data_dict['HOSTGAL_SEP'] = hostgal_sep
    data_dict['HOSTGAL_DDLR'] = hostgal_ddlr
    data_dict['HOSTGAL_PHOTOZ'] = hostgal_pz
    data_dict['HOSTGAL_PHOTOZERR'] = hostgal_pze
    data_dict['HOSTGAL_SPECZ'] = hostgal_sz
    data_dict['HOSTGAL_SPECZERR'] = hostgal_sze
    dat_file_dict[snid] = data_dict

for dat in lightcurve_dats:
    parse_dat(dat_file_dict, dat)

with open('data_dict_{}.pickle'.format(proc_day), 'wb') as handle:
    pickle.dump(dat_file_dict, handle, protocol=pickle.HIGHEST_PROTOCOL)
