from desgw_db_writer.api import DESGWApi
import json 
import datetime
import pandas as pd
import healpy as hp 
import os
import argparse
import configparser
from sys import exit
import numpy as np
from astropy.cosmology import FlatLambdaCDM
from astropy import units as u

# You can write this once at the top of the script and use the same instance throughout
db_writer = DESGWApi()

#ranking_df = pd.read_csv('./S230922g.csv')

#hosts = pd.read_csv('/data/des70.a/data/trruch/PP_10_2/Post-Processing/hostmatch_10_2.txt', sep = ' ')
#hosts_df = hosts[hosts['rank'] == 1]

#with open('./candidates_11_7.txt') as cands:
    #cand = [line.rstrip() for line in cands]

with open('./candidate_objects_11_7.txt') as candobj:
    cand_objs = [line.rstrip() for line in candobj]
    
#with open('./galaxies_11_7.txt') as gal:
    #gals = [line.rstrip() for line in gal]

success = []

"""default_gal ={
        "galaxy_id":-9,
        "photoz":-9.0,
        "specz":-9.0,
        "ra":999.0,
        "dec":999.0,
        "snsep":999.0,
        "gmag":888.0,
        "imag":888.0,
        "rmag":888.0,
        "zmag":888.0}
db_writer.add_galaxy(default_gal)"""

'''count = 0
for i in gals:
    count +=1
    res = json.loads(i)    
    res["galaxy_id"] = int(float(res["galaxy_id"]))
    res["photoz"] = float(res["photoz"])
    res["specz"] = float(res["specz"])
    res["ra"] = float(res["ra"])
    res["dec"] = float(res["dec"])
    res["snsep"] = float(res["snsep"])
    res["gmag"] = float(res["gmag"])
    res["imag"] = float(res["imag"])
    res["rmag"] = float(res["rmag"])
    res["zmag"] = float(res["zmag"])
    success.append(db_writer.add_galaxy(res))
    if count % 10 == 0:
        print(count)'''


'''count = 7100
for i in cand[7100:]:
    count+=1
    res = json.loads(i)
    #if int(float(res["candidate_label"])) in list(hosts_df['#trans_name']):
        #gal_id = hosts_df[hosts_df['#trans_name'] == int(float(res["candidate_label"]))].iloc[0]['SNID']
    #else:
        #gal_id = -9
    
    #result = ranking_df[ranking_df['ID']==float(gal_id)]
    #if len(result) == 1:
        #res["galaxy_percentage"] = float(result['Percent'].values[0])
    #else:
        #res["galaxy_percentage"] = -9.0

    #res["candidate_label"] = res["candidate_label"]
    #res["event_datetime"] =  datetime.datetime.now()
    #res["trigger_label"] = res["trigger_label"]
    #res["candidate_location"] = float(credible_levels[ipix])
    res["ra"] = float(res["ra"])
    res["dec"] = float(res["dec"])
    res["max_ml_score"] = float(res["max_ml_score"])
    res["cnn_score"] = float(res["cnn_score"])
    if res["first_mag"] == 'nan':
        del res["first_mag"]
    else:
        res["first_mag"] = float(res["first_mag"])
    #res["path_to_fits"] = res["path_to_fits"]
    res["host_final_z"] = float(res["host_final_z"])
    res["host_final_z_error"] = float(res["host_final_z_error"])
    res["trigger_mjd"] = float(res["trigger_mjd"])
    res["area"] = float(res["area"])
    res["gwid"] = int(res["gwid"])
    res["far"] = float(res["far"])
    res["light_curve_img"] = "https://des-ops.fnal.gov:8082/desgw-new/S230922g/pngs_11_7/" + res["light_curve_img"]
    #res["survey"] = res["survey"]
    res["pixsize"] = float(res["pixsize"])
    res["mwebv"] = float(res["mwebv"])
    res["mwebv_error"] = float(res["mwebv_error"])
    res["redshift_helio"] = float(res["redshift_helio"])
    res["redshift_final"] = float(res["redshift_final"])
    res["redshift_final_error"] = float(res["redshift_final_error"])
    res["host_galaxy_id"] = int(float(res["host_galaxy_id"]))
    del res["cnn_score"]
    success.append(db_writer.add_candidate(res))
    if count % 10 == 0:
        print(count)
    
#if not all(success):
    #print("data insertion failed, check the logs")'''

count = 24280
for i in cand_objs[24280:]:
    count+=1
    res = json.loads(i)
    #res["candidate_label"] = res["candidate_label"]
    res["mjd"] = float(res["mjd"])
    res["obj_id"] = int(float(res["obj_id"]))
    #res["flt"] = res["flt"]
    #res["field"] = res["field"]                  
    res["fluxcal"] = float(res["fluxcal"])
    res["fluxcal_error"] = float(res["fluxcal_error"])
    if res["mag"] == 'nan':
        res["mag"] = -9.0
    else:
        res["mag"] = float(res["mag"])
    res["mag"] = float(res["mag"])
    res["mag_error"] = float(res["mag_error"])
    res["photflag"] = float(res["photflag"])
    res["photprob"] = float(res["photprob"])
    res["zpflux"] = float(res["zpflux"])
    res["psf"] = float(res["psf"])
    res["skysig"] = float(res["skysig"])
    res["skysig_t"] = float(res["skysig_t"])
    res["gain"] = float(res["gain"])
    res["xpix"] = float(res["xpix"])
    res["ypix"] = float(res["ypix"])
    res["nite"] = float(res["nite"])
    res["expnum"] = float(res["expnum"])
    res["ccdnum"] = float(res["ccdnum"])
    res["temp_img"] = "https://des-ops.fnal.gov:8082/desgw-new/S230922g/stamps_11_7" + res["temp_img"][8:]
    res["search_img"] = "https://des-ops.fnal.gov:8082/desgw-new/S230922g/stamps_11_7" + res["search_img"][8:]
    res["diff_img"] = "https://des-ops.fnal.gov:8082/desgw-new/S230922g/stamps_11_7" + res["diff_img"][8:]
    res["cnn_score"] = -9.0
    #print(res["search_img"])
    success.append(db_writer.add_candidate_object(res))
    if count % 10 == 0:
        print(count)
#if not all(success):
    #print("data insertion failed, check the logs")