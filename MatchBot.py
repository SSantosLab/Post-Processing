# First call files with DESGW_Bot ####################################

'''
NOTE: Method of running code: use command line
      >> python3 MatchBot.py --file //full pathname of CSV file//
      [e.g. >> python3 MatchBot.py --file /Users/tristanbachmann/Documents/Candidates.csv]
NOTE: If the code fails to run, may need to pip install requests, argparse, and/or pandas.
NOTE: When creating the CSV file to run through MatchBot, it is important the the keys/headings for RA and DEC are called
      'radeg' and 'decdeg' respectively, as these keys allow MatchBot to use the coordinates to search the TNS remotely.
      Additionally, the names should have the key 'objname' and dates should have the key 'discoverydate'.
'''

# !/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Thu Sep 14 14:38:11 2017

Developed and tested in :
- Python version 3.6.3
- Linux Ubuntu version 16.04 LTS (64-bit)

@author: Nikola Knezevic
"""

import os
import requests
import json
from collections import OrderedDict
import argparse
import numpy as np

parser = argparse.ArgumentParser()
#parser.add_argument('--ra', metavar='a',type=float, nargs='+', help='RA degrees')
#parser.add_argument('--dec', metavar='d', type=float, nargs='+', help='Dec in degrees.'
#                                                                    ' Make sure to specify positive or negative.')
parser.add_argument('--radius', metavar='r',type=str, nargs='+', help= 'Radius around search location, in arcsec.',
                    default='3')
parser.add_argument('--time', metavar='t',type=str, nargs='+', help='The timestamp of the event.',default='')
parser.add_argument('--file', metavar='f',type=str, nargs='+', help='The input file of the list of GW candidates.'
                                                                    ' (CSV format preferred).')
args = parser.parse_args()
#ra = args.ra[0]
#dec = args.dec[0]
radius = args.radius[0]
time = args.time
file = args.file[0]

############################# PARAMETERS #############################
# API key for Bot                                                    #
api_key = "c5cb7a7e9216fecb4d7f1fb87dbeb54dbc1ea9cc"  #
# list that represents json file for search obj                      #
search_obj = [("ra", ""), ("dec", ""), ("radius", ""), ("units", ""),  #
              ("objname", ""), ("internal_name", "")]  #
# list that represents json file for get obj                         #
get_obj = [("objname", ""), ("photometry", "0"), ("spectra", "1")]  #
######################################################################

#############################    URL-s   #############################
# url of TNS and TNS-sandbox api                                     #
url_tns_api = "https://wis-tns.weizmann.ac.il/api/get"  #
url_tns_sandbox_api = "https://sandbox-tns.weizmann.ac.il/api/get"  #
######################################################################

############################# DIRECTORIES ############################
# current working directory                                          #
cwd = os.getcwd()  #
# directory for downloaded files                                     #
download_dir = os.path.join(cwd, 'downloaded_files')  #


######################################################################

########################## API FUNCTIONS #############################
# function for changing data to json format                          #
def format_to_json(source):  #
    # change data to json format and return                          #
    parsed = json.loads(source, object_pairs_hook=OrderedDict)  #
    result = json.dumps(parsed, indent=4)  #
    return result  #


# --------------------------------------------------------------------#
# function for search obj                                            #
def search(url, json_list):  #
    try:  #
        # url for search obj                                             #
        search_url = url + '/search'  #
        # change json_list to json format                                #
        json_file = OrderedDict(json_list)  #
        # construct the list of (key,value) pairs                        #
        search_data = [('api_key', (None, api_key)),  #
                       ('data', (None, json.dumps(json_file)))]  #
        # search obj using request module                                #
        response = requests.post(search_url, files=search_data)  #
        # return response                                                #
        return response  #
    except Exception as e:  #
        return [None, 'Error message : \n' + str(e)]  #


# --------------------------------------------------------------------#
# function for get obj                                               #
def get(url, json_list):  #
    try:  #
        # url for get obj                                                #
        get_url = url + '/object'  #
        # change json_list to json format                                #
        json_file = OrderedDict(json_list)  #
        # construct the list of (key,value) pairs                        #
        get_data = [('api_key', (None, api_key)),  #
                    ('data', (None, json.dumps(json_file)))]  #
        # get obj using request module                                   #
        response = requests.post(get_url, files=get_data)  #
        # return response                                                #
        return response  #
    except Exception as e:  #
        return [None, 'Error message : \n' + str(e)]  #


# --------------------------------------------------------------------#
# function for downloading file                                      #
def get_file(url):  #
    try:  #
        # take filename                                                  #
        filename = os.path.basename(url)  #
        # downloading file using request module                          #
        response = requests.post(url, files=[('api_key', (None, api_key))],  #
                                 stream=True)  #
        # saving file                                                    #
        path = os.path.join(download_dir, filename)  #
        if response.status_code == 200:  #
            with open(path, 'wb') as f:  #
                for chunk in response:  #
                    f.write(chunk)  #
            print('File : ' + filename + ' is successfully downloaded.')  #
        else:  #
            print('File : ' + filename + ' was not downloaded.')  #
            print('Please check what went wrong.')  #
            print(response.status_code)  #
    except Exception as e:  #
        print('Error message : \n' + str(e))  #


######################################################################

import pandas as pd
from pandas.io.json import json_normalize
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

CandidateList = pd.read_csv(file)

RA = CandidateList.radeg
DEC = CandidateList.decdeg

print(CandidateList)

MatchBox = pd.DataFrame(columns=['objname','redshift','hostname','host_redshift','isTNS_AT','discovery_date',
                                 'discoverymag','radeg','decdeg','ra','dec','num_matches','ra_match','dec_match'])
columns=['objname','redshift','hostname','host_redshift','isTNS_AT','discovery_date',
                                 'discoverymag','radeg','decdeg','ra','dec','num_matches','ra_match','dec_match']
MatchBoxList = []
num_matches = []
MatchRA = []
MatchDEC = []
MatchName = []
MatchDate = []
for i in CandidateList.index:
    search_obj = [("ra", RA[i]), ("dec", DEC[i]), ("radius", radius), ("units", "arcsec"),
                  ("objname", ""), ("internal_name", ""), ("public_timestamp", time)]
    response = search(url_tns_api, search_obj)
    if None not in response:
        # Here we just display the full json data as the response
        json_data = format_to_json(response.text)
        print(json_data)
    else:
        print(response[1])

    df = pd.read_json(json_data)
    df1 = df['data']['reply']
    num_matches.append(len(df1))
    DFList = []
    RAs = []
    DECs = []
    RAdiffs = []
    DECdiffs = []
    dists = []
    distvals = []
    Names = []
    Dates = []
    for j in range(len(df1)):
        searchobjdict = df1[j]
        searchobj = searchobjdict['objname']
        print("Here is the search object:")
        print(searchobj)
        print(type(searchobj))
        # get obj
        get_obj = [("objname", searchobj)]
        response = get(url_tns_api, get_obj)
        if None not in response:
            # Here we just display the full json data as the response
            json_data = format_to_json(response.text)
            print(json_data)
        else:
            print(response[1])

        # Now create dataframe from Server Data -- Compare with candidate list

        ServerDataFrame = pd.read_json(json_data)
        DataFrame = ServerDataFrame['data']['reply']
        DataFrame2 = pd.DataFrame(DataFrame)
        DataFrame3 = DataFrame2[["objname", "discoverydate", "radeg", "decdeg","discoverymag"]]
        DataFrameID = pd.DataFrame(DataFrame3, index=['id'])
        DFList.append(DataFrameID)
        RAs.append(DataFrameID.radeg)
        DECs.append(DataFrameID.decdeg)
        Names.append(DataFrameID.objname)
        Dates.append(DataFrameID.discoverydate)
        try:
            FullDataFrame = pd.concat(DFList)
            MatchBoxList.append(DataFrameID)
        except ValueError:
            MatchBoxList.append(pd.DataFrame(["NaN"]))
            continue

    if (len(df1) >= 1):
        row = CandidateList.iloc[i]
        rowra = row['radeg']
        rowdec = row['decdeg']

        for RA_val in RAs:
            RAdiffs.append(abs(RA_val - rowra))
        for DEC_val in DECs:
            DECdiffs.append(abs(DEC_val - rowdec))
        for k in range(len(RAdiffs)):
            dists.append((RAdiffs[k] ** 2 + DECdiffs[k] ** 2) ** 0.5)
        for x in range(len(dists)):
            distvals.append(dists[x][0])
        print(RAdiffs)
        print(DECdiffs)
        print(dists)
        print(distvals)
        print(type(distvals))
        mindist = min(distvals)
        mindex = distvals.index(mindist)
        MatchRA.append(RAs[mindex][0])
        MatchDEC.append(DECs[mindex][0])
        MatchName.append(Names[mindex][0])
        MatchDate.append(Dates[mindex][0])
    else:
        filler = None
        MatchRA.append(filler)
        MatchDEC.append(filler)
        MatchName.append(filler)
        MatchDate.append(filler)

    print(FullDataFrame)

MatchBox = pd.concat(MatchBoxList)
print(num_matches)
CandidateList['Matches'] = num_matches
CandidateList['RA Match'] = MatchRA
CandidateList['DEC Match'] = MatchDEC
CandidateList['Match Name'] = MatchName
CandidateList['Match Date'] = MatchDate
print(CandidateList)
# Optionally, can print out the MatchBox, which stores all matches and their info
# print(MatchBox)
