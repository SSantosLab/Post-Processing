#!/usr/bin/env python
""" 
Ravi Gupta, Chris D'Andrea
10 Dec 2013
This script queries the NCSA Oracle DB table SNCAND to obtain transient coordinates 
which are then matched to potential host galaxies, whose information is given in 
Rachel Cane's merged source catalogs.  For each transient, the script searches within 
a 15 arcsec radius and ranks all sources according to their separation from the transient 
in units of directional light radius (DLR).
The results are output to a text file and written to DB.

DEPENDENCIES: Python, cx_Oracle package, atc-tools
INPUTS: 
  -- NCSA Oracle DB
  -- 10 catalog files (one for each SN field)
OUTPUTS:
  -- outfile = "DES_hostmatch_sep_ABT.txt" (All objects w/in 15 arcsec ordered by ang. sep.)
  -- outfile2 = "DES_hostmatch_DLR_ABT.txt"  (This contains the actual hosts you want)
  -- outfile3 = "DES_host_discrepancy_ABT.txt" (List of objects for which host!=nearest object)

v2.1:  August 2014 -- Updates the searched catalog to be a mix of the SVA1-GOLD catalog
                      (where defined) and the SVA1 catalog.

v2.2:  Sep 17 2014 -- With update to ATC2, use atc-tools to ingest the closest host galaxy as a
                      target and add a tag on the transient pointing to the host.

v2.3:  Oct 14 2014 -- Fix bug where all magnitudes written to SNGALS table due to comparison
                      being made on value of array (which is type STRING) instead of float(STRING)

v2.4:  Oct 22 2014 -- Change injestion into ATC2 from the nearest galaxy to the three nearest
                      galaxies.
                      
v2.5:  Oct 31 2014 -- Bug fix introduced in last update:  was tagging in ATC incorrect galaxies
                      as the host.  Added the requirement on hostinfo['rank'][k] == 1

v3.0:  Jul 16 2015 -- Make code command-line runnable
                   -- Edited text for readability (100 char width) & enhanced comments
                   -- Changed names, locations of output files
                   -- Added ability to read in input file in case there is a failure in an
                        earlier running of the script.
                   -- Split off algorithms that compute the DLR into a separate script.
v3.1  Jun 15 2016  -- Add call to function that computes HOST_CONFUSION
                   -- Input HOST_CONFUSION into SNCAND table

v3.2  Sep 12 2016  -- Update SNGALS for all *good* candidates with numepochs_ml>=1 instead of 
                      just those with a transient_name
                   -- Only insert into ATC hosts for candidates with a transient_name
                   -- Select field location for host match from FIELD column in SNCAND table                   
                   -- Instead of entering nothing into SNGALS for hostless candidates, 
                      write SNGALID=-1, DLR_RANK=0 for those candidates.  Are thus excluded
                      from matching from there onwards.
                   -- improved the logfile output readability
                   -- Change order in which HOST CATALOG files are read in; if there is nothing
                      to match in a field, the file is skipped. 
"""


import cx_Oracle
import sys
import os
import math
import numpy as np
import time
import argparse
from healpy.pixelfunc import ang2pix
from astropy.io import fits
from math import pi

#from   atc_tools.ext import helpers as atcfuncs
import HostMatchAlgorithms_Ravi as hma # RRG

################### Input Parsing #################################################################

parser = argparse.ArgumentParser( description = "Assignment of DES transient "
                  "names to SN candidates (SNIDs) based on the current set of naming "
                  "criteria.  Subsequently ingests targets into the ATC if the relevant"
                  " flag is set. Required arguments is SEASON." )

parser.add_argument( "--input", help = "File containing list of named "
                     "transients to ingest to ATC." )

parser.add_argument( "--dbname", "-db", help = "Name of Database at NCSA in "
                     "which we are looking for new transients.   Default is desoper.",
                     default="destest")

parser.add_argument( "--username", "-u", help = "Username for NCSA access.  "
                     "Default is read-only generic username.  CANNOT ACCESS Y2 DB.",
                     default="marcelle")

parser.add_argument( "--password", "-p", help = "Password for NCSA access.  "
                     "Default is read-only generic password.  CANNOT ACCESS Y2 DB.",
                     default="mar70chips")

parser.add_argument( "--verbose", "-v", help = "Print helpful statements to "
                     "check progress of code.  Default is false.", action = "count")

parser.add_argument( "season", type=int, help = "Season number for transient. Default is 70. ",
                     default=70, nargs=1)

#parser.add_argument( "--atc", help = "Ingest all targets into the ATC. "
#                     "Default is false.  In either case, transients info is "
#f                     "written to a log file.", action = "store_true" )

parser.add_argument( "--test", "-t" , help = "Test mode: No writing to "
                     "database is done.  Rather, only an output file is written to "
                     "scratch with the transient names and host names, along with "
                     "information about the test ATC ingestion.", action = "store_true" )
parser.add_argument( "--testdb", "-tdb" , help = "Test mode: No writing to "
                     "database SNGAL is done. It writes to SNGAL_TEST instead. ", action = "store_true" )
args = parser.parse_args()

#####################   DEFINE GLOBAL VARIABLES   #################################################

rad  = np.pi/180                   # convert deg to rad
search = 15.0                      # search radius (arcsec)
farc = search/3600                 # search radius (degree)

isNearest = 0                      # number of cases for which smallest DLR galaxy is also
                                   #the nearest

DLR_cut = 4.0                      #keep as potential hosts only galaxies with d_DLR
                                   #less than this

#Catalog = 'SVA1'                   #SVA1-GOLD catalog in fact.  Variable is prefix for name.

atc_flag = 0         #Set to non-zero for each failure at ATC target ingestion  
db_flag  = 0         #Set to non-zero for each failure at DB update      

#################### Location and names of the catalog files ######################################

#DIR = os.environ.get('TOPDIR_HOSTMATCH') 
DIR = "/home/s1/palmese/GW_reject_cat/"
DIR = "/data/des60.b/data/palmese/GW_reject_cat/"

if not DIR:
    print 'Environment variable for HOSTMATCH directory no set'
    sys.exit("Environment variable not specified for HOSTMATCH; exiting ...")

from time import strftime
thisTime = strftime("%Y%m%d.%H%M%S")

INPUT_DIR  = DIR + '/v1.0.0/'
OUTPUT_DIR = DIR + '/hostmatching/RUNLOGS/' + thisTime + '/'

try:
    os.mkdir(OUTPUT_DIR, 0755)
except:
    sys.exit("Could not create output directory for RUNLOGS ... ")

outfile_sep      = OUTPUT_DIR + "byAngularSeparation.ABT.txt"
outfile_dlr      = OUTPUT_DIR + "byDLR.ABT.txt"
outfile_mismatch = OUTPUT_DIR + "mismatchhost.ABT.txt"

file_sep      = open(outfile_sep, 'a'      )
file_dlr      = open(outfile_dlr, 'a'      )
file_mismatch = open(outfile_mismatch, 'a' )

# header
h  = ('#trans_name SNID      COADD_OBJ_ID   RA          DEC           sep          '
      'DLR          A          B          rPHI     rank\n' )
h2 = ('#trans_name SNID      COADD_OBJ_ID   RA          DEC           sep          '
      'DLR          A          B          rPHI      rank  host\n')

file_sep.write(h)
file_dlr.write(h2)
file_mismatch.write(h)


logfile_name_atc = OUTPUT_DIR + "hostmatch.atc.log"
logfile_name_db  = OUTPUT_DIR + "hostmatch.db.log"

logfile_atc = open(logfile_name_atc,'w')
logfile_db  = open(logfile_name_db,'w')

logfile_db.write('#DB\n')
logfile_db.write('#List of new host galaxies assigned\n')
logfile_db.write('#Status=0 --> Successful update to database\n')
logfile_db.write('#Status=1 --> Failed update\n')
logfile_db.write('#             A new run of desHostMatch should be done\n')
logfile_db.write('#             if there are any failures in desHostMatch\n')
logfile_db.write('#             Input file NOT REQUIRED.\n')

logfile_db.write('{0:<16}{1:<16}{2:<16}{3:<12}{4:<12}{5:>10}{6:>7}\n'.format(
            '#SNID','TransientName','HostObjectID','RA','DEC','DLR_RANK','Status'))

logfile_atc.write('#ATC\n')
logfile_atc.write('#List of new host galaxies assigned\n')
logfile_atc.write('#Status=0 --> Successful ingestion to ATC\n')
logfile_atc.write('#Status=1 --> Failed ingestion; remains to be uploaded\n')
logfile_atc.write('#Status=2 --> Failed tagging; remains to be associated with SN\n')
logfile_atc.write('#Status=4 --> ATC tagging skipped.  remains to be associated with SN\n')
logfile_atc.write('#             Any Status > 0 requires a new run of desHostMatch\n')
logfile_atc.write('#             with this file given to the --input flag\n')

logfile_atc.write('{0:<16}{1:<25}{2:<16}{3:<16}{4:<7}{5:<7}\n'.format(
                '#TransientName','HostName','RA','DEC','DLR_RANK','Status'))


###############################   MAIN   ##########################################################

def main():
    global isNearest
    global maxgalid
    global db_flag
    global atc_flag
    
    if args.verbose > 0:
        print 'verbose = ',args.verbose
    
    if args.testdb >0:
	sngals_file = 'SNGALS_TEST'
	sngals_file_id = 'OBJECTS_ID'
    else:
	sngals_file = 'SNGALS'
        sngals_file_id = 'COADD_OBJECTS_ID'

    start_time = time.time()

    hostname   = 'leovip148.ncsa.uiuc.edu'
    port       = 1521
    dbname     = args.dbname
    username   = args.username
    password   = args.password
    
    dsn        = cx_Oracle.makedsn(hostname, port, service_name = dbname)
    connection = cx_Oracle.Connection(username, password, dsn)
    cursor     = connection.cursor()

    #----------------------------------------------------------------------------------------------
    #----------- Query all transients that have no entry in the SNGALS table. ---------------------
    # ---------- These are the galaxies we are to host-match --------------------------------------
    #----------------------------------------------------------------------------------------------

    query  = ( "SELECT c.transient_name, c.SNID, c.ra, c.dec, "
               #"substr(c.transient_name,6,2), FIELD "
               "from SNCAND c LEFT JOIN SNGALS g on c.SNID=g.SNID "
               "where c.transient_name is not NULL "
               "and c.transient_status >= 0 and c.snfake_id = 0 "
               "and g.SNGALID is NULL " )

    query_new  = ( "SELECT c.transient_name, c.SNID, c.ra, c.dec "
               #"c.FIELD as field "
               "from SNCAND c LEFT JOIN "+sngals_file+" g on c.SNID=g.SNID "
               #TO BE PUT BACK LATER! "where c.numepochs_ml>=1 and c.numobs_ml>=2 "
               "where (c.transient_status is NULL or c.transient_status >=0) "
               "and c.snfake_id=0 and c.cand_type >=0 "
	       "and g.SNGALID is NULL "
               #"and g.SNGALID is NULL and c.FIELD is not NULL ") 
               "and c.season="+str(args.season[0])+" ")

    print query_new
    cursor.execute(query_new)
    data=cursor.fetchall()

    if args.verbose > 0:
        print "Number of DES Candidates to match = ",len(data)

    #Sort by field to lessen memory issues on loading large files
    if len(data) > 0:
        #data.sort(key=lambda x:x[4])
        data = np.array(data)
    print data

    #----------------------------------------------------------------------------------------------
    #------------ Query the maximum SNGALID to start counting with --------------------------------
    #----------------------------------------------------------------------------------------------

    query = "SELECT max(SNGALID) from "+sngals_file

    cursor.execute(query)
    galmax=cursor.fetchall()

    if galmax[0][0] is None:
        maxgalid = 0
    else:
        maxgalid = galmax[0][0]
         
    if args.verbose > 0:
        print "Max GALID = ",maxgalid

    #----------------------------------------------------------------------------------------------
    #------------- Select all field names where a transient is ------------------------------------
    #------------- lacking an identified host and requires matching -------------------------------
    #----------------------------------------------------------------------------------------------
    
    ra_cand = np.array(data[:,2],dtype=float)
    dec_cand = np.array(data[:,3],dtype=float)
    idx = [ra_cand<0.0]
    ra_cand[idx] = ra_cand[idx]+360.
    ra_cand = ra_cand*pi/180.
    dec_cand = (90.-dec_cand)*pi/180.
    pix = ang2pix(32,dec_cand,ra_cand)
    fields = np.unique(pix)

    #----------------------------------------------------------------------------------------------
    #------------- Select all the host galaxies that have already been assigned -------------------
    #------------- to a transient; this way we reassign the same SNGALID if one -------------------
    #------------- has previously been used -------------------------------------------------------
    #----------------------------------------------------------------------------------------------

    query = ( "select SNID, "+sngals_file_id+" , SNGALID from "+sngals_file+
              " where "+sngals_file_id+" is not NULL" )
    
    cursor.execute(query)
    temp = cursor.fetchall()
    temp = np.array(temp)
    
    current_host = np.zeros(np.sum(max(len(temp),1)), \
                            dtype={'names':['SNID','SNGALID','COADD_ID'], \
                                   'formats':['S8','S8','S12']})
    
    if args.verbose > 0:
        print 'Number of objects currently in SNGALS:  ',len(temp)

    if len(temp):
        current_host['SNID']     = temp[:,0]
        current_host['COADD_ID'] = temp[:,1]
        current_host['SNGALID']  = temp[:,2]
    else:
        current_host['SNID']     = -1
        current_host['COADD_ID'] = -1
        current_host['SNGALID']  = -1
        

    
    #----------------------------------------------------------------------------------------------
    #------------- Loop through the data, querying for nearby galaxies ----------------------------
    #------------- and writing results to the database --------------------------------------------
    #----------------------------------------------------------------------------------------------

    for thisField in fields:
        if args.verbose > 0:
            print "Starting field = ",thisField

        # Find all SNe in this field ...                                                                              
        NCands = 0
        if len(data) > 0:
            index  = np.where(pix == thisField)
            index  = [s for s in data] #if s[4].find(thisField) >=0]  #### Check if this line now makes sense
            NCands = len(index)

        if args.verbose > 0 :
            print "\t",NCands," entries to match in this field"

        if NCands > 0:
            # Read in field catalog using 'loadtxt' and store relevant data to an array
            if thisField < 10000:
              filename  = INPUT_DIR + "GW_cat_hpx_0" + str(thisField) + ".fits"
            else:
              filename  = INPUT_DIR + "GW_cat_hpx_" + str(thisField) + ".fits"
            if args.verbose > 0:
                print "\tReading in catalog from file . . . . \n"
                print filename+'\n'

            h = fits.open(filename)
            cat = h[1].data
#            cat = np.loadtxt(filename, \
#                                 dtype={'names': ('COADD_OBJECTS_ID','RA','DEC','PHOTOZ','PHOTOZ_ERR', \
#                                                      'MAG_AUTO_G','MAG_AUTO_R','MAG_AUTO_I','MAG_AUTO_Z', \
#                                                      'MAGERR_AUTO_G','MAGERR_AUTO_R','MAGERR_AUTO_I', \
#                                                      'MAGERR_AUTO_Z','MAG_APER_4_G','MAG_APER_4_R',   \
#                                                      'MAG_APER_4_I','MAG_APER_4_Z','MAGERR_APER_4_G', \
#                                                      'MAGERR_APER_4_R','MAGERR_APER_4_I','MAGERR_APER_4_Z', \
#                                                      'MAG_DETMODEL_G','MAG_DETMODEL_R','MAG_DETMODEL_I', \
#                                                      'MAG_DETMODEL_Z','MAGERR_DETMODEL_G', \
#                                                      'MAGERR_DETMODEL_R','MAGERR_DETMODEL_I',\
#                                                      'MAGERR_DETMODEL_Z','THETA_IMAGE','A_IMAGE','B_IMAGE', \
#                                                      'GALFLAG','CATALOG'), 'formats': \
#                                            ('S12','f8','f8','f8','f8','S10','S10','S10','S10','S10','S10', \
#                                                 'S10','S10','S10','S10','S10','S10','S10','S10','S10','S10', \
#                                                 'S10','S10','S10','S10','S10','S10','S10','S10',\
#                                                 'f8','f8','f8','f4','S12')}, \
#                                 skiprows=1, usecols=[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,\
#                                                          20,21,22,23,24,25,26,27,28,29,30,31,32,33])
        
       
            if args.verbose > 0 :
                print "\tCatalog contains",cat.shape[0],"rows\n"

            #Begin Loop...
            for i,entry in enumerate(index):  #this will not work

                transient_name = entry[0]
                if transient_name is None:
                    transient_name = 'NULL'

                SNID           = entry[1]
                ra             = float(entry[2])  
                dec            = float(entry[3])
                #field          = entry[4]

                if args.verbose > 0:
                    print '\ni=',i, entry[0], entry[1], '(RA, dec) = (',ra,',',dec,')'

                # First pass search over a box
                jndex = (cat['RA'] > ra - farc/math.cos(dec*rad) ) & \
                    (cat['RA'] < ra + farc/math.cos(dec*rad) ) & \
                    (cat['DEC'] > dec - farc) & (cat['DEC'] < dec + farc)
            
                # store all catalog objects within tolerance in a temporary array
                array = cat[jndex]  
                #Keep track of whether a host has been assigned for this object.
                #If not, mark as hostless in SNGALS to prevent repeated searches
                #for this candidate on subsequent runs.
                hostlessFlag = 1 

                if len(array) > 0: 
                
                    # distance to SN in arcsec
                    dist = 3600. * np.sqrt( ( np.cos( dec*rad ) * ( array['RA'] - ra ) )**2 + \
                                                ( array['DEC'] - dec )**2 ) 

                    # Check if galaxies are within search radius
                    circle = dist < search 

                    if np.sum(circle) > 0: 
                        # define structured array containing potential host info
                        hostinfo = np.zeros(np.sum(circle), \
                                                dtype={'names':['transient_name','SNID','SNGALID', \
                                                                    'COADD_ID','RA','DEC','sep','DLR', \
                                                                    'rank','host', 'X2','Y2','XY','A','B', \
                                                                    'rPHI','A_IMAGE','B_IMAGE','THETA_IMAGE'],\
                                                           'formats':['S12','S8','S8','S20','f8','f8','f8', \
                                                                          'f8','i4','i4','f8','f8','f8','f8','f8',\
                                                                          'f8','f8','f8','f8']})
                    
                        hostinfo['transient_name'][:] = transient_name
                        hostinfo['SNID'][:]           = SNID
                        
                        hostinfo['sep']               = dist[circle] # SN-galaxy separation
                        
                        hostinfo['COADD_ID']          = array['ID'][circle]
                        hostinfo['RA']                = array['RA'][circle]
                        hostinfo['DEC']               = array['DEC'][circle]
                        hostinfo['A_IMAGE']           = array['A'][circle]
                        hostinfo['B_IMAGE']           = array['B'][circle]
                        hostinfo['THETA_IMAGE']       = array['THETA'][circle]
                        #print array.shape,hostinfo['COADD_ID']
                        #Deprecated until these parameters are included in DESDM catalogs
                        #hostinfo['X2']               = array['X2WIN_IMAGE'][circle]
                        #hostinfo['Y2']               = array['Y2WIN_IMAGE'][circle]
                        #hostinfo['XY']               = array['XYWIN_IMAGE'][circle]
                                          

                        # Compute d_DLR for all potential hosts
                        retval =  hma.get_DLR_ABT( ra, dec, hostinfo['RA'], hostinfo['DEC'],\
                                                       hostinfo['A_IMAGE'], hostinfo['B_IMAGE'], \
                                                       hostinfo['THETA_IMAGE'], hostinfo['sep'] )
                                        
                        hostinfo['DLR']  = retval[0]
                        hostinfo['A']    = retval[1]
                        hostinfo['B']    = retval[2]
                        hostinfo['rPHI'] = retval[3]

                        # Compute HOST_CONFUSION parameter and insert into SNCAND # RRG
                        #HC = hma.compute_HC(hostinfo['DLR'])
                        #if args.verbose > 0:
                        #    print "\tHOST_CONFUSION = ", HC
                        #query_HC = ("UPDATE SNCAND SET HOST_CONFUSION = {0} "
                        #            "WHERE SNID={1}").format(HC, SNID)
                        #if not args.test:
                        #    cursor.execute(query_HC)
                        #    connection.commit()
                
                        # continue # RRG -- skip the host matching part
 
                        #------------------------------------------------------------------------------
                        #-------------- Order by Angular Separation -----------------------------------
                        #-------------- Results are output to file, and compared to -------------------
                        #---------------DLR, but are not written to database --------------------------
                        #------------------------------------------------------------------------------

                        hostinfo.sort(order='sep')
                           
                        for k in range(0, len(hostinfo)):
                            # rank ordered galaxies
                            hostinfo['rank'][k] = k+1 

                            s = ( "{:10s}  {:7s}   {:30s}   {:10.5f}   {:10.5f}   {:10.5f}   "
                                  "{:10.5f}   {:8.4f}   {:8.4f}   {:8.4f}   {:d}\n" )
                            s = s.format(hostinfo['transient_name'][k], hostinfo['SNID'][k], \
                                             hostinfo['COADD_ID'][k], hostinfo['RA'][k], \
                                             hostinfo['DEC'][k], hostinfo['sep'][k], hostinfo['DLR'][k], \
                                             hostinfo['A'][k], hostinfo['B'][k], \
                                             hostinfo['rPHI'][k], hostinfo['rank'][k])

                            if args.verbose > 1:
                                print '\tSep: ',k, hostinfo['sep'][k], hostinfo['DLR'][k], \
                                    hostinfo['rank'][k], hostinfo['X2'][k], hostinfo['Y2'][k], \
                                    hostinfo['XY'][k]

                            file_sep.write(s)


                        hostinfo.sort(order='DLR')
                        if hostinfo['DLR'][0] == 99.99:
                            hostinfo.sort(order='sep')
                            # if nearest ordered DLR host has DLR = 99.99,
                            # default to nearest separation

                        # Write to file_mismatch if host by angular separation and DLR are different
                        # Write both galaxies

                        if hostinfo['rank'][0] == 1:  # smallest DLR is already nearest galaxy
                            isNearest += 1  # increment counter for this case
                        else:
                            # write smallest angular separation object
                            r = np.where(hostinfo['rank'] == 1)
                            s = ( "{:10s}  {:7s}   {:30s}   {:10.5f}   {:10.5f}   {:10.5f}   {:10.5f}  "
                                  "{:8.4f}   {:8.4f}   {:8.4f}   {:d}\n" )
                            s = s.format( hostinfo['transient_name'][r[0][0]], \
                                              hostinfo['SNID'][r[0][0]], hostinfo['COADD_ID'][r[0][0]], \
                                              hostinfo['RA'][r[0][0]], hostinfo['DEC'][r[0][0]], \
                                              hostinfo['sep'][r[0][0]], hostinfo['DLR'][r[0][0]], \
                                              hostinfo['A'][r[0][0]], hostinfo['B'][r[0][0]], \
                                              hostinfo['rPHI'][r[0][0]], hostinfo['rank'][r[0][0]] )
                            file_mismatch.write(s)

                            hostinfo['rank'][:] = 0    # reset all to 0 to clear the nearest galaxy
                            hostinfo['rank'][0] = 1    # set smallest DLR to have host=1

                            # write smallest DLR object
                            r = np.where(hostinfo['rank'] == 1)
                            s = ( "{:10s}  {:7s}   {:30s}   {:10.5f}   {:10.5f}   {:10.5f}   {:10.5f}  "
                                  "{:8.4f}   {:8.4f}   {:8.4f}   {:d}\n" )
                            s = s.format(hostinfo['transient_name'][r[0][0]], \
                                             hostinfo['SNID'][r[0][0]], hostinfo['COADD_ID'][r[0][0]], \
                                             hostinfo['RA'][r[0][0]], hostinfo['DEC'][r[0][0]], \
                                             hostinfo['sep'][r[0][0]], hostinfo['DLR'][r[0][0]], \
                                             hostinfo['A'][r[0][0]], hostinfo['B'][r[0][0]], \
                                             hostinfo['rPHI'][r[0][0]], hostinfo['rank'][r[0][0]] )
                            file_mismatch.write(s)  

                        #------------------------------------------------------------------------------
                        #------------------ Now go through each galaxy by DLR -------------------------
                        #------------------ (already sorted above) and write to file, -----------------
                        #------------------ write to DB, ngest into ATC, and tag there ----------------
                        #------------------------------------------------------------------------------
                    
                        for k in range(0,len(hostinfo)):
                            # rank ordered galaxies
                            if hostinfo['DLR'][k] < DLR_cut:
                                hostinfo['rank'][k] = k+1
                            else:
                                # assign negative rank if fails DLR cut
                                hostinfo['rank'][k] = (k+1)*-1
                            
                            if hostinfo['rank'][k] == 1:
                                # assign 'host'=1 only if 'rank'=1, else 'host'=0
                                hostinfo['host'][k] = 1  
                            else:
                                hostinfo['host'][k] = 0

                            s = ( "{:10s}  {:7s}   {:30s}   {:10.5f}   {:10.5f}   {:10.5f}   {:10.5f}  "
                                  "{:8.4f}   {:8.4f}   {:8.4f}   {:3d}    {:d}\n" )
                            s = s.format(hostinfo['transient_name'][k], hostinfo['SNID'][k], \
                                             hostinfo['COADD_ID'][k], hostinfo['RA'][k], \
                                             hostinfo['DEC'][k], hostinfo['sep'][k], hostinfo['DLR'][k], \
                                             hostinfo['A'][k], hostinfo['B'][k], hostinfo['rPHI'][k], \
                                             hostinfo['rank'][k], hostinfo['host'][k] )

                            if args.verbose > 1:
                                print '\tDLR: ',k, hostinfo['sep'][k], hostinfo['DLR'][k], \
                                    hostinfo['host'][k],hostinfo['X2'][k], hostinfo['Y2'][k], \
                                    hostinfo['XY'][k]
                                
                            
                            # Only write top 3 ranked galaxies to file/DB
                            if k < 3:
                                # ---------------- Write to File --------------------------------------
                                file_dlr.write(s)
                            
                                # ---------------- Write to Database ----------------------------------
                                temp_galid = 0
                                db_status  = 0

                                # Does this galaxy already exist in the DB?
                                kndex = np.where(current_host['COADD_ID'] == hostinfo['COADD_ID'][k])
                                
                                if len(kndex[0]) > 0:
                                    temp_galid = current_host['SNGALID'][kndex[0][0]]
                                else:
                                    maxgalid   += 1
                                    temp_galid  = maxgalid
				#print array
                                kndex = np.where(array['ID'][:] == int(hostinfo['COADD_ID'][k]))
                                tt = kndex[0][0]
                                #hostname = hostinfo['SURVEY'][k] + "_ID-" + str(array['ID'][tt])
                                
                                query  = ( "INSERT INTO "+sngals_file+
                                           " ( "+sngals_file_id+", RA, DEC, PHOTOZ, "
                                           "PHOTOZ_ERR, MAG_AUTO_I, "
                                           "THETA_IMAGE, A_IMAGE, B_IMAGE, "
                                           "SNID, TRANSIENT_NAME, SNGALID, SEPARATION, "
                                           "DLR_RANK, HOST, SEASON, SURVEY ) "   #Add DLR again once the a b issues are fixed
                                           "VALUES ("
                                           "{0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12},"
                                           "{13},{14},{15},{16} )" )
                                
                                # The Galaxy Catalogs use 99.999 for mag uncertainties.
                                # We have an upper limit of 9.999.
                                # Here's a very stupid fix to that.

                                #if float(array['MAGERR_AUTO_G'][tt]) >= 10:
                                #    array['MAGERR_AUTO_G'][tt] = 9.000
                                #if float(array['MAGERR_AUTO_R'][tt]) >= 10:
                                #    array['MAGERR_AUTO_R'][tt] = 9.000
                                #if float(array['MAGERR_AUTO_I'][tt]) >= 10:
                                #    array['MAGERR_AUTO_I'][tt] = 9.000
                                #if float(array['MAGERR_AUTO_Z'][tt]) >= 10:
                                #    array['MAGERR_AUTO_Z'][tt] = 9.000
                                #if float(array['MAGERR_APER_4_G'][tt]) >= 10:
                                #    array['MAGERR_APER_4_G'][tt] = 9.000
                                #if float(array['MAGERR_APER_4_R'][tt]) >= 10:
                                #    array['MAGERR_APER_4_R'][tt] = 9.000
                                #if float(array['MAGERR_APER_4_I'][tt]) >= 10:
                                #    array['MAGERR_APER_4_I'][tt] = 9.000
                                #if float(array['MAGERR_APER_4_Z'][tt]) >= 10:
                                #    array['MAGERR_APER_4_Z'][tt] = 9.000
                                #if float(array['MAGERR_DETMODEL_G'][tt]) >= 10:
                                #    array['MAGERR_DETMODEL_G'][tt] = 9.000
                                #if float(array['MAGERR_DETMODEL_R'][tt]) >= 10:
                                #    array['MAGERR_DETMODEL_R'][tt] = 9.000
                                #if float(array['MAGERR_DETMODEL_I'][tt]) >= 10:
                                #    array['MAGERR_DETMODEL_I'][tt] = 9.000
                                #if float(array['MAGERR_DETMODEL_Z'][tt]) >= 10:
                                #    array['MAGERR_DETMODEL_Z'][tt] = 9.000

                                # Now execute query
                                query = query.format(array['ID'][tt],array['RA'][tt],\
                                                         array['DEC'][tt],array['ZPHOTO'][tt],\
                                                         array['ZPHOTO_ERR'][tt],array['MAG_I'][tt],\
                                                         array['THETA'][tt],array['A'][tt],\
                                                         array['B'][tt],\
                                                         hostinfo['SNID'][k],\
                                                         hostinfo['transient_name'][k],temp_galid,\
                                                         hostinfo['sep'][k],\
                                                         hostinfo['rank'][k],hostinfo['host'][k],args.season[0],'\''+array['CATALOG'][tt]+'\'')
                                
                                if args.verbose > 0:
                                    print hostinfo['SNID'][k],hostinfo['transient_name'][k],\
                                        array['ID'][tt],\
                                        array['THETA'][tt],array['A'][tt],\
                                        array['B'][tt]

                                dbstring  = 'SN: {0:<12}{1:<16}{2:<16}{3:<12}{4:<12}{5:>10}{6:>7}\n'
                                hostlessFlag = 0
                                try:
                                    if not args.test:
                                        cursor.execute(query)
                                        connection.commit()
                                        db_status  = 0
                                except:
                                    db_status  = 1
                                    #db_flag   += 1
                                print hostlessFlag

                                logfile_db.write( dbstring.format(hostinfo['SNID'][k],
                                                                  hostinfo['transient_name'][k],
                                                                  array['ID'][tt],
                                                                  array['RA'][tt],array['DEC'][tt],
                                                                  hostinfo['rank'][k],db_status) )
                                
                                #-------------------- Ingest to ATC (if not star ) --------------------

                                #if int(array['GALFLAG'][tt]) == 1: 
                                
                                #    atcstring = 'SN: {0:<12}{1:<25}{2:<16.8f}{3:<16.8f}{4:<7d}\n'
                                #    atc_status = 0
                                
                                    #Only ingest the host galaxies if a) we set the ingest flag, and 
                                    # b) the object the galaxies are associated with have a name.
                                    # While this latter step is not strictly necessary, it saves time
                                    # by avoiding ingesting lots of unneccesary entries.
                                    #if args.atc and transient_name != 'NULL':
                                    
                                    #    try:
                                    #        host_incept = atcfuncs.create_incept_post(hostname,
                                    #                                                  array['RA'][tt],
                                    #                                                  array['DEC'][tt])
                                    #    except:
                                    #        atc_status += 1

                                    #    try:
                                    #       out = atcfuncs.send_posts(host_incept,
                                    #                                  test_mode = args.test)
                                    #        if args.verbose > 0:
                                    #            print ('Successfully ingested host of ' +
                                    #                   hostinfo['transient_name'][k] +
                                    #                   ' as galaxy ' + hostname )
                                    #    except:
                                    #        # Assumes that all ATC post errors are due to the
                                    #        # host galaxy already existing in the DB.  
                                    #        print ('WARNING:  ' + hostinfo['transient_name'][k] +
                                    #               ' host ' + hostname + ' already exists in ATC.')
                                    #
                                    #    if int(hostinfo['rank'][k]) == 1:
                                    #        host_tag = atcfuncs.create_host_post(
                                    #            hostinfo['transient_name'][k],hostname
                                    #            )

                                    #        try:
                                    #            out = atcfuncs.send_posts(host_tag,
                                    #                                      test_mode = args.test)
                                    #        except:
                                    #            print ('WARNING:  unable to tag transient ' +
                                    #                   hostinfo['transient_name'][k] +
                                    #                   ' with host ' + hostname )
                                    #            atc_status += 2
                                    #    
                                    #else:
                                    #    # If there is no attempt to write to the ATC, then candidates
                                    #    # by definition remain to be uploaded
                                    #    atc_status += 4
                                    
                                    #
                                    #if atc_status > 0 and transient_name != 'NULL':
                                    #    atc_flag += 1
                                        #If the atc_status flag is set, we print a failure warning
                                        #UNLESS the SN candidate the host is matched to has no name, 
                                        #  in which case there is nowhere to upload to.
                                    
                                     # Write to logfile for potential later ingestion
                                    #logfile_atc.write( atcstring.format( transient_name,
                                    #                                    hostname, array['RA'][tt],
                                    #                                    array['DEC'][tt], atc_status ) )
                                    
                                        

                #At this point we know whether the hostlessFlag has been updated, or if the SNID
                # has no matching host galaxy.  If there is none, update with -1 values
                if hostlessFlag:
                    nohost_GALID = -1
                    nohost_RANK  =  0

                    query = ( "INSERT INTO "+sngals_file+" (SNID, TRANSIENT_NAME, SNGALID, DLR_RANK) VALUES "
                              " ({0}, '{1}', {2}, {3}) " )
                    query = query.format(SNID, transient_name, nohost_GALID, nohost_RANK)
                    print query

                    #if args.verbose > 0:
                    #    print SNID, transient_name,' NO HOST'
                    
                    dbstring  = 'SN: {0:<12}{1:<16}{2:<16}{3:<12}{4:<12}{5:>10}{6:>7}\n'
                
                    try:
                        if not args.test:
                            cursor.execute(query)
                            connection.commit()
                            db_status  = 0
                    except:
                        db_status  = 1
                        #db_flag   += 1                                                                            
		    #### I have added this line as it was giving error (didn't know what to put as db_status in logfile_db.write)
		    if args.test:
			db_status = 1 

                    logfile_db.write( dbstring.format(SNID, transient_name,'N/A',
                                                      'N/A','N/A',0,db_status) )
    

    #------------------------------------------------------------------------------
    #------------------- Summarize and close log files; Post to ATC ---------------
    #------------------------------------------------------------------------------
    print 'db_flag ',db_flag

    if db_flag == 0:
        logfile_db.write('#HostMatch Database Update:  SUCCESS\n')
        print 'HostMatch Database Update:  SUCCESS'
    else:
        logfile_db.write('#HostMatch Database Update:  FAILURE '
                         'on {} uploads\n'.format(db_flag))
        logfile_db.write('#Re-run HostMatch.  No input files needed.\n')    
        print 'HostMatch Database Update:  FAILURE '

    #if (args.atc and atc_flag == 0):
    #    logfile_atc.write('#HostMatch ATC Posting & Tagging:  SUCCESS\n')
        
    #else:
    #    if args.atc:
    #        logfile_atc.write('#HostMatch ATC Posting & Tagging:  FAILURE '
    #                          'on {} posts\n'.format(atc_flag))
    #        print 'HostMatch ATC Posting & Tagging:  FAILURE'
    #    else:
    logfile_atc.write('#HostMatchATC Posting & Tagging:  SKIPPED '
                      'on ALL posts\n')
    print 'HostMatch ATC Posting & Tagging:  SKIPPED'
        
    logfile_atc.write('#Re-run with --input flag set and input file:\n')
    logfile_atc.write('#{0}'.format(logfile_name_atc))
    print 'Run with desHostMatchFix with input file set as {0}'.format(logfile_name_atc)
    
    if args.test:
        logfile_atc.write('#...Except this was only a TEST, so nothing has REALLY happened!\n')
        print '...Except this was only a TEST, so nothing has REALLY happened!'

    file_dlr.close()
    file_sep.close()
    file_mismatch.close()
    
    logfile_atc.close()
    logfile_db.close()
    
    connection.close()
    



############################### Call main script ##################################################

if __name__ == "__main__":
    main()

