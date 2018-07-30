import os
#import easyaccess as ea
import pandas
import psycopg2

#connection = ea.connect('desoper')
#
#q = "select expnum,nite,mjd_obs,radeg,decdeg,band,exptime,propid,obstype,object from prod.exposure where obstype='object' order by expnum" 

#os.system('rm -f exposures.tab')

#connection.query_and_save(q,'exposures.tab')

###query = "SELECT expnum,date,ra,declination,band,exptime,propid,obstype,object FROM exposure.exposure where obstype='object' order by expnum LIMIT 100"
### we do not consider exposures before 182809, which was the first exposure taken on MJD 56352, which is 2013-03-01
query = """SELECT id as EXPNUM,
       TO_CHAR(date - '12 hours'::INTERVAL, 'YYYYMMDD') AS NITE,
       EXTRACT(EPOCH FROM date - '1858-11-17T00:00:00Z')/(24*60*60) AS MJD_OBS,
       ra AS RADEG,
       declination AS DECDEG,
       filter AS BAND,
       exptime AS EXPTIME,
       propid AS PROPID,
       flavor AS OBSTYPE,
       qc_teff as TEFF,
       object as OBJECT
FROM exposure.exposure
WHERE flavor='object' and exptime>29.999  and RA is not NULL and id>=182809
ORDER BY id"""

conn =  psycopg2.connect(database='decam_prd',
                           user='decam_reader',
                           host='des20.fnal.gov',
#                           password='THEPASSWORD',
                         port=5443) 
some_exposures = pandas.read_sql(query, conn)
conn.close()
#print some_exposures.keys()
 

mystrings=''

mystrings=some_exposures.to_string(index_names=False,index=False,justify="left")

myout=open('newdbtest.dat','w')


myout.write(mystrings)
myout.write('\n')
myout.close()


os.system('mv newdbtest.dat exposures.list')
