import os
import pandas
import psycopg2
import argparse

parser = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
parser.add_argument('--lastExp', metavar='e',type=int, nargs='+', help='Last exposure found.')

args = parser.parse_args()
lastExp=args.lastExp

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
WHERE flavor='object' and exptime>29.999  and RA is not NULL and id>=182809 and expnum>"""+lastExp+"""
ORDER BY id"""

conn =  psycopg2.connect(database='decam_prd',
                           user='decam_reader',
                           host='des20.fnal.gov',
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

line = subprocess.check_output(['tail', '-1', 'exposures.list'])
lastExp=line.split(' ')[1]
g=open('lastExp.txt', 'w+')
g.write(lastExp)
g.close()
