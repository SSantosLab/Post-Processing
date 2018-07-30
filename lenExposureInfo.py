import os
#import easyaccess as ea                                                                
import pandas
import psycopg2

query= """SELECT COUNT(propid)                                                       
FROM exposure.exposure                                                                 
WHERE flavor='object' and exptime>29.999  and RA is not NULL and id>=182809            
"""


conn =  psycopg2.connect(database='decam_prd',
                           user='decam_reader',
                           host='des20.fnal.gov',
                           #password='mar70chips',                                      
                         port=5443)
lenExp = pandas.read_sql(query, conn)
conn.close()

print('lenExp',lenExp)

f=open('ExpLen.txt','w+')
f.write(lenExp)
f.close()

