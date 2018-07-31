import os                                                                
import pandas
import psycopg2

query= """SELECT COUNT(propid)                                                       
FROM exposure.exposure                                                                 
WHERE flavor='object' and exptime>29.999  and RA is not NULL and id>=182809            
"""


conn =  psycopg2.connect(database='decam_prd',
                           user='decam_reader',
                           host='des20.fnal.gov',                                      
                         port=5443)
lenExpFrame = pandas.read_sql(query, conn)
conn.close()

lenExp=lenExpFrame.iloc[-1]['count']

print('lenExp',lenExp)

f=open('ExpLen.txt','w+')
f.write(str(lenExp))
f.close()

