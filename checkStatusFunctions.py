import time
from time import strftime
import numpy as np
import os
from glob import glob



def checkForcephoto(season, time):
    passing=0
    date=time.split('_')[0]
    print(time.split('_')[1])
    print(date)
    if date != strftime("%Y%m%d"):
        return False
    logs=glob('.'+season+'/forcephoto/output/dp'+season+'/'+time+'_DESY'+season+'/*_FORCEPHOTO.LOG')
    print('.'+season+'/forcephoto/output/dp'+season+'/'+time+'_DESY'+season)
    print(len(logs),'len o logs')
    count=1
    if len(logs)==0:
        return False
        #while len(logs)==0:
         #   ymd=time.split('_')[0]
          #  minsec=time.split('_')[1]
           # timePlus=ymd+'_'+str(int(minsec)+count)
            #print('./forcephoto/output/dp'+season+'/'+timePlus+'_DESY'+season)
            #logs=glob('./forcephoto/output/dp'+season+'/'+timePlus+'_DESY'+season+'/*_FORCEPHOTO.LOG')
           # print(len(logs))
           # if len(logs)==0:
#                count+=1
    for log in logs:
        l=open(log,'r')
        ll=l.readlines()
        l.close()
        try:
            print(ll[-1].split(' ')[-1].split('\n')[0])
            if ll[-1].split(' ')[-1].split('\n')[0] == '0':
                passing+=1
        except:
            print('no')
            passing=0
    try:
        if passing/len(logs)==1:
            status=True
        else:
            status=False
    except ZeroDivisionError:
        status=False
    
    return status



def checkStatus(statusFile,season,Time): ##All strings
    status=open(statusFile,'r')
    stats=status.readlines()
    status.close()
    progress=stats[-1].split('\n')[0]
    
    currentStatus=[0]*(len(stats))
    
    print('statusFile!', statusFile)
    print('inside checkStatus')

    while progress == 'incomplete':
        
        print('inside the while looooooop.')

        time.sleep(60) ##Pause the program for a minute before checking statuses
        
        status=open(statusFile,'r') ###Read contents of file
        stats=status.readlines()
        status.close()
        print('stats!',stats)
        progress=stats[-1].split('\n')[0] ##Set postproc progress
        print(progress)
        for i in range(len(stats)):
            stat=stats[i]
            if stat == '1 \n':
                currentStatus[i]=True
            elif stat == '0 \n':
                currentStatus[i]=False
            else:
                continue
                
    forcephotoStatus=checkForcephoto(season, Time)
    currentStatus[3]=forcephotoStatus
    
 
    return currentStatus
