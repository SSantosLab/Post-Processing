import time
import numpy as np
import os
from glob import glob
import checkStatusFunctions
import statusPage
import sys
import argparse
import datetime

#parser = argparse.ArgumentParser(description=__doc__,formatter_class=argparse.RawDescriptionHelpFormatter)
#parser.add_argument('--season', help='Season number', type=int)
#args = parser.parse_args()
#season=str(args.season)

import make_Mastermaster

def checker(season):
    seaTim=open('seasonTime'+season+'.txt','r') #season num \n date_time
    seTi=seaTim.readlines()
    #print(seTi)
    seaTim.close()
    season=seTi[0].split('\n')[0]
    time=seTi[1]
    #print(season)
    #print(time)
    print('./forcephoto/output/dp'+season+'/'+time+'_DESY'+season)

    #statusList=checkStatusFunctions.checkStatus('status'+season+'.txt',season,time)
    #print(statusList)
    
    statusList = []
    txtfile = open('PostProcStatus'+str(season)+'.txt', 'r')
    lines = txtfile.read().split('\n')
    txtfile.close()
    for line in lines:
        statusList.append(line)

    statusPage.statusPage(statusList,season)

#    if not os.path.isdir('./html_files'):
#        os.makedirs('./html_files')
    

#    os.system('mv PostProc_statusPage'+str(season)+'.html html_files/')
    os.system('rsync -a PostProc_statusPage'+str(season)+'.html codemanager@desweb.fnal.gov:/des_web/www/html/desgw/post-processing-all/')
    os.system('rsync -a theProtoATC_'+str(season)+'*.html codemanager@desweb.fnal.gov:/des_web/www/html/desgw/post-processing-all/')
    os.system('rsync -a masterHTML'+str(season)+'.html codemanager@desweb.fnal.gov:/des_web/www/html/desgw/post-processing-all/')
    os.system('rsync -a LightCurve_des_real*.png codemanager@desweb.fnal.gov:/des_web/www/html/desgw/post-processing-all/')
    os.system('rsync -a fakemltest_'+str(season)+'*.png codemanager@desweb.fnal.gov:/des_web/www/html/desgw/post-processing-all/')
    os.system('rsync -a fullmap_'+str(season)+'*.png codemanager@desweb.fnal.gov:/des_web/www/html/desgw/post-processing-all/')
    os.system('rsync -a -r GifAndFitsstamps* codemanager@desweb.fnal.gov:/des_web/www/html/desgw/post-processing-all/')

    print("copied html to codemanager")

#    make_Mastermaster.mastermaster('masterHTML'+str(season)+'.html', 'Post Proc', season) original
    date = datetime.date.today()
    today = date.strftime("%Y%m%d")
    make_Mastermaster.mastermaster('masterHTML'+str(season)+'.html', 'Post Proc', today, season)
    
    return("Run checker done")


def public(season):
    #candidate page
    os.system('rsync -aP theProtoATC_'+str(season)+'*.html codemanager@desweb.fnal.gov:/des_web/www/html/desgw/post-processing-all/')
    
    #link to cand page
    os.system('rsync -aP masterHTML'+str(season)+'.html codemanager@desweb.fnal.gov:/des_web/www/html/desgw/post-processing-all/')

    #plots and things for cands
    os.system('rsync -a LightCurve_des_real*.png codemanager@desweb.fnal.gov:/des_web/www/html/desgw/post-processing-all/')
    os.system('rsync -a fakemltest_'+str(season)+'*.png codemanager@desweb.fnal.gov:/des_web/www/html/desgw/post-processing-all/')
    os.system('rsync -a fullmap_'+str(season)+'*.png codemanager@desweb.fnal.gov:/des_web/www/html/desgw/post-processing-all/')
    os.system('rsync -a -r GifAndFitsstamps* codemanager@desweb.fnal.gov:/des_web/www/html/desgw/post-processing-all/')
