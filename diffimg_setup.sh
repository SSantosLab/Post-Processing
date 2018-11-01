
##### edit these lines before running the code:
export ROOTDIR=/pnfs/des/persistent/gw
export ROOTDIR2=$(pwd) #This should just be where you are running /data/des40.b/data/nsherman/postprocBig 
#export SEASON=416 
#####

source /cvmfs/des.opensciencegrid.org/eeups/startupcachejob21i.sh
#for IFDH
export EXPERIMENT=des
#export PATH=${PATH}:/cvmfs/fermilab.opensciencegrid.org/products/common/db/../prd/cpn/v1_7/NULL/bin:/cvmfs/fermilab.opensciencegrid.org/products/common/db/../prd/ifdhc/v2_1_0/Linux64bit-2-6-2-12/bin
#export PYTHONPATH=/cvmfs/fermilab.opensciencegrid.org/products/common/db/../prd/ifdhc/v2_0_8/Linux64bit-2-6-2-12/lib/python:${PYTHONPATH}
export IFDH_NO_PROXY=1
#export IFDHC_LIB=/cvmfs/fermilab.opensciencegrid.org/products/common/prd/ifdhc/v2_1_0/Linux64bit-2-6-2-12/lib
export IFDH_CP_MAXRETRIES=2
#/cvmfs/grid.cern.ch/util/cvmfs-uptodate /cvmfs/des.opensciencegrid.org
#source /cvmfs/des.opensciencegrid.org/2015_Q2/eeups/SL6/eups/desdm_eups_setup.sh
#source /cvmfs/des.opensciencegrid.org/eeups/startup.sh
export EUPS_PATH=/cvmfs/des.opensciencegrid.org/eeups/fnaleups:$EUPS_PATH

#other setups
setup perl 5.18.1+6 # || exit 134
setup Y2Nstack 1.0.6+18
setup diffimg
setup ftools v6.17 
export HEADAS=$FTOOLS_DIR
setup autoscan
setup easyaccess
setup extralibs 1.0
setup astropy 0.4.2+6
setup psycopg2 2.4.6+8
setup -j healpy 1.8.1+3
setup -j pandas 0.15.2+3

#setup html
echo "EUPS setup complete"

export DES_SERVICES=${HOME}/.desservices.ini 
export DES_DB_SECTION=db-sn-test
export DIFFIMG_HOST=FNAL
export SCAMP_CATALOG_DIR=$PWD/SNscampCatalog
export AUTOSCAN_PYTHON=$PYTHON_DIR/bin/python
export DES_ROOT=/data/des20.b/data/SNDATA_ROOT/INTERNAL/DES 


### Add for multi-season processing ###
ARRAY=($(ls postproc_*.ini)) #list of postproc_SEASON.ini files
ELEMENTS=${#ARRAY[@]}
echo $ELEMENTS

for ((count=0;count<$ELEMENTS;count++))
do
    INI=${ARRAY[${count}]}
    python getSeason.py --ini $INI
    SEASON=`cat getSeason.txt`

    export SEASON

    export TOPDIR_SNFORCEPHOTO_IMAGES=${ROOTDIR2}/forcephoto/images/dp${SEASON} 
    export TOPDIR_SNFORCEPHOTO_OUTPUT=${ROOTDIR2}/forcephoto/output/dp${SEASON} 

    if [ ! -d $TOPDIR_SNFORCEPHOTO_OUTPUT ]; then mkdir -p $TOPDIR_SNFORCEPHOTO_OUTPUT ; fi
###                                                      
    if [ ! -d $TOPDIR_SNFORCEPHOTO_IMAGES ]; then
	mkdir -p $TOPDIR_SNFORCEPHOTO_IMAGES
	if [ -d $(echo $ROOTDIR/forcephoto/images/dp${SEASON}) ]; then
            for nite in $(ls $ROOTDIR/forcephoto/images/dp${SEASON})
            do
		mkdir ${TOPDIR_SNFORCEPHOTO_IMAGES}/$(basename $nite)                                          
		ln -sf $ROOTDIR/forcephoto/images/dp${SEASON}/${nite}/*/*.fits $ROOTDIR/forcephoto/images/dp${SEASON}/${nite}/*/*.psf ${TOPDIR_SNFORCEPHOTO_IMAGES}/$(basename $nite)/
            done
	fi
    fi
done


###----------------------------------------------------------------------###  
export TOPDIR_DATAFILES_PUBLIC=${ROOTDIR}/DESSN_PIPELINE/SNFORCE/DATAFILES_TEST
export TOPDIR_WSTEMPLATES=${ROOTDIR}/WSTemplates
export TOPDIR_TEMPLATES=${ROOTDIR}/WSTemplates
export TOPDIR_SNTEMPLATES=${ROOTDIR}/SNTemplates
export TOPDIR_WSRUNS=${ROOTDIR}/data/WSruns
export TOPDIR_SNRUNS=${ROOTDIR}/data/SNruns

# these vars are for the make pair function that we pulled out of makeWSTemplates.sh
TOPDIR_WSDIFF=${TOPDIR_WSTEMPLATES}
DATADIR=${TOPDIR_WSDIFF}/data             # DECam_XXXXXX directories
CORNERDIR=${TOPDIR_WSDIFF}/pairs          # output XXXXXX.out and XXXXXX-YYYYYY.out
ETCDIR=${DIFFIMG_DIR}/etc                 # parameter files
CALDIR=${TOPDIR_WSDIFF}/relativeZP        # relative zeropoints
MAKETEMPLDIR=${TOPDIR_WSDIFF}/makeTempl   # templates are made in here

XY2SKY=${WCSTOOLS_DIR}/bin/xy2sky
AWK=/bin/awk
export PFILES=${PWD}/syspfiles

##mkdir -p $TOPDIR_SNFORCEPHOTO_IMAGES $DES_ROOT $TOPDIR_SNFORCEPHOTO_OUTPUT $TOPDIR_DATAFILES_PUBLIC

export SNANA_DIR=/data/des41.b/data/kessler/snana/snana
export SNANA_ROOT=/data/des41.b/data/SNDATA_ROOT

## use Ken's development version of the diffimg code:
#export DIFFIMG_DIR=/data/des40.b/data/kherner/Diffimg-devel/diffimg-trunk
#export PATH=$DIFFIMG_DIR/bin:$PATH

######ACTUALLY, UNLESS SOMETHING RELATING TO SYMLINKS DOES NOT WORK, IGNORE THIS
###The following should be removed once Adam is fuctional
#if [ ! -d $TOPDIR_SNFORCEPHOTO_OUTPUT ]; then mkdir -p $TOPDIR_SNFORCEPHOTO_OUTPUT ; fi
###
##FORCE PHOTO GOODNESS TO BE UNCOMMENTED WHEN NECESSARY
#if [ ! -d $TOPDIR_SNFORCEPHOTO_IMAGES ]; then 
#     mkdir -p $TOPDIR_SNFORCEPHOTO_IMAGES
#     if [ -d $(echo $ROOTDIR/forcephoto/images/dp${SEASON}) ]; then
# 	for nite in $(ls $ROOTDIR/forcephoto/images/dp${SEASON})
# 	do
# 	    mkdir ${TOPDIR_SNFORCEPHOTO_IMAGES}/$(basename $nite)}
#	    #Backtothebeginning
#	   THIS IS THE DROID YOU ARE LOOKING FOR (IGNORE THE OTHER 2) #ln -sf $ROOTDIR/forcephoto/images/dp${SEASON}/${nite}/*/*.fits $ROOTDIR/forcephoto/images/dp${SEASON}/${nite}/*/*.psf ${TOPDIR_SNFORCEPHOTO_IMAGES}/$(basename $nite)/
#	    ln -sf $ROOTDIR/forcephoto/images/dp${SEASON}/${nite}/*.fits $ROOTDIR/forcephoto/images/dp${SEASON}/${nite}/*.psf ${TOPDIR_SNFORCEPHOTO_IMAGES}/$(basename $nite)/
#
## 	    ln -sf $ROOTDIR/forcephoto/images/dp${SEASON}/${nite}/*.fits $ROOTDIR/forcephoto/images/dp${SEASON}/${nite}/*.psf ${TOPDIR_SNFORCEPHOTO_IMAGES}/$(basename $nite)/
# 	done
#     fi%%%
# fi
