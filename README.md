############
Usage: example: python run_postproc.py --expnums 938511 938513 938515 938517 --outputdir ./output/ --season 1370

Flags:

--expnums: List of Exposures. Not necessary if a .list file with exposures is given (see instructions below).

--outputdir: Location of output files.

--season: Season number.

--triggerid: LIGO trigger ID (GW######). (Optional)

--ligoid: LIGO event ID (G######). (Optional)

 --mjdtrigger: MJD of LIGO trigger. (Optional)

--ups: ups mode: True/False. (Optional)

--checkonly: only do the processing check. (Optional)

--schema: Schema used. (Optional)

Outputs:

If an output dir is provided, it will be saved there (with the exception of the forcephoto dir, which will always be saved in the cwd).

Within the saved directory there will be various other directories:

checkoutputs  
log files with the success of each step  
goodchecked.list  
hostmatch  
masterlist  
blacklist.txt - black listed exposures  
MasterExposureList.fits  
MasterExposureList_prelim.fits  
plots  
lightcurves  
stamps  

truthtable{SEASON NUMBER}  
fakes_truth.tab  
truthplus.tab - list of SNFAKE_ID EXPNUM CCDNUM RA DEC MAG MAGERR FLUXCNT TRUEMAG TRUEFLUXCNT SNR REJECT ML_SCORE BAND NITE SEASON  

How to make Post Processing work form a fresh GitHub pull (26.10.2021):  

(1) Copy into the Post-Processing directory a .list file with a list of exposures (one line per exposure, no spaces or commas).  
(2) Install the torch package.  
    == At the end of `diffimg_setup.sh`, set `PYTHONPATH=<path where torch is installed>`.  
(3) Create an additional postproc.ini named `postproc_<season>.ini` (e.g. `postproc_1370.ini`).  
    === Under `[general]`, change:  
    ==== `season` to the desired season,  
    ==== `propid` to the desired prop id,  
    ==== `indir` to the full path of the current directory,  
    ==== `exposures_listfile` to the full path of the file with the list of exposures,  
    ==== `bands` to the desired band(s) (if multiple bands, separate by a comma and no spaces; if all, write `all`).  
    == Remove other .ini files with a similar naming convention, if unnecessary.  
(4) Copy model1.pt and model2.pt from /data/des80.b/data/anavarro/CNNstuff/DECam_CNN.  

How to execute run_posproc from a clean shell:  
(1) Source diffimg_setup.sh.  
(2) Source des20a.sh.  
(3) Activate the des20a conda environment (`conda activate des20a`).  
(4) Run run_postproc.py as shown in the Usage section.  
    
    
    ###############################################################################################
    ######                                   WARNING!                                        ###### 
    ###### Contains elements (`startSeasonCycler.sh`, `seasonCycler`, among possible others) ######
    ###### which make post processing run on multiple seasons simultaneously. Such scripts   ######
    ###### will require unknown amounts of modifications before this can happen for this     ######
    ###### iteration of Post Processing. Similarly, the hostmatching process has             ######
    ###### encountered unforseen issues that will need remedy. She or he who undertakes      ###### 
    ###### these modifications, may the odds be ever in your favor.                          ######
    ###############################################################################################
