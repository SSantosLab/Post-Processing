Usage: example: python run_postproc.py --expnums 787202 789559 787204 789561 --outputdir ./icecube_try2/ --season 901 &> runpostproc_iandr_try2.out

Flags:

--expnums: List of Exposures

--outputdir: Location of output files

–season: Season number

--triggerid: LIGO trigger ID (GW######)

--ligoid: LIGO event ID (G######)

 --mjdtrigger: MJD of LIGO trigger

--ups: ups mode: True/False

--checkonly: only do the processing check

--schema: Schema used

Outputs:

If an output dir is provided, it will be saved there (with the exception of the forcephoto dir, which will always be saved in the cwd)

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



How to make Post Processing work form a fresh GitHub pull (23.05.2019)

(1) Copy into the Post-Processing directory an exposures list.
(2) Modify the `postproc.ini` file:
    == Create an additional postproc.ini such as `postproc_SEASON.ini`
    ==== `cp postproc.ini postproc_SEASON.ini` 
    == Remove other .ini files with a similar naming convention, if unnecessary
    == Under `[general]`, change
    ==== `season` to the desired season
    ==== `propid` to the desired propped
    ==== (include an exposures list file, following 
    ====== `exposures_listfile = full190510.list` as an example)
    ==== `bands` to the desired bands
    ===== Band list should have NO WHITESPACE!!!!