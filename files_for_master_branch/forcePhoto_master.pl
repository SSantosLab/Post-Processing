#!/usr/bin/env perl
#
# Created Jul 15 2014 by R.Kessler
# Driver script to 
#
# 1) create list using makeForceList_allCand
# 2) run the forcePhoto jobs using avove list, fill SNFORCE table
# 3) run the hostDB job(=90), fill SNHOSTFLUX table
#
#
# Usage:
#  forcePhoto_master.pl  \
#     -season    <season>      ! default = -1
#        [OPTIONAL ARGS]
#     -outSubDir  <subdir>     ! sdir under TOPDIR_OUT; default = TIMESTAMP
#     -outSubDir_prefix <prefix>  ! sdir = $prefix_$timeStamp
#     -ncore      <ncore>      ! default = 1
#     -numepochs_min <cut>     ! default = 2
#     -nite_min  yyyymmdd      ! default = 20090101
#     -nite_max  YYYYMMDD      ! default = 20201231
#     -snid_min  <snid_min>    ! default = 1
#     -snid_max  <snid_max>    ! default = huge number
#     -day_min   <day_min>     ! start this many days ago 
#     -day_max   <day_max>     ! continue up to this many days ago
#     -version_template <ver>   ! template version for force_hostGal
#     -TOPDIR_SNFORCEPHOTO_IMAGES <dir>  ! over-ride ENV
#     -writeDB                 ! write to DB; default is NO DB
#     -FORCELIST               ! force entire list; ignore SNFORCE entries
#     -NCSA                    ! debug: pretend we're at NCSA (for rsync)
#     -NOTNCSA                 ! pretend we're NOT at NCSA
#     -binPath  <binPath>      ! debug: point to test codes here
#     -inFile_forceList        ! read RA,DEC from ascii file
#     -RSYNC_ONLY              ! just rysync and nothing else (for cron job)
#     -SKIP_CORRUPTFILE        ! skip corrupt images instead of aborting
#
# Examples:
#   forcePhoto_master.pl  -day_min -10   -day_max -5
#        [process nites between -10 and -5 days ago]
#     
#   if ncore > 0 then each job processes NCCD/ncore jobs.
#
#
#              HISTORY
#           ~~~~~~~~~~~~~
#
# Nov 17 2014: replace -outFile_rsyncList  with  -outFile_fpackList so that
#              fits files are fpack'ed as they are copied. PSF files is the
#              same as before.
#
# Dec 1 2014: in the snana nml to make FITS format, add SNTABLE_LIST = '' .
#
# Dec 22, 2014: fix snana output so that after 'wget' to $SNDATA_ROOT/lcmerge,
#               just tar -xzf xxx.tar.gz and it's ready to go for snana jobs.
#
# Jan 13 2015: remove makeDataFile part since there is now a separate script
#                makeDataFiles_master.pl
#
# Jan 20 2015 : new input option -SKIP_CORRUPTFILE
#
# Aug 9 2015: check for makeForceList abort; see MAKEFORCELIST_STDOUT
#
# Jan 8 2016: in mkdir_output(), create INTERNAL_SEASON.DAT file if
#             -season arg is given.
#             
#             Fix harmless-but-annoying bug in submitJobs to launch
#             SBhost job only if templates are defined.
#
# Oct 3 2016: create INTERNAL_SEASON.DAT only if SEASON>10 --> GW program.
#             Otherwise, compute SEASON from NITE for SN program.
#
############################################

use strict ;
use Time::Local ;

# globals
my @DATAFORMAT_LIST = ( "snana" );  # for makeDataFiles

my $NCCD_TOT = 62 ;

my $JOBNAME_MAKELIST      = "makeForceList_allCand" ;
my $JOBNAME_FORCEPHOTO    = "forcePhoto" ;
my $JOBNAME_MAKEDATAFILES = "makeDataFiles_fromSNforce" ;
my $JOBNAME_HOSTGAL       = "forcePhoto_hostGal";

my $NITE_TODAY = `date +%Y%m%d` ;
my $TIMESTAMP  = `date +%Y%m%d_%H%M` ;  $TIMESTAMP   =~ s/\s+$// ; 
my $USER       = `whoami` ;  $USER   =~ s/\s+$// ; 

my $DIFFIMG_HOST     =  $ENV{'DIFFIMG_HOST'};
my $TOPDIR_OUT       =  $ENV{'TOPDIR_SNFORCEPHOTO_OUTPUT'};
my $TOPDIR_IMAGES    =  $ENV{'TOPDIR_SNFORCEPHOTO_IMAGES'};

my $TOPDIR_URL       =  $ENV{'TOPDIR_DATAFILES_PUBLIC'};
my $TOPDIR_TEMPLATES =  $ENV{'TOPDIR_TEMPLATES'};

my $NCSA_HISTORY_LOGFILE = "HISTORY_FORCEPHOTO.TXT" ;

my $qq = '"' ;
my $RETURN_CODE = "echo ${qq}RETURN CODE: \$\?${qq}" ;

my @REALFAKE = ( "REAL", "FAKE" ) ;

my $IJOB_HOSTGAL            = 90 ;
my $IJOB_MAKEDATAFILES_REAL = 100;
my $IJOB_MAKEDATAFILES_FAKE = 101;

my $rsyncList = "RUN000_RSYNC" ;  # used at NCSA only
my $rsyncLog  = "RUN000_RSYNC.LOG" ; 
my $rsyncDone = "RUN000_RSYNC.DONE" ; 
my $QUIT_AFTER_RSYNC = 0;

my $SKIP_CORRUPTFILE = 0 ;  # default is to abort on corrupt file

# - - - user command-line inputs - - - -

my ($INPUT_NCORE, $INPUT_OUTSUBDIR, $INPUT_OUTSUBDIR_PREFIX);
my ($INPUT_WRITEDB, $TOPDIR_SNFORCE_IMAGES );
my ($INPUT_DAY_MIN, $INPUT_DAY_MAX);
my ($INPUT_NITE_MIN, $INPUT_NITE_MAX, $INPUT_SEASON );
my ($INPUT_SNID_MIN, $INPUT_SNID_MAX);
my ($INPUT_NUMEPOCHS_MIN, $INPUT_BINPATH );
my ($INPUT_FORCELIST_FLAG, $INPUT_FILE_FORCELIST );
my ($INPUT_VERSION_TEMPLATES);
my $OPT_PROMPT = 1 ;
my $iYear = -9 ;
my $MJD_START = -9.0 ;
my $Yi    = "Yi" ;

# - - - - misc. globals - - - - 

my ($OUTDIR, $atNCSA );
my (@RUNFILE_LIST, @RUNLOG_LIST, @DONEFILE_LIST );
my (@runFile_list, @runLog_list, @doneFile_list );

my (@VERSION_ASCII, @VERSION_FITS, @PRIVATE_SNDATA_PATH ) ;
my (@SNANA_INFILE, @SNANA_LOGFILE) ;
my ($MAKEFORCELIST_STDOUT);

# ------ subs ------

sub checkDuplicateForceJobs ;
sub setInputDefaults ;
sub parse_args ;
sub addDays ;
sub addDaysToDate ;
sub checkENV ;
sub mkdir_output ;
sub addForceJob ;
sub user_prompt ;
sub submitJobs ;

sub set_RUNFILENAMES ;

sub prepareJob_makeForceList ;
sub prepareJob_forcePhoto ;
sub prepareJob_hostGal ;
sub make_inFile_snana ;
sub get_iYear ;
sub historyLog ;
sub wait_for_DONEFILES ;
sub run_IJOB_range ;

# ============= BEGIN MAIN ===============

# verify required ENVs after setInputDefaults
&checkENV("DIFFIMG_HOST",               $DIFFIMG_HOST   );
&checkENV("TOPDIR_SNFORCEPHOTO_OUTPUT", $TOPDIR_OUT     );
&checkENV("TOPDIR_SNFORCEPHOTO_IMAGES", $TOPDIR_IMAGES  );
&checkENV("TOPDIR_TEMPLATES",           $TOPDIR_TEMPLATES );

# check if we're at NCSA (needed to setup rsync job)
if ( $DIFFIMG_HOST eq "NCSA" ) { $atNCSA = 1; }  else { $atNCSA=0; }

# abort if there is already a 'force' process going (Aug 28 2014)
&checkDuplicateForceJobs();

&setInputDefaults();

&parse_args() ;

# get integer season index from nite_min
$Yi = "Y${INPUT_SEASON}" ;
## &get_iYear();


# create output dir for ascii output and stdout
&mkdir_output() ;

# prepare the RUN* scripts
&prepareJob_makeForceList();

if ( $QUIT_AFTER_RSYNC ) { &submitJobs(); }

&prepareJob_forcePhoto();

&prepareJob_hostGal();  # for host surface brightness

# submit, or quit
&submitJobs();


# ========== END MAIN ===============

sub checkDuplicateForceJobs {

    my (@BLA, $bla, $NTOT);

    @BLA = qx(ps -aef | grep force | grep perl);
    $NTOT = scalar(@BLA);

    # quit on more than 2 since we expect 'current' and 'grep'
    # register in the ps command.

    if ( $NTOT > 2 ) {
	foreach $bla (@BLA) { print "\n$bla" ; }
	die "\n ABORT: force job already running.\n" ;
    }

} # end of sub checkDuplicateForceJobs 

sub setInputDefaults {    

    $INPUT_FILE_FORCELIST    = "" ;
    $INPUT_OUTSUBDIR         = $TIMESTAMP ;
    $INPUT_OUTSUBDIR_PREFIX  = "" ;

    $TOPDIR_SNFORCE_IMAGES = "" ;  

    $INPUT_DAY_MIN = 999 ;
    $INPUT_DAY_MAX = 999 ;

    $INPUT_SEASON   = -1;
    $INPUT_NITE_MIN = "20090101" ;
    $INPUT_NITE_MAX = "20201231" ;

    $INPUT_SNID_MIN = 1 ;
    $INPUT_SNID_MAX = 99111888 ;

    $INPUT_NCORE   = 1;
    $INPUT_WRITEDB = 0 ;

    $INPUT_NUMEPOCHS_MIN = 2 ;

    $INPUT_BINPATH = "" ;

    $INPUT_FORCELIST_FLAG = 0 ;

    $INPUT_VERSION_TEMPLATES = "" ;

} # end of sub setInputDefaults


# ===========================
sub parse_args {

    # parse command-line args and fill global $INPUT_XXX variables

    my ($NARG, $arg, $ARG, $nextArg, $i);

    $NARG = scalar(@ARGV);
    
    for ( $i = 0; $i < $NARG ; $i++ ) {
	$arg     = lc($ARGV[$i]);
	$ARG     = uc($ARGV[$i]);
	$nextArg = $ARGV[$i+1] ;

	if ( $arg eq "-snid_min" )	{ 
	    $i++ ; $INPUT_SNID_MIN = $nextArg ; 
	}
	elsif ( $arg eq "-snid_max" )  { 
	    $i++ ; $INPUT_SNID_MAX = $nextArg ; 
	}

	elsif ( $arg eq "-season" )  { 
	    $i++ ; $INPUT_SEASON = $nextArg ; 
	}
	elsif ( $arg eq "-nite_min" )  { 
	    $i++ ; $INPUT_NITE_MIN = $nextArg ; 
	}
	elsif ( $arg eq "-nite_max" )  { 
	    $i++ ; $INPUT_NITE_MAX = $nextArg ; 
	}
	elsif ( $arg eq "-numepochs_min" )  { 
	    $i++ ; $INPUT_NUMEPOCHS_MIN = $nextArg ; 
	}
	elsif ( $arg eq "-day_min" )  { 
	    $i++ ; $INPUT_DAY_MIN = $nextArg ; 
	}
	elsif ( $arg eq "-day_max" )  { 
	    $i++ ; $INPUT_DAY_MAX = $nextArg ; 
	}

	elsif ( $arg eq "-infile_forcelist" )  { 
	    $i++ ; $INPUT_FILE_FORCELIST = $nextArg ; 
	}

	elsif ( $arg eq "-ncore" )  { 
	    $i++ ; $INPUT_NCORE = $nextArg ; 
	}
	elsif ( $arg eq "-outsubdir" )  { 
	    $i++ ; $INPUT_OUTSUBDIR = $nextArg ; 
	}
	elsif ( $arg eq "-outsubdir_prefix" )  { 
	    $i++ ; $INPUT_OUTSUBDIR_PREFIX = $nextArg ; 
	}
	elsif ( $arg eq "-binpath" )  { 
	    $i++ ; $INPUT_BINPATH = "$nextArg/" ; 
	}
	elsif ( $ARG eq "-TOPDIR_SNFORCEPHOTO_IMAGES" )  { 
	    $i++ ; $TOPDIR_SNFORCE_IMAGES = $nextArg ; 
	}
	elsif ( $arg eq "-writedb" )  { 
	    $INPUT_WRITEDB = 1;
	}
	elsif ( $ARG eq "-FORCELIST" )  { 
	    $INPUT_FORCELIST_FLAG = 1 ;
	}
	elsif ( $arg eq "-ncsa" )  { 
	    $atNCSA = 1;
	}

	elsif ( $arg eq "-noprompt" )  {
	    $OPT_PROMPT = 0;
	}

	elsif ( $arg eq "-notncsa" )  { 
	    $atNCSA = 0 ;
	}

	elsif ( $arg eq "-rsync_only" )  { 
	    $QUIT_AFTER_RSYNC = 1 ;
	}

	elsif ( $arg eq "-skip_corruptfile" )  { 
	    $SKIP_CORRUPTFILE = 1 ;
	}

	elsif ( $arg eq "-version_template" )  { 
	    $i++ ; $INPUT_VERSION_TEMPLATES = $nextArg ;
	}
	else {
	    die "\n FATAL ERROR: unknown arg = '$arg' \n";
	}

    } # end i loop over NARG


    # -----------------------

    if ( $INPUT_DAY_MIN < 900 ) { 
	$INPUT_NITE_MIN = &addDays($NITE_TODAY,$INPUT_DAY_MIN); 
	my $day3 = sprintf("%3d", $INPUT_DAY_MIN);
	print " day_min = $day3  --> nite_min = $INPUT_NITE_MIN \n";
    }

    if ( $INPUT_DAY_MAX < 900 ) { 
	$INPUT_NITE_MAX = &addDays($NITE_TODAY,$INPUT_DAY_MAX); 
	my $day3 = sprintf("%3d", $INPUT_DAY_MAX);
	print " day_max = $day3  --> nite_max = $INPUT_NITE_MAX \n";
    }

    if ( length($INPUT_BINPATH) > 0 ) {
	print " binPath = $INPUT_BINPATH \n";
    }

    if( $INPUT_SEASON < 0 ) {
	die "\n ERROR: must give -season <season> \n";
    }

} # end of parse_args


# =============
sub checkENV {
    my($name,$value) = @_ ;    
    if ( $value eq "" ) {
	die "\n ERROR: Required ENV variable '$name' is not defined \n";
    }
    print " $name    = $value \n" ;
}

# ===============================
sub addDays {    

    my ($today, $dayAdd) = @_ ;

    my ($Y0, $M0, $D0,  $Y1, $M1,  $D1, $dayOut );

    $Y0 = substr($today,0,4);
    $M0 = substr($today,4,2);
    $D0 = substr($today,6,2);

#    print " xxx --------------- \n";
#    print " xxx today = $today   dayAdd=$dayAdd \n";
#    print " xxx Y0=$Y0  M0=$M0   D0=$D0 \n";

    ($Y1, $M1, $D1)  = addDaysToDate($Y0, $M0, $D0, $dayAdd);

    $dayOut = sprintf("%4.4d%2.2d%2.2d", $Y1, $M1, $D1 ) ;

#    print " xxx dayOut = $dayOut \n";

    return $dayOut ;

} # end of   convertDayRange_to_niteRange


### ------------------------------------------------
sub addDaysToDate {

    # pulled this function from the internet:
    # http://www.borngeek.com/2009/04/22/replacement-for-add_delta_days/

    my ($y, $m, $d, $offset) = @_;

    # Convert the incoming date to epoch seconds
    my $TIME = timelocal(0, 0, 0, $d, $m-1, $y-1900);

    # Convert the offset from days to seconds and add
    # to our epoch seconds value
    $TIME += 60 * 60 * 24 * $offset;

    # Convert the epoch seconds back to a legal 'calendar date'
    # and return the date pieces
    my @values = localtime($TIME);
    return ($values[5] + 1900, $values[4] + 1, $values[3]);

}  # addDaysToDate 

# ===============================
sub mkdir_output { 

    # get name of OUTDIR and the RUN-file names

    my ($PREFIX, $SDIR) ;

    $PREFIX = $INPUT_OUTSUBDIR_PREFIX ;
    
    if ( length($PREFIX) > 0 ) { 
	$SDIR = "${TIMESTAMP}_${PREFIX}" ; 
    }
    else { 
	$SDIR = "${INPUT_OUTSUBDIR}_DES${Yi}" ; 	
    }

    if ( $QUIT_AFTER_RSYNC ) 
    { $OUTDIR   =  "$TOPDIR_OUT/RSYNC_ONLY/${SDIR}" ; }
    else 
    { $OUTDIR   =  "$TOPDIR_OUT/${SDIR}" ; }   

    my($ijob, $c3job, $prefix, $i);

    for ($ijob=0; $ijob <= $INPUT_NCORE; $ijob++ ) {
	$c3job = sprintf("%3.3d", $ijob);

	if ( $ijob == 0 ) 
	{ $prefix = "RUN000_MAKELIST" ; }
	else 
	{ $prefix = "RUN${c3job}_FORCEPHOTO" ; }

	&set_RUNFILENAMES($ijob,$prefix);
    }

    # tack on the hostGal job
    $c3job = sprintf("%3.3d", $IJOB_HOSTGAL);
    $prefix = "RUN${c3job}_FORCEPHOTO_HOSTGAL" ;
    &set_RUNFILENAMES($IJOB_HOSTGAL,$prefix);    

# xxxxxxxxxxxxx mark for deletion jan 8 2016 xxxxxxxxxx
#    # tack on the jobs to make ascii data files
#    for($i=0; $i <=1;  $i++ ) {
#	$ijob  = 100 + $i;
#	$c3job = sprintf("%3.3d", $ijob);
#	$prefix               = "RUN${c3job}_MAKEDATAFILES_$REALFAKE[$i]" ;
#	&set_RUNFILENAMES($ijob,$prefix);
#   }
# xxxxxxxxxxxxx

    # create the outdir
    if ( -d $OUTDIR ) { qx(rm -r $OUTDIR); }
    qx(mkdir -p $OUTDIR; chmod g+w $OUTDIR );

    # preserve command in a text file
    my ($CMDFILE, $arg, $c0 );
    $CMDFILE = "$OUTDIR/AAA_forcePhoto_master_COMMAND.LOG" ;
    open  PTRCMD , "> $CMDFILE" ;
    print PTRCMD "$0\n";
    foreach $arg (@ARGV) {
	$c0 = substr($arg,0,1);
	if ( $c0 eq "-" ) 
	{ print PTRCMD "   $arg  " ; }
	else
	{ print PTRCMD "$arg \n" ; }
    }


    # Jan 8 2016
    # if -season is passed as argument, write it to file
    # so that whatSeason() C function reads it instead of
    # computing it.
    if ( $INPUT_SEASON > 10 ) {
	my $tmpFile = "$OUTDIR/INTERNAL_SEASON.DAT" ;
	qx(echo $INPUT_SEASON > $tmpFile);
    }
    

    close PTRCMD ;

} # end of mkdir_output


# ==========================
sub set_RUNFILENAMES {

    my ($ijob, $prefix) = @_ ;

    $runFile_list[$ijob]  = "${prefix}" ;
    $runLog_list[$ijob]   = "${prefix}.LOG" ;
    $doneFile_list[$ijob] = "${prefix}.DONE" ;

    $RUNFILE_LIST[$ijob]  = "$OUTDIR/${prefix}" ;
    $RUNLOG_LIST[$ijob]   = "$OUTDIR/${prefix}.LOG" ;
    $DONEFILE_LIST[$ijob] = "$OUTDIR/${prefix}.DONE" ;	

}  # end of set_RUNFILENAMES

# ===============================
sub prepareJob_makeForceList {

    # prepare job to create list needed by forcePhoto

    my ($arg, @argList, $prefix, $outFile_stdout, $outFile_forceList );

    # create MAKELIST job using RUNFILE[0]
    open PTR_RUN , "> $RUNFILE_LIST[0]" ;
    print "\n Created \n    $RUNFILE_LIST[0] \n";

    # check option to create ENV for path to images;
    # allows creating list on different machine than
    # where the image files reside.
    if ( length($TOPDIR_SNFORCE_IMAGES) > 0 ) {
	print PTR_RUN 
	    "export TOPDIR_SNFORCEPHOTO_IMAGES=$TOPDIR_SNFORCE_IMAGES\n";
    }


    $prefix            = "forcePhoto_${TIMESTAMP}" ;
    $outFile_stdout    = "${prefix}_makeList.stdout" ;
    $outFile_forceList = "${prefix}.input" ;
    $MAKEFORCELIST_STDOUT = $outFile_stdout;  # set global to check ABORT

    @argList = ( "${INPUT_BINPATH}${JOBNAME_MAKELIST}" ) ; 

    if ( length($INPUT_FILE_FORCELIST) > 1 ) {
	@argList = ( @argList , "  -inFile_forceList  $INPUT_FILE_FORCELIST" );
	qx(cp $INPUT_FILE_FORCELIST $OUTDIR/);
    }
    else {
	@argList = ( @argList , "  -numepochs_min  $INPUT_NUMEPOCHS_MIN" );
	@argList = ( @argList , "  -snid_min       $INPUT_SNID_MIN" );
	@argList = ( @argList , "  -snid_max       $INPUT_SNID_MAX" );
    }

    @argList = ( @argList , "  -season         $INPUT_SEASON" );
    @argList = ( @argList , "  -nite_min       $INPUT_NITE_MIN" );
    @argList = ( @argList , "  -nite_max       $INPUT_NITE_MAX" );
    @argList = ( @argList , "  -outFile_forceList  $outFile_forceList" );
    @argList = ( @argList , "  -outFile_stdout     $outFile_stdout" );

    if ( $INPUT_FORCELIST_FLAG ) {
	@argList = (@argList , "  -FORCELIST");
    }

    # if at NCSA then add rsync input    
    if ( $atNCSA ) {
#	@argList = ( @argList , "  -outFile_rsyncList  $rsyncList" );
	@argList = ( @argList , "  -outFile_fpackList  $rsyncList" );
    }

    foreach $arg (@argList) { print PTR_RUN "$arg \\\n" ; }

    print PTR_RUN "\n $RETURN_CODE \n\n";

    print PTR_RUN "touch $doneFile_list[0] \n";
    close PTR_RUN ;

} # end of prepareJob_makeForceList 

### ===========================
sub prepareJob_forcePhoto {

    # ---------------------------------------------
    # prepare the NCORE forcePhoto jobs

    my  $prefix  = "forcePhoto_${TIMESTAMP}" ;
    my ($NCCD_PER_JOB, $XNCCD, $CCDNUM_MIN, $CCDNUM_MAX, $IJOB );

    $XNCCD        = $NCCD_TOT / $INPUT_NCORE ;
    $NCCD_PER_JOB = int($XNCCD + 0.999) ;

    my $txtCCD = "$NCCD_PER_JOB CCDs per job";
    print "\n\t Prepare $INPUT_NCORE parallel forcePhoto jobs with ${txtCCD}.\n";

    $CCDNUM_MIN = 1 ;  $CCDNUM_MAX = $CCDNUM_MIN + $NCCD_PER_JOB-1;
    $IJOB       = 0 ;
    while ( $CCDNUM_MIN <= $NCCD_TOT ) {
	$IJOB++ ;
	&addForceJob($IJOB, $prefix, $CCDNUM_MIN,$CCDNUM_MAX);
	$CCDNUM_MIN += $NCCD_PER_JOB ;
	$CCDNUM_MAX  = $CCDNUM_MIN + $NCCD_PER_JOB-1;

	if ( $CCDNUM_MAX > $NCCD_TOT ) { $CCDNUM_MAX = $NCCD_TOT; }
    }


}  # end of prepareJob_forcePhoto 


# ============================================
sub prepareJob_hostGal {

    # create MAKELIST job using RUNFILE[0]
    # Oct 13 2015: add -numepochs_min arg 

    my ($IJOB, $prefix, $inFile_templateInfo);
    my ($outFile_stdout, $outFile_results);
    my (@argList, $arg);

    if ( length($INPUT_VERSION_TEMPLATES) == 0 ) { return ; }

    $IJOB = $IJOB_HOSTGAL ;

    open PTR_RUN , "> $RUNFILE_LIST[$IJOB]" ;
    print "\n Created \n    $RUNFILE_LIST[$IJOB] \n";

    $prefix = "forcePhoto_hostGal" ;
    $outFile_stdout  = "${prefix}.stdout" ;
    $outFile_results = "${prefix}.out" ;

    # -----------
    $inFile_templateInfo = "TEMPLATE_INFO.DAT" ;
    open  PTR_INFO , "> $OUTDIR/$inFile_templateInfo";
    print PTR_INFO   "TOPDIR_TEMPLATES:   \$TOPDIR_TEMPLATES \n";
    print PTR_INFO   "VERSION_TEMPLATES:  $INPUT_VERSION_TEMPLATES\n";
    close ptr_INFO ;

    @argList = ( "${INPUT_BINPATH}${JOBNAME_HOSTGAL}" ) ; 

    @argList = ( @argList , "  -inFile_templateInfo  $inFile_templateInfo");
    @argList = ( @argList , "  -outFile_results      $outFile_results" );
    @argList = ( @argList , "  -outFile_stdout       $outFile_stdout" );
    @argList = ( @argList , "  -numepochs_min        $INPUT_NUMEPOCHS_MIN");

    if ( $INPUT_WRITEDB ) {
	@argList = (@argList , "  -writeDB");
    }

    foreach $arg (@argList) { print PTR_RUN "$arg \\\n" ; }

    print PTR_RUN "\n $RETURN_CODE \n\n";

    print PTR_RUN "touch $doneFile_list[$IJOB] \n";

    close PTR_RUN ;
    
}  # end of prepareJob_hostGal


# ============================================
sub make_inFile_snana { 

    my ($ijob) = @_ ;

    my ($NMLFILE, $VER_ASCII, $VER_FITS, $dataPath, @MJDRANGE );
    
    $NMLFILE   = "$OUTDIR/$SNANA_INFILE[$ijob]" ;
    $VER_ASCII = "$VERSION_ASCII[$ijob]" ;
    $VER_FITS  = "$VERSION_FITS[$ijob]" ;
    $dataPath  = "$PRIVATE_SNDATA_PATH[$ijob]";

    # define MJDrange to force PKMJDINI to be in this season (Oct 11 2014)
    $MJDRANGE[0] = $MJD_START ; 
    $MJDRANGE[1] = $MJD_START + 230 ;

    open  PTRNML , "> $NMLFILE" ;
    print PTRNML " &SNLCINP \n";
    print PTRNML "    VERSION_PHOTOMETRY    = '$VER_ASCII' \n";
    print PTRNML "    PRIVATE_DATA_PATH     = '$dataPath' \n" ;
    print PTRNML "    VERSION_REFORMAT_FITS = '$VER_FITS' \n";
    print PTRNML "    ABORT_ON_DUPLCID      =  F \n";
    print PTRNML "    MJDRANGE_SETPKMJD     =  $MJDRANGE[0] , $MJDRANGE[1] \n";
    print PTRNML "    SNTABLE_LIST          = ''  \n";  # added Dec 1, 2014
    print PTRNML " &END \n" ;

    close PTRNML ;

}    # end of make_inFile_snana

# ============================================
sub get_iYear{

    # use NITE from first detection to determine integer season
    # 2013->1, 2014->2, etc ... Also determine MJD at start of
    # season.

    my ($nn, $y, $m);

    $nn = "nite_min=$INPUT_NITE_MIN" ;

    # get season index from nite_min
    if ( $INPUT_NITE_MIN < 20120101 ) {
	print "\n WARNING: Cannot determine season from $nn \n";
    }
    
    $y = substr($INPUT_NITE_MIN,0,4); # year
    $m = substr($INPUT_NITE_MIN,4,2); # month

    if ( $m < 5 ) { $y-- ; }

    $iYear = $y - 2012 ;
    $Yi    = "Y${iYear}" ;

    # get approx MJD start in early August (Oct 11 2014)
    $MJD_START = 56500 + 365 * ( $iYear - 1 ) ;

    print "\t iYear = $iYear for $nn (y=$y m=$m)   MJD_START=$MJD_START \n";

} # end of get_iYear


# =============================================
sub historyLog {
    my ($OPT) = @_ ;
    
# OPT=0 --> start log with time stamp to show job is running
# OPT=1 --> job done; fill in misc. info.
#
# Dec 12 2014: return on 'quit-after-rsyn' option 
#

    my ($LOGFILE);

    if ( $atNCSA == 0      ) { return ; }
    if ( $QUIT_AFTER_RSYNC ) { return ; }

    $LOGFILE = "$TOPDIR_URL/$Yi/$NCSA_HISTORY_LOGFILE" ;

    if ( $OPT == 0 ) {
	# add full path to file name of history log
	open  PTR_HISTORY , ">> $LOGFILE" ;
	print PTR_HISTORY "$TIMESTAMP   ***** RUNNING NOW ***** \n";
	close PTR_HISTORY ;
    }
    else {
	print "Update history log file on URL: \n";
	print "   $LOGFILE \n";

	my ($forceList, @bla, @wdlist, $LINE);
	my ($Ncand, $Nimage, $Nep, $pFake, $NITERANGE);

	$forceList  = "$OUTDIR/forcePhoto_${TIMESTAMP}.input" ;
        @bla        = qx(grep SUMMARY_VALUES $forceList );
        @wdlist     = split(/\s+/,$bla[0]) ;

	$Ncand      = sprintf("%6d",   $wdlist[2]);
	$Nimage     = sprintf("%5d",   $wdlist[3]);
	$Nep        = sprintf("%6d",   $wdlist[4]);
	$pFake      = sprintf("%5.3f", $wdlist[5]);
	$NITERANGE  = "$wdlist[6]-$wdlist[7]" ;

	$LINE = "$TIMESTAMP   $Ncand    $Nimage      $Nep   " . 
	    "    $pFake    $NITERANGE   $USER" ;

	open  PTR_HISTORY , ">> $LOGFILE" ;
	print PTR_HISTORY "$LINE\n";	    
	close PTR_HISTORY ;
    }

} # end of historyLog

# ============================================
sub  addForceJob {
    my ($IJOB, $PREFIX, $CCDNUM_MIN,$CCDNUM_MAX) = @_ ;

    # add forcePhoto job using input CCDNUM range
    # Write job command to PTR_RUN
    # PREFIX is used to construct output file names.
    #
    # Jan 20 2015: check SKIP_CORRUPTFILE

    my ($arg, @argList, $runFile, $cnumRange, $JOBNAME );
    my ($outFile_results, $outFile_stdout, $inFile_forceList);
    
    $runFile   = $RUNFILE_LIST[$IJOB] ;
    open PTR_RUN , "> $runFile" ;
    print "    $runFile \n";

    $cnumRange = sprintf("ccdnum%2.2d-%2.2d", $CCDNUM_MIN, $CCDNUM_MAX);

    $inFile_forceList = "${PREFIX}.input" ;
    $outFile_results  = "${PREFIX}_${cnumRange}.out" ;
    $outFile_stdout   = "${PREFIX}_${cnumRange}.stdout" ;

    $JOBNAME = "${INPUT_BINPATH}${JOBNAME_FORCEPHOTO}" ;
    @argList = ( $JOBNAME );

    @argList = (@argList , "  -inFile_forceList  $inFile_forceList");
    @argList = (@argList , "  -outFile_results   $outFile_results ");
    @argList = (@argList , "  -outFile_stdout    $outFile_stdout ");
    @argList = (@argList , "  -ccdnum_min        $CCDNUM_MIN ");
    @argList = (@argList , "  -ccdnum_max        $CCDNUM_MAX ");

    if ( $SKIP_CORRUPTFILE ) {
	@argList = (@argList , "  -ABORT_ON_CORRUPTFILE 0 ");
    }

    if ( $INPUT_WRITEDB ) {
	@argList = (@argList , "  -writeDB");
    }

    foreach $arg (@argList) { print PTR_RUN "$arg \\\n" ; }
    print PTR_RUN "\n $RETURN_CODE \n\n";

    print PTR_RUN "touch $doneFile_list[$IJOB] \n";
    close PTR_RUN ;

} # end of addForceJob


### =======================================                                    
sub user_prompt {

    my ($comment) = @_ ;
    my ($response);

    if ( $OPT_PROMPT == 0 ) { return ; }

    print 
    print " $comment \n";
    print " enter y + <CR> to continue  ? " ;
    $response = <STDIN> ;
    $response =~ s/\s+$// ;
    unless ( $response eq "y" ) {
        die " Bye bye. --> external program/person must submit the jobs.\n";
    }

} # end of user_prompt


# ================================
sub submitJobs {

    # launch make-list job first;
    # when list is made then submit all of the
    # RUN*FORCE jobs simultaneously.
    # (no ssh, no batch; just run interactively)

    my ($IJOB, $IJOB_MIN, $IJOB_MAX, $cmd, $DONEFILE );
    my (@tmp, @wdlist, $outFile_stdout, $txtDB, $comment );
    # --------------------------------------
    # give +x privs on the RUN scripts
    qx(cd $OUTDIR; chmod +x RUN*);

    # ask user to check files
    $txtDB   = "WRITEDB=$INPUT_WRITEDB";
    $comment = "\n Launch RUN-files above with $txtDB";

    if ( $QUIT_AFTER_RSYNC == 0 )  { &user_prompt($comment); }

    # --------------------------------
    # get stdout file to print message for user
    @tmp = qx(grep outFile_stdout $RUNFILE_LIST[0]) ;    
    @wdlist = split(/\s+/,$tmp[0]) ;
    $outFile_stdout = $wdlist[2] ;

    &historyLog(0);

    # ------ make_forceList
    $IJOB = 0 ;
    &run_IJOB_range($IJOB, $IJOB, "makeForceList");

    # Aug 9 2015: if makeForceList aborted, then quit everything
    @tmp = qx(cd $OUTDIR; grep ABORT $MAKEFORCELIST_STDOUT);
    if ( scalar(@tmp) > 0 ) {
	my $tmpFile = "MAKEFORCELIST.ABORT" ;
	qx(cd $OUTDIR; touch $tmpFile);
	die "\n\n FATAL ERROR: makeForceList job aborted. \n" . 
	    "\t ***** ABORT *****\n";
    }

    # --------------------------------------------------------
    # if at NCSA, launch the rsync job and wait for that too
    if ( $atNCSA ) {
	print " QUIT_AFTER_RSYNC: $QUIT_AFTER_RSYNC \n";
	print " Wait for rsync job; for status do \n";
	print "    cd $OUTDIR \n";
	print "    tail -40 $rsyncLog \n";
	
	$cmd =	"cd $OUTDIR; ./$rsyncList >& $rsyncLog" ;
	system("$cmd &");  

	$DONEFILE = "$OUTDIR/$rsyncDone" ;
	&wait_for_DONEFILES(1,$DONEFILE);
	print " Done with rsync. \n\n\n";

	if ( $QUIT_AFTER_RSYNC ) { die " Done. \n"; }
    }

    # ---------------------------------------------------
    # submit force jobs in the background interactively
    $IJOB_MIN = 1;  $IJOB_MAX = $INPUT_NCORE;
    &run_IJOB_range($IJOB_MIN, $IJOB_MAX, "forcePhoto" );

    # -------------------------------
    # launch the hostSB job if templates are defined.
    if ( length($INPUT_VERSION_TEMPLATES) > 0 ) { 
	$IJOB = $IJOB_HOSTGAL ;
	&run_IJOB_range($IJOB, $IJOB, "hostSB" );
    }

  HISTORY_UPDATE:
    &historyLog(1);

} # end of submitJobs

# ======================================
sub run_IJOB_range {
    my ($IJOB_MIN, $IJOB_MAX, $JOBNAME ) = @_ ;

    my ($IJOB, $NJOB, $cmd, $runFile, $runLog, @doneList_local, $idone);

    $NJOB = $IJOB_MAX - $IJOB_MIN + 1;

    print "\n" ;
    print "# ----------------------------------------------------- \n";
    print " Launch $NJOB $JOBNAME jobs interactively: \n";
    $idone = 0 ;

    for($IJOB=$IJOB_MIN; $IJOB <= $IJOB_MAX; $IJOB++ ) {
	$runFile  =  $runFile_list[$IJOB];
	$runLog   =  $runLog_list[$IJOB];
	$cmd   = "cd $OUTDIR; ./$runFile >& $runLog" ;
	print "\t Submit $runFile \n";
	system("$cmd &") ;

	$doneList_local[$idone] = $DONEFILE_LIST[$IJOB] ;
	$idone++ ;
    }

    print "\t Wait for $NJOB DONE file(s). \n" ;
    &wait_for_DONEFILES($NJOB, @doneList_local) ;
    print "\t Done with  $NJOB  $JOBNAME  jobs. \n\n";
    
} # end of run_IJOB_range
 
# ====================================
sub wait_for_DONEFILES {
    my ($NDONE,@DONEFILES) = @_ ;

    # wait for all NDONE @DONEFILES.
    my ($NFOUND_DONE, $ALLDONE, $doneFile);

    $ALLDONE = 0 ;
    
  CHECK_DONE:
    while ( $ALLDONE == 0 ) {
	sleep(5);
	$NFOUND_DONE = 0 ;
	foreach $doneFile (@DONEFILES) {
	    if ( -e $doneFile ) { $NFOUND_DONE++ ; }
	}

	if ( $NFOUND_DONE >= $NDONE ) { $ALLDONE = 1; }
    }

}  # end of wait_for_DONEFILES
 
