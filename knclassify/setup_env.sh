export CONDA_DIR=/cvmfs/des.opensciencegrid.org/fnal/anaconda2
source $CONDA_DIR/etc/profile.d/conda.sh
conda activate des18a

source /cvmfs/des.opensciencegrid.org/eeups/startupcachejob21i.sh
export PYTHONPATH=/cvmfs/fermilab.opensciencegrid.org/products/common/prd/pycurl/v7_16_4/Linux64bit-2-6-2-12/pycurl:$PYTHON\
PATH
