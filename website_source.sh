#!/bin/bash
#export API_BASE_URL=https://desgw-api-physics-soares-santos-flaskapi.apps.gnosis.lsa.umich.edu/api/v0.1/
#export CONDA_DIR=/cvmfs/des.opensciencegrid.org/fnal/anaconda2
#source $CONDA_DIR/etc/profile.d/conda.sh
#unset PYTHONPATH
conda activate website
#cat candidates.txt >>  ./dictionaries/all_candidates_$(date +%F).txt
#cat candidate_objects.txt >> ./dictionaries/all_candidates_objects_$(date +%F).txt
#cat galaxies.txt >>  ./dictionaries/all_galaxies_$(date +%F).txt
python website_populator.py
#rm candidates.txt candidate_objects.txt galaxies.txt
conda deactivate 

