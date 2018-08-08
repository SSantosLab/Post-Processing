import yaml
from astropy.time import Time
import os

with open('recycler.yaml', 'r') as f:
    recy = yaml.load(f)

propid = recy["propid"]
MJDnite = recy["start_of_season"]

nite=Time(MJDnite, format="mjd").iso
nite=nite.split(' ')[0].split('-')[0]+nite.split(' ')[0].split('-')[1]+nite.split(' ')[0].split('-')[2]

os.system('. ./getExpWPropIDandNite.sh -n '+nite+' -p '+propid)
