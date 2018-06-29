import os
import tarfile
import sys
import time
import math
import subprocess
from glob import glob
import pandas as pd
from collections import OrderedDict as OD
import easyaccess
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import MultipleLocator, FormatStrFormatter
from astropy.io import fits
from astropy.table import Table
import fitsio
import psycopg2
import fnmatch
import configparser
import postproc
import ZapHTML


tars=glob('GifAndFitsstamps_20170825_WS346-527_i_59/*.gif')
MJDList=['57980.366','57982.273','57985.342','57988.337']
Alist=['15934105','15934107','15934131','15934108']
datInfo=[453414,34.895362,-52.245472,-888,-9.0000,-9.0000,-9.0000,-9.0000,-9.000,888.00,888.00,888.00,888.00]
Dictionary,Dict2=postproc.MakeDictforObjidsHere(tars,Alist,MJDList)
HTML=postproc.ZapHTML(Dictionary,Dict2,'Happiness',datInfo)
print(Dictionary)
#print(tars)
print(list(Dictionary.keys()))
