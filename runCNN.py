#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import numpy as np
from PIL import Image
import os
import sys

import artifact_cnn

def runNN(stampDir, snobjidLS = [], psfLS = [], fluxcalLS = [], fluxcalerrLS = [], CSVdir = ""):
    fullGifList = []
    gifList = []
    CNNDIR = "."
#    outDir = "/data/des80.b/data/anavarro/CNNstuff/DECam_CNN/"
#    stampDir = "/data/des80.b/data/anavarro/CNNstuff/DECam_CNN/stamps"
    """
    Arguments:
    stampDir: STRING
        Directory in which all the stamps being processed are contained
    snobjidLS: LIST of STRINGS
        List of SNOBJIDs to be processed, optional, if not included, processing will be performed on all stamps in stampDir
    psfLS: optional LIST of FLOAT
        List of PSF associated with each SNOBJID being processed, if snobjidLS is non-empty, must match its size
        if snobjidLS is empty, must match the number of gif triplets in stampDir
    fluxcalLS: optional LIST of FLOAT
        List of FLUXCAL associated with each SNOBJID being processed, if snobjidLS is non-empty, must match its size
        if snobjidLS is empty, must match the number of gif triplets in stampDir
    fluxcalerrLS: optional LIST of FLOAT
        List of FLUXCALERR associated with each SNOBJID being processed, if snobjidLS is non-empty, must match its size
        if snobjidLS is empty, must match the number of gif triplets in stampDir
    CSVdir: optional STRING
        Path to which cnnscores.csv is exported. cnnscores.csv contains column names.
        If CSVdir is not a proper directory, or is left as default value, no csv will be generated.
        
    Returns:
    returnArray: LIST
        Nested LISTS with 3 columns, SNOBJID, CNN_SCORE, CNN_VERSION. List just contains values, does not contain column names.

    Needed Libraries:
        Pytorch
        Numpy
        Pillow (aka PIL)
    As well as any libraries or modules the above are dependent on.
    
    Needs Robert Morgan's Adam Shandonay's CNN Code, specifically the files:
        artifact_cnn.py
        cnn_utils.py
    These can be found at: https://github.com/rmorgan10/DECam_CNN

    Needs both model files (model1.pt and model2.pt). If they can not be found
    in the git_repository, a copy is stored on the FNAL DES computers at:
        /data/des80.b/data/anavarro/CNNstuff/DECam_CNN
    """


    # Prepare full list of all stamp files in the directory    
    for file in os.listdir(stampDir):
        if file.endswith('.gif'):
            fullGifList.append(file)
            
    # Check that PSF, FLUXCAL, and FLUXCALERR lists have expected length
    if len(psfLS) != 0 and len(psfLS) != len(snobjidLS) and len(snobjidLS) != 0:
        sys.exit("PSF and SNOBJID lists do not have same length")
    elif len(psfLS) != 0 and len(snobjidLS) == 0 and len(psfLS) != (len(fullGifList) / 3):
        sys.exit("PSF List length does not match the number of stamp files")
        
    if len(fluxcalLS) != 0 and len(fluxcalLS) != len(snobjidLS) and len(snobjidLS) != 0:
        sys.exit("FLUXCAL and SNOBJID lists do not have same length")
    elif len(fluxcalLS) != 0 and len(snobjidLS) == 0 and len(fluxcalLS) != (len(fullGifList) / 3):
        sys.exit("FLUXCAL List length does not match the number of stamp files")
        
    if len(fluxcalerrLS) != 0 and len(fluxcalerrLS) != len(snobjidLS) and len(snobjidLS) != 0:
        sys.exit("FLUXCALERR and SNOBJID lists do not have same length")
    elif len(fluxcalerrLS) != 0 and len(snobjidLS) == 0 and len(fluxcalerrLS) != (len(fullGifList) / 3):
        sys.exit("FLUXCALERR List length does not match the number of stamp files")

# Prepare list of stamps that will be processed, removing PSF, FLUXCAL, and FLUXCALERR of stamps that can not be found.
    failStamps = []
    if snobjidLS != []:
        for stampVal in snobjidLS:
            if os.path.isfile(os.path.join(stampDir, "srch" + str(int(stampVal)) + ".gif")) and os.path.isfile(os.path.join(stampDir,"temp" + str(int(stampVal)) + ".gif")) and os.path.isfile(os.path.join(stampDir,"diff" + str(int(stampVal)) + ".gif")):
                gifList.append("srch" + str(int(stampVal)) + ".gif")
                gifList.append("temp" + str(int(stampVal)) + ".gif")
                gifList.append("diff" + str(int(stampVal)) + ".gif")
            else:
                failStamps.append(stampVal)
        if failStamps != []:
            print("SNOBJIDs for which stamp files could not be found: ", failStamps)
            print("Removing Stamps that could not be found from processing")
            for item in failStamps:
                failIndex = snobjidLS.index(item)
                if psfLS != []:
                    psfLS.pop(failIndex)
                if fluxcalLS != []:
                    fluxcalLS.pop(failIndex)
                if fluxcalerrLS != []:
                    fluxcalerrLS.pop(failIndex)
                snobjidLS.pop(failIndex)
    else:
        gifList = fullGifList
    
    # Get number of stamp triplets being worked with then prepare initially empty arrays
    stampNum = len(gifList)/3
    if np.floor(stampNum) != np.ceil(stampNum):
        sys.exit("Error: There is not a complete set of .gif Files")
    #print(stampNum)    
    stampArray = np.zeros([int(stampNum),3,51,51])
    idArray = np.repeat('NULL',[int(stampNum)])
    idArray = idArray.tolist()
    returnArray = np.zeros([int(stampNum), 3])
    
    # Populate stamp array
    n = 0
    stampDict = {}

    for stamp in gifList:
        if stamp[4:] in stampDict:
            value = stampDict[stamp[4:]]
        else:
            stampDict[stamp[4:]] = n
            value = n
            n += 1
        if stamp.startswith('srch'):
            img = Image.open(os.path.join(stampDir,stamp))
            ary = np.asarray(img)
            stampArray[value, 0, :, :] = ary
#            print(stamp)
#            print(str(int(stamp[4:-4])))
            idArray[value] = str(int(stamp[4:-4]))
#            print(idArray)
        elif stamp.startswith('temp'):
            img = Image.open(os.path.join(stampDir,stamp))
            ary = np.asarray(img)
            stampArray[value, 1, :, :] = ary
        else:
            img = Image.open(os.path.join(stampDir,stamp))
            ary = np.asarray(img)
            stampArray[value, 2, :, :] = ary

    if psfLS == []:
        psfLS = None
    else:
        psfLS = np.array(psfLS)
    if fluxcalLS == []:
        fluxcalLS = None
    else:
        fluxcalLS = np.array(fluxcalLS)
    if fluxcalerrLS == []:
        fluxcalerrLS = None
    else:
        fluxcalerrLS = np.array(fluxcalerrLS)
    # Run CNN on stamps.
    version = artifact_cnn.__version__
    Eval = artifact_cnn.StampEvaluator(stampArray, model_dir = CNNDIR, psf_array = psfLS, flux_array = fluxcalLS, fluxerr_array = fluxcalerrLS)
    scoreArray = Eval.run()
#    print(idArray)
    returnArray = returnArray.tolist()

    for i in range(len(returnArray)):
        returnArray[i][0] = idArray[i]
        returnArray[i][1] = float(scoreArray[i])
        returnArray[i][2] = version
    # for i in range(len(returnArray)):
    #     returnArray[i][0] = str(int(returnArray[i][0]))
    #     returnArray[i][1] = "{:.5f}".format(returnArray[i][1])
    # Generate CSV
    if os.path.exists(CSVdir):
        colNames = np.array(["SNOBJID"," CNNSCORE"," VERSION"])
        exportArray = np.insert(returnArray, 0, colNames, axis=0)
        np.savetxt(os.path.join(CSVdir, 'cnnscores.csv'), exportArray, delimiter=",", fmt='% s')
    return returnArray

