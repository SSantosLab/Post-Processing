#!/usr/bin/env python
""" 
Chris D'Andrea, Ravi Gupta
21 July 2015

The routines hereing are called by the desHostMatch.py script, which the directional
light radius (DLR) to a galaxy from an RA and DEC given either
    a.  Second moments (get_DLR)
    b.  A_IMAGE, B_IMAGE, THETA_IMAGE (get_DLR_ABT)
    
v0.1:  July 2015   -- Split off host-match functions into a separate script to be imported.
                      Makes swapping out the modules simpler.
v0.2:  June 2016   -- Add function to compute HOST_CONFUSION from DLR (Ravi)
"""

import sys
import os
import numpy as np
import math as m

#####################   DEFINE GLOBAL VARIABLES   #################################################

rad  = np.pi/180                   # convert deg to rad
pix_arcsec = 0.264                 # pixel scale (arcsec per pixel)
pix2_arcsec2 = 0.264**2            # pix^2 to arcsec^2 conversion factor
pix2_deg2 = pix2_arcsec2/(3600**2) # pix^2 to deg^2 conversion factor


####################### Function to compute DLR distance from 2nd moments #########################

def get_DLR(RA_SN, DEC_SN, RA, DEC, X2, Y2, XY, angsep):
    # inputs are arrays

    global numFailed 
    rPHI = np.empty_like(angsep)
    d_DLR = np.empty_like(angsep)

    # convert from IMAGE units (pixels^2) to WORLD (arcsec^2)
    X2_ARCSEC = X2*pix2_arcsec2
    Y2_ARCSEC = Y2*pix2_arcsec2
    XY_ARCSEC = XY*pix2_arcsec2

    dX2Y2 = X2_ARCSEC - Y2_ARCSEC  # difference
    sX2Y2 = X2_ARCSEC + Y2_ARCSEC  # sum

    # ensures not all 2nd moments are same & arctan is not undefined later
    good = ( (dX2Y2 + sX2Y2 -2*XY_ARCSEC != 0) &
             ((np.fabs(XY_ARCSEC) > 1.e-6) | (np.fabs(dX2Y2) > 1.e-6)) )
    bad = np.invert(good)
    
    # spatial rms of object profile along semi-major axis.  If bad, set to 0.0
    A_ARCSEC = np.where(good, np.sqrt(0.5*sX2Y2 + np.sqrt((0.5*dX2Y2)**2 + XY_ARCSEC**2)), 0.0)
    # spatial rms of object profile along semi-minor axis.  If bad, set to 0.0
    B_ARCSEC = np.where(good, np.sqrt(0.5*sX2Y2 - np.sqrt((0.5*dX2Y2)**2 + XY_ARCSEC**2)), 0.0)

    # if bad, the default THETA is 0.0
    THETA = np.where(good, 0.5*np.arctan2(2.0*XY_ARCSEC, dX2Y2), 0.0)
    # angle between RA-axis and SN-host vector
    GAMMA = np.arctan((DEC_SN - DEC)/(np.cos(DEC_SN*rad)*(RA_SN - RA)))
    # angle between semi-major axis of host and SN-host vector
    PHI = THETA + GAMMA

    rPHI = np.where(good, A_ARCSEC*B_ARCSEC/np.sqrt((A_ARCSEC*np.sin(PHI))**2 +
                                                    (B_ARCSEC*np.cos(PHI))**2), 0.0)

    # directional light radius
    #  where 2nd moments are bad, set d_DLR = 99.99
    d_DLR = np.where(good, angsep/rPHI, 99.99) 

    return [d_DLR, A_ARCSEC, B_ARCSEC, rPHI]

######################## Function to compute DLR distance from ####################################
######################## A_IMAGE, B_IMAGE, THETA_IMAGE ############################################

def get_DLR_ABT(RA_SN, DEC_SN, RA, DEC, A_IMAGE, B_IMAGE, THETA_IMAGE, angsep):
    # inputs are arrays

    global numFailed 
    rPHI = np.empty_like(angsep)
    d_DLR = np.empty_like(angsep)
 
    # convert from IMAGE units (pixels) to WORLD (arcsec^2)
    A_ARCSEC = A_IMAGE*pix_arcsec
    B_ARCSEC = B_IMAGE*pix_arcsec
 
    # angle between RA-axis and SN-host vector
    GAMMA = np.arctan((DEC_SN - DEC)/(np.cos(DEC_SN*rad)*(RA_SN - RA)))
    # angle between semi-major axis of host and SN-host vector
    PHI = THETA_IMAGE + GAMMA # angle between semi-major axis of host and SN-host vector

    rPHI = A_ARCSEC*B_ARCSEC/np.sqrt((A_ARCSEC*np.sin(PHI))**2 +
                                     (B_ARCSEC*np.cos(PHI))**2)

    # directional light radius
    #  where 2nd moments are bad, set d_DLR = 99.99
    d_DLR = angsep/rPHI  

    return [d_DLR, A_ARCSEC, B_ARCSEC, rPHI]

################### Function to compute host confusion from DLR  ##################################

def compute_HC(DLR):
    dlr = np.sort(DLR) # first sort DLR array
    e = 1e-5 # define epsilon, some small number
    if len(dlr)==1: # only 1 object in radius
        HC = -99.0
    else:
        delta12 = dlr[1] - dlr[0] + e
        D1D2 = dlr[0]**2/dlr[1] + e
        Dsum = 0
        for i in range(0, len(dlr)):
            for j in range(i+1, len(dlr)):
                didj = dlr[i]/dlr[j] + e
                delta_d = dlr[j] - dlr[i] + e
                Dsum += didj/((i+1)**2*delta_d)
        HC = m.log10(D1D2*Dsum/delta12)
    return HC

###############################   MAIN   ##########################################################

