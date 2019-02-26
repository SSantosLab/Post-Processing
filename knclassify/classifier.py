# A code to select KN candidates

import pandas as pd
import numpy as np
import sys
import os

from sklearn.model_selection import train_test_split
from sklearn.model_selection import GridSearchCV
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import confusion_matrix

#Make the interface look pretty
os.system('clear')
print "*************************************************************************"
print "*                       Welcome to KN-Classify!                         *"
print "*                                                                       *"
print "*         Written by Robert Morgan, robert.morgan@wisc.edu              *"
print "*************************************************************************\n"
print "Overview: "
print "  This program is designed to select kilonovae light curves from DES"
print "  DiffImg outputs. It uses a Random Forest Classifier from Sci-Kit Learn.\n"
print "Operations:"
print "  The rapid follow-up for the LIGO trigger should have had one of four "
print "  possible cadences. Please note the code that applies to yours below:"
print "            |          Cadence            |      Code     |"
print "            |-----------------------------|---------------|"
print "            | One epoch in i and r        |      ir2      |"
print "            | One epoch in i and z        |      iz2      |"
print "            | Two epochs in i and r       |      ir4      |"
print "            | Two epochs in i and z       |      iz4      |"
print "            |---------------------------------------------|"
print "  Based on your cadence, KN-Classify will create a model for selecting "
print "  KN light curves and rejecting background light curves. All light "
print "  curves are simulated using SNANA.\n"
print "Requirements:"
print "  File structure---------"
print "     * Make a directory in the current working directory (i.e. datadir)"
print "     * Make a subdirectory named 'data' inside 'datadir'"
print "     * Put all .DAT files inside 'datadir/data'"
print "     * Put a list file of .DAT files (i.e. datadir.LIST) inside 'datadir'"
print "     *** Look at 'testdir_XXX' to see an example"
print "  System requirements-----"
print "     * This script has been developed and tested using:"
print "          python 2.7.14"
print "          numpy 1.13.3"
print "          pandas 0.22.0"
print "          sklearn 0.19.1"
print "*************************************************************************"

#Select cadence mode
mode = raw_input("Please enter the code corresponding to your cadence: ")
while mode not in ['ir2', 'iz2', 'ir4', 'iz4']:
    print "The code entered is not from the table. Try again..."
    mode = raw_input("Please enter the code corresponding to your cadence: ")

#Collect training data
print "Reading training data..."
sim_sig = pd.read_csv('training_data_kn_sig.csv', index_col=0)
sim_sig['KN'] = 1
sim_bkg = pd.read_csv('training_data_kn_bkg.csv', index_col=0)
sim_bkg['KN'] = 0
train_df = pd.concat([sim_sig, sim_bkg])

#encode categorical values
train_df['BRIGHTEST_i'] = train_df['BRIGHTEST'] == 'i'
train_df['BRIGHTEST_r'] = train_df['BRIGHTEST'] == 'r'
train_df['BRIGHTEST_z'] = train_df['BRIGHTEST'] == 'z'
train_df = train_df.drop(['BRIGHTEST'], axis=1)

#separte into different observing conditions
ir_2_df = train_df[(train_df['NOBS'] == 2) & (train_df['BANDS'] == 'ir')]
ir_4_df = train_df[(train_df['NOBS'] == 4) & (train_df['BANDS'] == 'ir')]
iz_2_df = train_df[(train_df['NOBS'] == 2) & (train_df['BANDS'] == 'iz')]
iz_4_df = train_df[(train_df['NOBS'] == 4) & (train_df['BANDS'] == 'iz')]


numerical_columns = ['BRIGHTEST_r', 'BRIGHTEST_i', 'BRIGHTEST_z', 'RISE_r', 'RISE_i', 'RISE_z',
                     'MAX_r-i', 'MIN_r-i', 'MAX_i-z', 'MIN_i-z', 'MAX_SNR_z', 'MAX_SNR_i', 'MAX_SNR_r',
                     'CUT_1', 'CUT_2', 'CUT_3']

#Choose mode
if mode == 'ir2':
    y = ir_2_df['KN'].values
    X_df  = ir_2_df[numerical_columns]
    feats = X_df.columns
    X = X_df.values
elif mode == 'ir4':
    y = ir_4_df['KN'].values
    X_df  = ir_4_df[numerical_columns]
    feats = X_df.columns
    X =	X_df.values
elif mode == 'iz2':
    y = iz_2_df['KN'].values
    X_df  = iz_2_df[numerical_columns]
    feats = X_df.columns
    X =	X_df.values
elif mode == 'iz4':
    y = iz_4_df['KN'].values
    X_df  = iz_4_df[numerical_columns]
    feats = X_df.columns
    X = X_df.values
else:
    print "Invalid mode. Choose from ir2, ir4, iz2, or iz4"
    #This case should never happen
    sys.exit()
    
#Split into training and testing data
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, stratify=y)

#Train classifier
print "Fitting classifier..."
rfc = RandomForestClassifier(n_estimators=1000, criterion='gini', max_depth=50, n_jobs=-1)
"""
rfc.fit(X_train, y_train)

#Determine preformance on testing set
y_test_pred = rfc.predict(X_test)
cmat = confusion_matrix(y_test, y_test_pred)
"""
#fit classifier to full dataset
rfc.fit(X, y)


##################################################################################################
# Load real data
data_dir = raw_input("\nThe classifier has been trained. KN-Classify is ready for the real data.\n" + \
                     "Keep in mind your directory must have the following structure:\n datadir/, datadir/data/, datadir/datadir.LIST, datadir/data/*.DAT" + \
                     "\n\nEnter name of datadir: ")

#Extract features
print "Analyzing DiffImg data..."
#os.system('pythonw features.py %s kn_data.csv' %data_dir)  #for MacOSX
os.system('python features.py %s kn_data.csv' %data_dir)
data_df = pd.read_csv('kn_data.csv', index_col=0)

#encode categorical values
data_df['BRIGHTEST_i'] = data_df['BRIGHTEST'] == 'i'
data_df['BRIGHTEST_r'] = data_df['BRIGHTEST'] == 'r'
data_df['BRIGHTEST_z'] = data_df['BRIGHTEST'] == 'z'
data_df = data_df.drop(['BRIGHTEST'], axis=1)

#select data relevent to classifier
data_X_df = data_df[numerical_columns]
data_X = data_X_df.values

#make predictions using classifier
data_y_pred = rfc.predict(data_X)
data_y_prob = rfc.predict_proba(data_X)
prob_kn = data_y_prob[:,1]

#scale probabilities using accuracy from confusion matrix
#acc = float(cmat[0,0] + cmat[1,1]) / np.sum(cmat)  # (TP + TN) / (TP + FP + TN + FN)
#prob_kn = prob_kn * acc

#Include probabilities into dataframe, sort by probability
data_df['PROB_KN'] = prob_kn
sorted_data_df = data_df.sort_values('PROB_KN', ascending=False)

#Output reports
report = open('KN_Report.csv', 'w+')
print >> report, "FILENAME,RA,DEC,CCD,MAG_r,MAG_i,MAG_z,EXPNUM_1,EXPNUM_2,EXPNUM_3,EXPNUM_4,PROB_KN"
for index, row in sorted_data_df.iterrows():
    list_output = [row['FILENAME'], str(row['RA']), str(row['DEC']), str(row['CCD']), str(row['MAG_r']),
                   str(row['MAG_i']), str(row['MAG_z']), str(row['EXPNUM_1']), str(row['EXPNUM_2']), 
                   str(row['EXPNUM_3']), str(row['EXPNUM_4']), str(row['PROB_KN'])]
    str_output = ','.join(list_output)
    print >> report, str_output
report.close()

sorted_data_df_cuts = sorted_data_df[(sorted_data_df['CUT_1']) & (sorted_data_df['CUT_2']) & (sorted_data_df['CUT_3'])]
report = open('KN_Report_cuts.csv', 'w+')
print >> report, "FILENAME,RA,DEC,CCD,MAG_r,MAG_i,MAG_z,EXPNUM_1,EXPNUM_2,EXPNUM_3,EXPNUM_4,PROB_KN"
for index, row in sorted_data_df_cuts.iterrows():
    list_output = [row['FILENAME'], str(row['RA']), str(row['DEC']), str(row['CCD']), str(row['MAG_r']),
                   str(row['MAG_i']), str(row['MAG_z']), str(row['EXPNUM_1']), str(row['EXPNUM_2']),
                   str(row['EXPNUM_3']), str(row['EXPNUM_4']), str(row['PROB_KN'])]
    str_output = ','.join(list_output)
    print >> report, str_output
report.close()

#Say goodbye
print "KN-Classify found %i potential KN candidates. A full report is available " %data_df[data_df['PROB_KN'] > 0.8].shape[0]
print "here:"
print "           KN_Report.csv\n"
print "A full report after cuts have been placed on the data is available here:"
print "           KN_Report_cuts.csv\n"
print "*************************************************************************"
print "*                   Thank you for using KN-Classify!                    *"
print "*                                                                       *"
print "*         Written by Robert Morgan, robert.morgan@wisc.edu              *"
print "*************************************************************************\n\n"

