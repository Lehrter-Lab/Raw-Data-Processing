## CMikolaitis @ Lehrter Lab, DISL

import pandas as pd
import os
import numpy as np
from sklearn.linear_model import LinearRegression

## For my sanity
pd.options.mode.copy_on_write = True
##-----------------------------------------------------------------------------
## Input Options
## One directory for each: PP, PCN, Nutrients, DIC, TN/DOC
inputPP     = 'PP'
inputPCN    = 'PCN'
inputDIC    = 'DIC'
inputTNDOC  = 'TNDOC'
inputNUT    = 'NUT'

inputDirs   = [inputPP,inputPCN,inputDIC,inputTNDOC,inputNUT]
##-----------------------------------------------------------------------------
## Move stuff around
def pullIn(inFile):
    if inFile.endswith('.xls') or inFile.endswith('.xlsx'):
        df = pd.read_excel(inFile)
    else:
        df = pd.read_csv(inFile)
    df.dropna(thresh=2,inplace=True) # cut rows with less than 2 values
    return df

def maketheDF(directory,analFunc):
    dfs = {}
    home = os.getcwd()
    path = os.path.join(home,directory)
    for file in os.listdir(directory):
        pathF = os.path.join(path,file)
        try:
            df = analFunc(pathF)
            dfs[file] = df
        except:
            print(pathF)
    return dfs

def buildMatrix():
    dfs = {}
    for folder in inputDirs:
        dfs[folder] = maketheDF(folder)
    return dfs

##-----------------------------------------------------------------------------
## Handle data
## Cleans but does not QC

## Add in R-squareds as proxy for QC
## Need function for PP cal curve

def parsePP(inFile):
    df = pullIn(inFile)
    stds = df[df.Sample.astype(str).str.contains('.',regex=False)]
    stds = stds[~pd.to_numeric(stds.Sample,errors='coerce').isnull()]
    return df

def parsePCN(inFile):
    colNames = ['SampleID','SampleWeight','N-Response','N-mg','N-percent',
                   'C-Response','C-mg','C-percent']
    df = pullIn(inFile)
    neededfill = len(df.columns)-len(colNames) # get num of columns
    if neededfill > 0: #fill to the left if not enough column names
        fill = ['potato']*neededfill
        fill.extend(colNames)
        colNames = fill
    df.columns = colNames # rename columns
    df = df.drop('potato',axis=1) # remove filled columns
    df = df[pd.to_numeric(df['N-mg'], errors='coerce').notnull()] #drop header2
    
    return df

def parseNUT(inFile):
    colNames = ['SampleID','NeedleNumber','ResultID','Position','SampleType',
                   'SampleIdentity','NO3+NO2','PO4','NO2','NH4','dSi']
    dropCols = ['NeedleNumber','ResultID','Position','SampleType',
                   'SampleIdentity']
    df = pullIn(inFile)
    diff = len(df.columns)-len(colNames)
    if diff != 0: # remove Si if not needed
        colNames = colNames[:-1]
    df.columns = colNames # rename columns
    df = df.drop(dropCols,axis=1) # remove filled columns
    return df

def parseDICTNDOC(inFile):
    cleanDFs,drift = {},{}
    keepCols = ['Sample Name','Conc.']
    df = pd.read_csv(inFile, delimiter='\t',skiprows=13)
    df = df[df.Excluded == 0] # Clean flagged reads
    df = df[(~df['Sample Name'].str.contains('Rinse')) & 
            (~df['Sample ID'].str.contains('Rinse',na=False))]
    gb  = df.groupby('Anal.') # Groupby analytes
    dfs = [gb.get_group(x) for x in gb.groups]
    for i in range(len(dfs)):
        # Split stds from values
        stds      = dfs[i]['Cal. Curve'].unique().tolist()
        realstds  = stds[-1]
        print(realstds)
        stdAreas  = dfs[i][dfs[i]['Origin'].str.contains(realstds,regex=False)]
        dfs[i]    = dfs[i][dfs[i]['Cal. Curve'].notna()]
        # Get mean concentrations
        cleanDFs[i] = dfs[i].filter(keepCols, axis=1)
        cleanDFs[i] = cleanDFs[i][~cleanDFs[i]['Sample Name'].str.contains('QC')]
        cleanDFs[i] = cleanDFs[i].groupby("Sample Name").mean().reset_index()
        # Do linear regression
        x = stdAreas[['Conc.']]
        y = stdAreas['Area']
        model = LinearRegression()
        model.fit(x, y)
        r2_score = model.score(x, y)
        cleanDFs[i]['r-sq.'] = r2_score
        # Get drift
        drift = dfs[i][dfs[i]['Sample ID'].str.contains('5',na=False)]
        drift = drift[drift['Conc.'] > 1.5] # Scrub empty vials
        if '20' in drift['Sample ID'].unique():
            cleanDFs[i]['Max Deviation of High Check (%)'] = (20 - drift['Conc.'].min())/5*100
        else:
            cleanDFs[i]['Max Deviation of High Check (%)'] = (5 - drift['Conc.'].min())/5*100
        # Add Reference 
        cleanDFs[i]['Raw File'] = inFile
    return cleanDFs
##-----------------------------------------------------------------------------
## Do the work
a = parseDICTNDOC('Chris TNDOC 012424 Detail.txt')
