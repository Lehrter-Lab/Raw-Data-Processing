## CMikolaitis @ Lehrter Lab, DISL

import pandas as pd
import os

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
    df = pd.read_csv(inFile, delimiter='\t',skiprows=13)
    df = df[df.Excluded == 0] # Clean flagged reads
    df = df[~df['Sample Name'].str.contains('Rinse')]
    gb  = df.groupby('Anal.') # Groupby analytes
    dfs = [gb.get_group(x) for x in gb.groups]
    for i in range(len(dfs)): # Remove excess standards
        stds      = dfs[i]['Cal. Curve'].unique().tolist()
        realstds  = stds[-1]
        dfs[i] = dfs[i][dfs[i].Origin.str.contains(realstds,regex=False) |
                        dfs[i]['Cal. Curve'].str.contains(realstds,regex=False)]
    return dfs
##-----------------------------------------------------------------------------
## Do the work
a = parsePP('4.8.2024_PP RAW DATA_NOAA RESTORE_AB.xlsx')