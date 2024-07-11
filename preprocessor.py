## CMikolaitis @ Lehrter Lab, DISL

import pandas as pd
import os
import numpy as np
from sklearn.linear_model import LinearRegression

## For my sanity
pd.options.mode.copy_on_write = True
##-----------------------------------------------------------------------------
## Move stuff around
def pullIn(inFile):
    if inFile.endswith('.xls') or inFile.endswith('.xlsx'):
        df = pd.read_excel(inFile)
    else:
        try:
            df = pd.read_csv(inFile, delimiter='\t',skiprows=13)
        except:
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

def buildMatrix(inputDirs,inputFuncs):
    dfs = {}
    for i in range(len(inputDirs)):
        try:
            key      = inputDirs[i].replace('input', '')
            dfs[key] = maketheDF(inputDirs[i],inputFuncs[i])
        except:
            print("Error in: ",inputDirs[i])
    return dfs

def buildFinal(inputDict,outname,outpath):
    # Match station ids to loc
    df = {}
    for key, item in inputDict.items():
        temp = pd.concat(item.values(),ignore_index=True)
        df[key] = temp
    with pd.ExcelWriter(outpath) as writer:
        for key in df:
            df[key].to_excel(writer, sheet_name=key,index = False)
    return df
##-----------------------------------------------------------------------------
## Handle data
## Cleans but does not QC

## Add in R-squareds as proxy for QC
## Need function for PP cal curve

## Don't touch yet
def parseStations(inFile):
    df = pullIn(inFile)
    df.dropna(thresh=4,inplace=True) # cut rows with less than 2 values
    df.dropna(thresh=4,axis=1,inplace=True) # drop most note cols
    df.columns = df.iloc[0] # Fix header
    df = df[1:] # Drop inline header
    try:
        df['StationID'] = df['Line']+df['Letter']+'-'+df['Number'].astype(str)
    except:
        pass
    df['Raw File'] = inFile
    return df

def parsePP(inFile):
    df = pullIn(inFile)
    stds = df[df.Sample.astype(str).str.contains('.',regex=False)]
    stds = stds[~pd.to_numeric(stds.Sample,errors='coerce').isnull()]
    df['Raw File'] = inFile
    return df

def parsePCN(inFile):
    df = pullIn(inFile)
    for index, row in df.iterrows():
        temp = row.astype(str).str.isnumeric()
        if any(temp):
            break
    if index > 0:
        currentH = df.columns.to_list()
        toAdd    = df.iloc[index-1].to_list()
        a        = ['' if pd.isnull(x) else ' ' + x for x in toAdd]
        newH     = [m+str(n) for m,n in zip(currentH,a)]
        df.columns = newH
        df.drop(index-1,inplace=True)
    df['Raw File'] = inFile
    return df

def parseNUT(inFile):
    dropCols = ['NeedleNumber','ResultID','Position','SampleType','SampleIdentity']
    df = pullIn(inFile)
    # Match what columns to keep
    realCols = df.columns
    analCols = ['NO3 NO2','PO4','NO2','NH4','D Si']
    realAnal = [val for val in realCols if any(anal in analCols for anal in analCols)]
    df.columns = realAnal
    # Drop empty cols and rename to Sample Id
    df.dropna(axis=1,how='all',inplace=True)
    sidCol = df.columns.get_loc('NeedleNumber')-1
    df = df.rename(columns={df.columns[sidCol]:'SampleID'})
    df = df.rename(columns=lambda x: x.strip())
    df = df[df.columns.drop(list(df.filter(regex='Unnamed')))]
    # # Get r-squared
    # stds = df[df['SampleType'].str.contains('S')]
    # # Get drift
    # drift = df[df['SampleType'].str.contains('D')]
    # Final Clean
    df = df.drop(dropCols,axis=1) # remove filled columns
    df = df.dropna(subset=['SampleID'])
    df['Raw File'] = inFile
    return df

def parseDICTNDOC(inFile):
    cleanDFs,drift = {},{}
    keepCols       = ['Sample Name','Conc.']
    originalCols   = ['Type','Anal.','Sample Name','Sample ID','Origin',
                      'Cal. Curve','Manual Dilution','Notes','Date / Time',
                      'Spl. No.','Inj. No.','Analysis(Inj.)','Area','Conc.',
                      'Result','Excluded','Inj. Vol.']
    checkNames     = ['QC','Q','L','H'] # Possible check 'Sample Names'
    checkIDsHigh   = ['H']              # Possible high check 'Sample IDs'
    df = pullIn(inFile)
    drift
    # Handle column names
    neededfill = len(df.columns)-len(originalCols) # get num of columns
    if neededfill > 0: #fill to the left if not enough column names
        fill = ['potato']*neededfill
        originalCols.extend(fill)
        df.columns = originalCols # rename columns
        df = df.drop('potato',axis=1) # remove filled columns
    else:
        df.columns = originalCols # rename columns
    df = df[df.Excluded == 0] # Clean flagged reads
    try:
        df = df[(~df['Sample Name'].str.contains('Rinse',na=False)) & 
                (~df['Sample ID'].str.contains('Rinse',na=False))]
    except:
        df = df[~df['Sample Name'].str.contains('Rinse',na=False)]
    ## Split df into dict of dfs based on analyte groups
    df['grouper'] = np.where(df['Type'].eq('Unknown'),df['Cal. Curve'],df['Origin'])
    gb  = df.groupby('grouper') # Groupby analytes
    dfs = [gb.get_group(x) for x in gb.groups]
    cleaned=[]
    for i in range(len(dfs)):
        # Split stds from values
        stds      = dfs[i]['Cal. Curve'].unique().tolist()
        realstds  = stds[-1]
        stdAreas  = dfs[i][dfs[i]['Origin'].eq(realstds)]
        dfs[i]    = dfs[i][dfs[i]['Cal. Curve'].notna()]
        # Get mean concentrations
        cleanDFs[i] = dfs[i].filter(keepCols, axis=1)
        if cleanDFs[i].empty:
            pass
        else:
            cleanDFs[i] = cleanDFs[i][~cleanDFs[i]['Sample Name'].isin(checkNames)]
            cleanDFs[i] = cleanDFs[i].groupby("Sample Name").mean().reset_index()
            # Do linear regression
            try:
                x = stdAreas[['Conc.']]
                y = stdAreas['Area']
                model = LinearRegression()
                model.fit(x, y)
                r2_score = model.score(x, y)
                cleanDFs[i]['r-sq.'] = r2_score
                highStd = max(x['Conc.'])
                checkIDsHigh.append(str(int(highStd)))
            except:
                cleanDFs[i]['r-sq.'] = "No curve available"
            # Get drift
            drift = dfs[i][(dfs[i]['Sample ID'].isin(checkIDsHigh)) | 
                           (dfs[i]['Sample Name'].isin(checkIDsHigh))]
            drift = drift[drift['Conc.'] > 1.5] # Scrub empty vials
            absDiff = (highStd - drift['Conc.']).abs().max()
            cleanDFs[i]['Max % Abs. Diff of High Check'] = absDiff/highStd*100
            # Add Reference
            newname = 'Conc. ' + dfs[i]['Analysis(Inj.)'].unique()
            cleanDFs[i].rename(columns={'Conc.':newname[0]},inplace=True)
            cleanDFs[i]['Raw File'] = inFile
    cleanDFs = {k:v for (k,v) in cleanDFs.items() if not v.empty}
    cleaned = pd.concat(cleanDFs,ignore_index=True)
    return cleaned
##-----------------------------------------------------------------------------
## Input Options
## One directory for each: PP, PCN, Nutrients, DIC, TN/DOC
inputPP     = 'PP'
inputPCN    = 'PCN'
inputDIC    = 'DIC'
inputTNDOC  = 'TNDOC'
inputNUT    = 'NUT'

inputDirs   = [inputPCN,inputDIC,inputTNDOC,inputNUT]
inputFuncs  = [parsePCN,parseDICTNDOC,parseDICTNDOC,parseNUT]
##-----------------------------------------------------------------------------
## Do the work
a = buildMatrix(inputDirs,inputFuncs)
b = buildFinal(a,'potato','product.xlsx')
#a = parseDICTNDOC('TNDOC\Chris TNDOC 012624 Detail.txt')
