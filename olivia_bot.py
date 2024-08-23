import pandas as pd

## For my sanity
pd.options.mode.copy_on_write = True

# Make df for import to xlsx
a     = pd.read_excel('master.xlsx')
a.rename(columns={'Sample Name':'Sample ID',"Conc. TN":"TN","Conc. NPOC":'DOC'},inplace=True)
a['Sample ID'] = a['Sample ID'].astype(str)
NPOC  = a.dropna(subset=['DOC'])
NPOC.drop(columns=['TN','r-sq.','Raw File','Max % Abs. Diff of High Check'],inplace=True)
TN    = a.dropna(subset=['TN'])
TN.drop(columns=['DOC','r-sq.','Raw File','Max % Abs. Diff of High Check'],inplace=True)
# Merge
NPOC.set_index('Sample ID',inplace=True)
TN.set_index('Sample ID',inplace=True)
b = NPOC.merge(TN,how='outer', on='Sample ID')
b.to_excel('sheety.xlsx')

# Import Master
c = pd.read_excel('Restore Master Data 2020-2024 v3.xlsx', sheet_name='TN_DOC')
c['Sample ID'] = c['Sample ID'].astype(str)
c.set_index('Sample ID',inplace=True)
c['TN']  = pd.to_numeric(c['TN'], errors='coerce')
c['DOC'] = pd.to_numeric(c['DOC'], errors='coerce')

# Merge
e        = c.merge(b,on='Sample ID',how='left',suffixes=('', '_y'))
e['TN']  = e['TN'].fillna(e['TN_y'])
e['DOC'] = e['DOC'].fillna(e['DOC_y'])
e.drop(columns=['DOC_y','TN_y'],inplace=True)
e.to_excel("output.xlsx")
