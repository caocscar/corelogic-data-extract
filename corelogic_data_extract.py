# -*- coding: utf-8 -*-
"""
Created on Tue Oct 10 09:31:43 2017

@author: caoa
"""

import pandas as pd
import os
import time
import json
from collections import Counter

pd.options.display.max_rows = 20

# Inputs
counties = [26017,26063,26075,26087,26091,26093,26115,26145,26147,26151,26157]
records = [51859,32986,82270,44407,52129,84674,68207,95896,81653,31256,35494] # needs to be calculated
records = sum(records)
recordtype = 'tax'
assert recordtype in {'foreclosure','tax','deed'}

#%% load parcel information
with open('recordtype.json','r') as fin:
    parcel = json.load(fin)
# remove existing files since we will be appending to them
for fips in counties:
    fname = f'{fips}_{recordtype}.txt'
    if os.path.isfile(fname):
        os.remove(fname)

#%%
fips_col = 'FIPS CODE' if recordtype == 'tax' else 'FIPS'
parcelinfo = parcel[recordtype]
wdir = parcelinfo['directory']
parcel_file = parcelinfo['filename']
columns = parcelinfo['columns']
# calculate what row we can start on (for efficiency)    
f = pd.read_csv(f'fips_ordering_{recordtype}.csv')
matching_rows = f[f['FIPS'].isin(counties)]
skiprows = matching_rows['first_row'].min()
chunksize = 200000

t0 = time.time()
fin = pd.read_csv(os.path.join(wdir, parcel_file), quoting=3, sep='|', chunksize=chunksize, skiprows=skiprows, header=None)

#%%
t1 = time.time()
print(f'Skipping {skiprows:,} rows took {(t1-t0)/60:.1f} mins')
c = Counter()
hitherto = 0
for i, df in enumerate(fin):
    if i%5 == 0:
        print(i*chunksize/1000000, time.time()-t0)
    df.columns = columns
    data = df[df[fips_col].isin(counties)]
    if not data.empty:
        print(i, data.shape[0])
        c.update(data[fips_col])
        for fips, df_county in data.groupby(fips_col):
            fname = f'{fips}_{recordtype}.txt'
            header_flag = False if os.path.isfile(fname) else True
            with open(fname, 'a') as fout:
                df_county.to_csv(fout, index=False, sep='|', quoting=3, header=header_flag, encoding='utf-8')
        hitherto += data.shape[0]
        if hitherto >= records:
            break
t2 = time.time()
print(f'Process time {(t2-t1)/60:.1f} mins with chunksize={chunksize}')
print(f'Processed lines per second: {i*chunksize/(t2-t1):,.0f}')
print(f'Records Extracted = {hitherto:,}')

#%%
# convert float to ints, replace True/False with 1/0
def minify_file(filename):
    with open('tmpfile.txt','w') as fout:
        with open(filename,'r') as fin:
            for line in fin:
                newline = line.replace(".0|", "|").replace("True","1").replace("False","0")
                fout.write(newline)
    os.remove(filename)
    os.rename('tmpfile.txt',filename)

#%%
t4 = time.time()
for fips in counties:
    filename = f'{fips}_{recordtype}.txt'
    minify_file(filename)
t5 = time.time()
print(f'minification: {t5-t4:.1f}s')
    