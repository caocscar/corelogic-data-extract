# -*- coding: utf-8 -*-
"""
Created on Tue Oct 10 09:31:43 2017

@author: caoa
"""

import pandas as pd
import os
import time
from collections import Counter
import json

pd.options.display.max_rows = 20

# Inputs
counties = [12025]
records = 905240 # needs to be calculated
recordtype = 'tax'
assert recordtype in {'foreclosure','tax','deed'}

#%%
with open('recordtype.json','r') as fin:
    parcel = json.load(fin)
    
for fips in counties:
    fname = '{}_{}.txt'.format(fips,recordtype)
    if os.path.isfile(fname):
        os.remove(fname)

#%%
fips_col = 'FIPS CODE' if recordtype == 'tax' else 'FIPS'
parcelinfo = parcel[recordtype]
wdir = parcelinfo['directory']
parcel_file = parcelinfo['filename']
columns = parcelinfo['columns']
    
f = pd.read_csv('fips_ordering_{}.csv'.format(recordtype))
rows_of_interest = f[f['FIPS'].isin(counties)]
skiprows = rows_of_interest['first_row'].min()
cs = 200000

t0 = time.time()
fin = pd.read_csv(os.path.join(wdir, parcel_file), quoting=3, sep='|', chunksize=cs, skiprows=skiprows, header=None)

#%%
t1 = time.time()
print('Skipping {:,} rows took {:.2f} mins'.format(skiprows,(t1-t0)/60) )
c = Counter()
hitherto = 0
for i, df in enumerate(fin):
    if i%5 == 0:
        print(i*0.2, time.time()-t0)
    df.columns = columns
    data = df[df[fips_col].isin(counties)]
    if not data.empty:
        print(i, data.shape[0])
        c.update(data[fips_col])
        for fips, df_county in data.groupby(fips_col):
            fname = '{}_{}.txt'.format(fips,recordtype)
            header_flag = False if os.path.isfile(fname) else True
            with open(fname, 'a') as fout:
                df_county.to_csv(fout, index=False, sep='|', quoting=3, header=header_flag, encoding='utf-8')
        hitherto += data.shape[0]
        if hitherto >= records:
            break
t2 = time.time()
print('Process time {:.1f} mins with chunksize={}'.format((t2-t1)/60,cs))
print('Processed lines per second: {:,.0f}'.format(i*cs/(t2-t1)) )
print('Records Extracted = {:,}'.format(hitherto))

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
    filename = '{}_{}.txt'.format(fips,recordtype)
    minify_file(filename)
t5 = time.time()
print('minification: {:.1f}s'.format(t5-t4) )
    