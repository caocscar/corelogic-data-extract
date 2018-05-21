# -*- coding: utf-8 -*-
"""
Created on Tue Oct 10 09:31:43 2017

@author: caoa
"""

import pandas as pd
import os
import time
from collections import Counter

pd.options.display.max_rows = 20

# Inputs
counties = [6001,6067]
records = 452696+467796 # needs to be calculated

#%%
parcel = {}
parcel['foreclosure'] = {'directory':r'\\ulib-licensed-content.m.storage.umich.edu\ulib-licensed-content\corelogic',
                         'filename':'Michigan_Uni_FCL_AKZA_85HRYY_Data.zip',
                         'lines':37487908,
                         }
parcel['tax'] = {'directory':r'\\ulib-licensed-content.m.storage.umich.edu\ulib-licensed-content\corelogic',
                 'filename':'Michigan_Uni_Tax_AKZA_85HRQ5.zip',
                 'lines':150014715,
                 }
parcel['deed'] = {'directory':r'X:\ParcelData',
                  'filename':'University_Michigan_Deed_KZA_85HRZB_clean3.txt',
                  'lines':367782480,
                  }

recordtype = 'tax'
fips_col = 'FIPS CODE' if recordtype == 'tax' else 'FIPS'
parcelinfo = parcel[recordtype]
wdir = parcelinfo['directory']
parcel_file = parcelinfo['filename']
    
cs = 200000
fin = pd.read_csv(os.path.join(wdir, parcel_file), quoting=3, sep='|', chunksize=cs)

#%%
t1 = time.time()
c = Counter()
hitherto = 0
for i, df in enumerate(fin):
    if i%5 == 0:
        print(i*0.2, time.time()-t1)
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
print('Process Time {:.1f} mins with chunksize={}'.format((t2-t1)/60,cs))
print('Lines per second: {:.0f}'.format(i*cs/(t2-t1)) )
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
    