# -*- coding: utf-8 -*-
"""
Created on Tue Oct 10 09:31:43 2017

@author: caoa
"""

import pandas as pd
import os
import time

pd.options.display.max_rows = 20

# Inputs
counties = [26081,26049]
records = 4119979 # needs to be calculated

#%%
parcel = {}
parcel['tax'] = {'directory':r'\\ulib-licensed-content.m.storage.umich.edu\ulib-licensed-content\corelogic',
                 'filename':'Michigan_Uni_Tax_AKZA_85HRQ5.zip',
                 'lines':150014715,
                 }
parcel['foreclosure'] = {'directory':r'\\ulib-licensed-content.m.storage.umich.edu\ulib-licensed-content\corelogic',
                         'filename':'Michigan_Uni_FCL_AKZA_85HRYY_Data.zip',
                         'lines':37487908,
                         }						 
parcel['deed'] = {'directory':r'X:\ParcelData',
                  'filename':'University_Michigan_Deed_KZA_85HRZB_clean3.txt',
                  'lines':367782480,
                  }

recordtype = 'tax'
fips_col = 'FIPS CODE' if recordtype == 'tax' else 'FIPS'
parcelinfo = parcel[recordtype]
lines = parcelinfo['lines']
wdir = parcelinfo['directory']
filename = parcelinfo['filename']
    
cs = 200000
fin = pd.read_csv(os.path.join(wdir, filename), quoting=3, sep='|', chunksize=cs)

#%%
t1 = time.time()
header_flag = True
hitherto = 0
basename = 'multicounty' if len(counties) > 1 else counties[0]
fname = '{0}_{1}.txt'.format(basename, recordtype)
with open(fname,'w') as fout:
    for i, df in enumerate(fin):
        if i%5 == 0:
            print(i*0.2, time.time()-t1)
        data = df[df[fips_col].isin(counties)]
        if not data.empty:
            print(i, data.shape[0])
            data.to_csv(fout, index=False, sep='|', quoting=3, header=header_flag, encoding='utf-8')
            if header_flag:
                header_flag = False
            hitherto += data.shape[0]
            if hitherto >= records:
                break
t2 = time.time()
print('Processing took {:.0f}s with chunksize={}'.format(t2-t1,cs))
print('Lines per second: {:.0f}'.format(i*cs/(t2-t1)) )
print('Records Extracted = {:,}'.format(hitherto))

#%%
# convert float to ints, replace True/False with 1/0
def minify_file(filename):
    with open("tmpfile.txt",'w') as fout:
        with open(filename,'r') as fin:
            for line in fin:
                newline = line.replace(".0|", "|").replace("True","1").replace("False","0")
                _ = fout.write(newline)
    os.remove(filename)
    os.rename("tmpfile.txt",filename)

#%%
t2 = time.time()
minify_file(fname)
t3 = time.time()
print('minification: {:.1f}s'.format(t3-t2) )
    