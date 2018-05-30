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
counties = [12099]
records = 631288 # needs to be calculated
recordtype = 'tax'
assert recordtype in {'foreclosure','tax','deed'}

#%%
for fips in counties:
    fname = '{}_{}.txt'.format(fips,recordtype)
    if os.path.isfile(fname):
        os.remove(fname)

#%%
parcel = {}
parcel['foreclosure'] = {'directory':r'\\ulib-licensed-content.m.storage.umich.edu\ulib-licensed-content\corelogic',
                         'filename':'Michigan_Uni_FCL_AKZA_85HRYY_Data.zip',
                         'lines':37487908,
                         'columns':['APN','FIPS','State','County','BatchDateSeq Number','DeedCategory','DocumentType','RecordingDate','DocumentYear','DocumentNumber','DocumentBook','DocumentPage','TitleCompanyCode','TitleCompanyName','AttorneyName','AttorneyPhoneNumber','1stDefendantBorrowerOwnerFirstName','1stDefendantBorrowerOwnerLastName','1stDefendantBorrowerOwnerCompanyName','2ndDefendantBorrowerOwnerFirstName','2ndDefendantBorrowerOwnerLastName','2ndDefendantBorrowerOwnerCompanyName','3rdDefendantBorrowerOwnerFirstName','3rdDefendantBorrowerOwnerLastName','3rdDefendantBorrowerOwnerCompanyName','4thDefendantBorrowerOwnerFirstName','4thDefendantBorrowerOwnerLastName','4thDefendantBorrowerOwnerCompanyName','DefendantBorrowerOwnerEtAlIndicator','Filler1','DateofDefault','AmountofDefault','Filler2','FilingDate','CourtCaseNumber','LisPendensType','Plaintiff1/Seller','Plaintiff2/Seller','FinalJudgmentAmount','Filler3','AuctionDate','AuctionTime','StreetAddressofAuctionCall','CityofAuctionCall','StateofAuctionCall','OpeningBid','Filler4','SalesPrice','SitusAddressIndicator1','SitusHouseNumberPrefix1','SitusHouseNumber1','SitusHouseNumberSuffix1','SitusStreetName1','SitusMode1','SitusDirection1','SitusQuadrant1','ApartmentUnit1','PropertyCity1','PropertyState1','PropertyAddressZipCode1','CarrierCode1','FullSiteAddressUnparsed1','LenderBeneficiaryFirstName','LenderBeneficiaryLastName','LenderBeneficiaryCompanyName','LenderBeneficiaryMailingAddress','LenderBeneficiaryCity','LenderBeneficiaryState','LenderBeneficiaryZip','LenderPhone','Filler5','TrusteeName','TrusteeMailing Address','TrusteeCity','TrusteeState','TrusteeZip','TrusteePhone','TrusteesSaleNumber','Filler6','OriginalLoanDate','OriginalLoanRecordingDate','OriginalLoanAmount','OriginalDocumentNumber','OriginalRecordingBook','OriginalRecordingPage','Filler7','ParcelNumberParcel ID','ParcelNumberUnformattedID','LastFullSaleTransferDate','TransferValue','Mailing AddressIndicator2','Mailing HouseNumberPrefix2','MailingHouseNumber2','MailingHouseNumberSuffix2','MailingStreetName2','MailingMode2','MailingDirection2','MailingQuadrant2','Mailing ApartmentUnit2','Mailing PropertyCity2','MailingPropertyState2','Mailing PropertyAddressZipCode2','Mailing CarrierCode2','FullSiteAddressUnparsed2','PropertyIndicator','UseCode','NumberofUnits','LivingAreaSquareFeet','NumberofBedrooms','NumberofBathrooms','NumberofCars','ZoningCode','LotSize','YearBuilt','CurrentLandValue','CurrentImprovementValue','Filler8','Section','Township','Range','Lot','Block','TractSubdivision Name','MapBook','MapPage','UnitNum','ExpandedLegal1','ExpandedLegal2','ExpandedLegal3','Filler9','PIDIRISFRMTD','Add/Change/Delete Record','DEED SEC CAT CODES','mtg sec cat code']
                         }
parcel['tax'] = {'directory':r'\\ulib-licensed-content.m.storage.umich.edu\ulib-licensed-content\corelogic',
                 'filename':'Michigan_Uni_Tax_AKZA_85HRQ5.zip',
                 'lines':150014715,
                 'columns':['FIPS CODE','UNFORMATTED APN','APN SEQUENCE NBR','FORMATTED APN','ORIGINAL APN','PREVIOUS PARCEL NUMBER','P-ID-IRIS-FRMTD','ACCOUNT NUMBER','MAP REFERENCE1','MAP REFERENCE2','CENSUS TRACT','BLOCK NUMBER','LOT NUMBER','RANGE','TOWNSHIP','SECTION','QUARTER SECTION','FLOOD ZONE COMMUNITY PANEL ID','LAND USE','COUNTY USE1','COUNTY USE2','MOBILE HOME IND','ZONING','PROPERTY INDICATOR','MUNICIPALITY NAME','MUNICIPALITY CODE','SUBDIVISION TRACT NUMBER','SUBDIVISION PLAT BOOK','SUBDIVISION PLAT PAGE','SUBDIVISION NAME','PROPERTY LEVEL LATITUDE','PROPERTY LEVEL LONGITUDE','SITUS HOUSE NUMBER PREFIX','SITUS HOUSE NUMBER','SITUS HOUSE NUMBER #2','SITUS HOUSE NUMBER SUFFIX','SITUS DIRECTION','SITUS STREET NAME','SITUS MODE','SITUS QUADRANT','SITUS UNIT NUMBER','SITUS CITY','SITUS STATE','SITUS ZIP CODE','SITUS CARRIER CODE','OWNER CORPORATE INDICATOR','OWNER FULL NAME','OWNER1 LAST NAME','OWNER1 FIRST NAME & MI','OWNER2 LAST NAME','OWNER2 FIRST NAME & MI','ABSENTEE OWNER STATUS','HOMESTEAD EXEMPT','OWNER ETAL INDICATOR','OWNER OWNERSHIP RIGHTS CODE','OWNER RELATIONSHIP TYPE','MAIL HOUSE NUMBER PREFIX','MAIL HOUSE NUMBER','MAIL HOUSE NUMBER #2','MAIL HOUSE NUMBER SUFFIX','MAIL DIRECTION','MAIL STREET NAME','MAIL MODE','MAIL QUADRANT','MAIL UNIT NUMBER','MAIL CITY','MAIL STATE','MAIL ZIP CODE','MAIL CARRIER CODE','MAILING OPT-OUT CODE','TOTAL VALUE CALCULATED','LAND VALUE CALCULATED','IMPROVEMENT VALUE CALCULATED','TOTAL VALUE CALCULATED IND','LAND VALUE CALCULATED IND','IMPROVEMENT VALUE CALCULATED IND','ASSD TOTAL VALUE','ASSD LAND VALUE','ASSD IMPROVEMENT VALUE','MKT TOTAL VALUE','MKT LAND VALUE','MKT IMPROVEMENT VALUE','APPR TOTAL VALUE','APPR LAND VALUE','APPR IMPROVEMENT VALUE','TAX AMOUNT','TAX YEAR','ASSESSED YEAR','TAX CODE AREA','BATCH-ID','BATCH-SEQ','MULTI APN FLAG','DOCUMENT NO.','BOOK & PAGE','DOCUMENT TYPE','RECORDING DATE','SALE DATE','SALE PRICE','SALE CODE','SELLER NAME','TRANSACTION TYPE','TITLE COMPANY CODE','TITLE COMPANY NAME','RESIDENTIAL MODEL INDICATOR','1st MORTGAGE AMOUNT','MORTGAGE DATE','MORTGAGE LOAN TYPE CODE','MORTGAGE DEED TYPE','MORTGAGE TERM CODE','MORTGAGE TERM','MORTGAGE DUE DATE','MORTAGE ASSUMPTION AMOUNT','LENDER CODE','LENDER NAME','2nd MORTGAGE AMOUNT','2nd MORTGAGE LOAN TYPE CODE','2nd DEED TYPE','FRONT FOOTAGE','DEPTH FOOTAGE','ACRES','LAND SQUARE FOOTAGE','UNIVERSAL BUILDING SQUARE FEET','BUILDING SQUARE FEET IND','BUILDING SQUARE FEET','LIVING SQUARE FEET','GROUND FLOOR SQUARE FEET','GROSS SQUARE FEET','ADJUSTED GROSS SQUARE FEET','BASEMENT SQUARE FEET','GARAGE/PARKING SQUARE FEET','YEAR BUILT','EFFECTIVE YEAR BUILT','BEDROOMS','TOTAL ROOMS','TOTAL BATHS CALCULATED','TOTAL BATHS','FULL BATHS','HALF BATHS','1QTR BATHS','3QTR BATHS','BATH FIXTURES','AIR CONDITIONING','BASEMENT FINISH','BASEMENT DESCRIPTION','BLDG CODE','BLDG IMPV CODE','CONDITION','CONSTRUCTION TYPE','EXTERIOR WALLS','FIREPLACE IND','FIREPLACE NUMBER','FIREPLACE TYPE','FOUNDATION','FLOOR','FRAME','GARAGE','HEATING','PARKING SPACES','PARKING TYPE','POOL','POOL CODE','QUALITY','ROOF COVER','ROOF TYPE','STORIES CODE','STORIES NUMBER','STYLE','VIEW','LOCATION INFLUENCE','NUMBER OF UNITS','UNITS NUMBER','ELECTRIC/ENERGY','FUEL','SEWER','UTILITIES','WATER','LEGAL1','LEGAL2','LEGAL3']
                 }
parcel['deed'] = {'directory':r'X:\ParcelData',
                  'filename':'University_Michigan_Deed_KZA_85HRZB_clean3.txt',
                  'lines':367782480,
                  'columns':['FIPS','APN (Parcel Number) (unformatted)','PCL ID IRIS FORMATTED','APN SEQUENCE NUMBER','PENDING RECORD INDICATOR','CORPORATE INDICATOR','OWNER FULL NAME','OWNER 1 LAST NAME','OWNER 1 FIRST NAME & M I','OWNER 2 LAST NAME','OWNER 2 FIRST NAME & MI','OWNER ETAL INDICATOR','C/O NAME','OWNER RELATIONSHIP RIGHTS CODE','OWNER RELATIONSHIP TYPE','PARTIAL INTEREST INDICATOR','ABSENTEE OWNER STATUS','PROPERTY LEVEL LATITUDE','PROPERTY LEVEL LONGITUDE','SITUS HOUSE NUMBER PREFIX','SITUS HOUSE NUMBER','SITUS HOUSE NUMBER SUFFIX','SITUS DIRECTION','SITUS STREET NAME','SITUS MODE','SITUS QUADRANT','SITUS APARTMENT UNIT','SITUS CITY','SITUS STATE','SITUS ZIP CODE','SITUS CARRIER CODE','MAILING HOUSE NUMBER PREFIX','MAILING HOUSE NUMBER','MAILING HOUSE NUMBER SUFFIX','MAILING DIRECTION','MAILING STREET NAME','MAILING MODE','MAILING QUADRANT','MAILING APARTMENT UNIT','MAILING PROPERTY CITY','MAILING PROPERTY STATE','MAILING PROPERTY ADDRESS ZIP CODE','MAILING CARRIER CODE','BATCH-ID','BATCH-SEQ','MULTI APN','SELLER LAST NAME','SELLER FIRST NAME','SELLER NAME 1','SELLER NAME 2','SALE CODE','SALE AMOUNT','SALE DATE (YYYYMMDD)','RECORDING DATE (YYYYMMDD)','DOCUMENT TYPE','TRANSACTION TYPE','DOCUMENT NUMBER','BOOK/PAGE (6x6)','LENDER FULL NAME','LENDER LAST NAME','LENDER FIRST NAME','LENDER ADDRESS','LENDER CITY','LENDER ST','LENDER ZIP','LENDER COMPANY CODE','TITLE COMPANY NAME','TITLE COMPANY CODE','MORTGAGE AMOUNT','MORTGAGE DATE','MORTGAGE INTEREST RATE','MORTGAGE LOAN TYPE CODE','MORTGAGE DEED TYPE','MORTGAGE TERM CODE','MORTGAGE TERM','MORTGAGE DUE DATE','MORTGAGE ASSUMPTION AMOUNT','MTG SEQ NUMBER','PRI-CAT-CODE','MTG SEC CAT CODES 1X10','DEED SEC CAT CODES 2X10','OWNERSHIP TRANSFER PERCENTAGE','LAND USE','PROPERTY INDICATOR','SELLER CARRY BACK','INTER FAMILY','PRIVATE PARTY LENDER','MORTGAGE INTEREST RATE TYPE','CONSTRUCTION LOAN','RESALE/NEW CONSTRUCTION','FORECLOSURE','CASH/MORTGAGE PURCHASE','EQUITY FLAG','REFI FLAG','RESIDENTIAL MODEL INDICATOR','ADD/CHANGE FIELD','FILLER']
                  }

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
    