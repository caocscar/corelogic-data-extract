# Corelogic Data Extract
This repo contains a Python script to extract county level data.

## Instructions
In the `Inputs` section of the script near the top, specify the following 3 variables:
1. list of counties' fips codes you want to extract
2. the total number of records for all the counties of interest (consult the xlsx file with the county counts)
3. record type; must be one of {'foreclosure','deed',tax'}

Run `corelogic_data_extract.py` script. Script will terminate once it reaches the number of records. So runtime will be a function of what order your counties are located in the original file. The script will create a separate file for each county in the form of `fipscode_recordtype.txt`

## `deed` input file
The original deed file `University_Michigan_Deed_KZA_85HRZB.zip` has an inconsistent number of columns in the file. `pandas` doesn't like this. I've manually edited these rows and created a new file as referenced in the script. This doesn't mean that the new file is error free (I'm 100% sure its not). It just means that each row now has the same number of columns.
