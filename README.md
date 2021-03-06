# Corelogic Data Extract
This repo contains a Python script to extract county level data.

## Instructions
In the `Inputs` section of the script near the top, specify the following 3 variables:
1. List of counties' fips codes you want to extract
2. List of the corresponding number of records for all the counties in step 1 (consult the xlsx file for the county counts)
3. Record type; must be one of `{'foreclosure','deed','tax'}`

Run `corelogic_data_extract.py` script. Script will terminate once it reaches the number of records in #2. So runtime will be a function of where your counties are located in the file (see *fips_order* section). The script will create a separate file for each county with a nomenclature of `<fipscode>_<recordtype>.txt`

**Note**: You can also change the `chunksize` variable if you are encountering out-of-memory issues or want to try to make the script run faster.

## `deed` input file
The original deed file `University_Michigan_Deed_KZA_85HRZB.zip` has an inconsistent number of columns in the file. `pandas` doesn't like this. I've manually edited these rows and created a new file as referenced in the script. This doesn't mean that the new file is error free (I'm 100% sure its not). It just means that each row now has the same number of columns.

## `fips_order` files
For each record type, the file will list the first and last row for each county. This will give you a good indication of how long the extraction process will take.

**Tip:** these files are rendered as interactive tables and are searchable.

