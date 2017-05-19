#! /usr/local/anaconda/bin/python2.7

import csv
import os
import json
import boto3
import zipfile
import gzip
#import bz2
import time 
#create String for file
from datetime import datetime, timedelta

nowMinus6 = datetime.now() - timedelta(hours = 6)
year = str(nowMinus6.year)
month = str(nowMinus6.month).zfill(2)
day = str(nowMinus6.day).zfill(2)
hour = str(nowMinus6.hour).zfill(2)
f = '01-demidas_' + year + month + day + '-' + hour + '0000.tsv'
		
os.chdir('C:\\Users\\christopher.brossman\\Documents\\Projects\\work\\AWS_adjustCompression\\newUploads')

OLD_ACCESS_KEY = 'AKIAJOAPCF34GTBIUTAQ'
OLD_SECRET_KEY = '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'
OLD_BUCKET_ID = 'bizintel-clickstream'

old_client = boto3.client(
		's3',
		aws_access_key_id=OLD_ACCESS_KEY,
		aws_secret_access_key=OLD_SECRET_KEY,
	)

NEW_ACCESS_KEY = 'AKIAJWXLRRMVIDMITTDA'
NEW_SECRET_KEY = '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'
NEW_BUCKET_ID = 'bizintel-clickstream-gzip'

new_client = boto3.client(
		's3',
		aws_access_key_id=NEW_ACCESS_KEY,
		aws_secret_access_key=NEW_SECRET_KEY,
	)


print('download from s3: ' + f)
#download file from bizintel-clickstream bucket
fname_dl = f + '.zip'
fnamePath_dl = os.getcwd() + '/zip/' + f + '.zip'
old_client.download_file(OLD_BUCKET_ID,fname_dl,fnamePath_dl)

#read in zip
print('read in zip: ' + f)
try:
	zfile = zipfile.ZipFile(fnamePath_dl)
	data = zfile.read(f)
finally:
	zfile.close()

os.remove(fnamePath_dl)

#convert to gzip
print('convert to gzip: ' + f)
fname_upload = f + '.gz'
fnamePath_upload = os.getcwd() + '/gzip/' + f + '.gz'
with gzip.open(fnamePath_upload, 'wb') as gz:
	gz.write(data)
	
#upload to new s3 bucket
print('upload to s3: ' + f)
new_client.upload_file(fnamePath_upload, NEW_BUCKET_ID, fname_upload)
os.remove(fnamePath_upload)