import csv
import os
import json
import boto3
import zipfile
import gzip
import bz2

os.chdir('C:\\Users\\christopher.brossman\\Documents\\Projects\\work\\AWS_adjustCompression')
#read in keyList to change transformations
keyList = []
cnt = 0
with open('keyList.csv','rb') as f:
	reader = csv.reader(f)
	for row in reader:
		if cnt > 0:
			keyList.append(row[0])
		cnt = cnt + 1

#adjust file to remove .zip suffix
file_list = [key[0:len(key)-4] for key in keyList]
		
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

for f in file_list:
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
	
	#convert to bzip2
	#print('convert to bzip2: ' + f)
	#fname_upload = f + '.bz2'
	#fnamePath_upload = os.getcwd() + '/bzip/' + f + '.bz2'
	#output = bz2.BZ2File(fnamePath_upload, 'wb')
	#try:
	#	output.write(data)
	#finally:
	#	output.close()
	
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
	
	