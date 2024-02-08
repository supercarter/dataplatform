import pandas as pd
import os
import duckdb
import s3fs

aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')

# List objects in the S3 bucket
s3 = s3fs.S3FileSystem()
yellow_tripdata_files = [file for file in s3.ls('nyc-tlc/trip data') if 'yellow_tripdata' in file]

#for file_path in yellow_tripdata_files[:1]:
#    filename = file_path.split('/')[-1]  # Extract filename from the full path
#    s3.get(file_path, f'duckdb/{filename}')  # Download the file to the duckdb folder

df = pd.read_parquet('duckdb/yellow_tripdata_2009-01.parquet')
print(df)

#conn = duckdb.connect('duckdb/nyctaxi.duckdb')

#print(type(conn.execute('SELECT mm, yyyy FROM stage.yellow_trip ORDER BY yyyy desc, mm desc LIMIT 1').fetchall()))

#conn.close()


