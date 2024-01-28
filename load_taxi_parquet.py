import pandas as pd
from dotenv import load_dotenv
import os
import duckdb
import fsspec 
import s3fs

load_dotenv(override=True)
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')

conn = duckdb.connect('duckdb/nyctaxi.duckdb')

print(type(conn.execute('SELECT mm, yyyy FROM stage.yellow_trip ORDER BY yyyy desc, mm desc LIMIT 1').fetchall()))

conn.close()


