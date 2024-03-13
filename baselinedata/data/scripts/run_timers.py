import mysql.connector as sql
import pandas as pd
import os
import requests as rq
from dotenv import load_dotenv
from tqdm import tqdm
load_dotenv()

def request_connection(DB_IDENTIFIER):
    conn = sql.connect(host=os.getenv('AWS_ENDPOINT'), user=os.getenv('AWS_USER'), password=os.getenv('AWS_PASSWORD'),
                       database=os.getenv(DB_IDENTIFIER), port=os.getenv('AWS_PORT'))
    if conn.is_connected():
        return conn, 200
    else:
        return conn, 201
    
df = pd.read_csv('match_durations1.csv')

conn,status = request_connection('AWS_BASEDB')
cursor = conn.cursor()
for i in tqdm(range(len(df))):
    cursor.execute(f"UPDATE matches_master SET duration = {df['duration'][i]} WHERE match_id = '{df['matchid'][i]}'")
    conn.commit()