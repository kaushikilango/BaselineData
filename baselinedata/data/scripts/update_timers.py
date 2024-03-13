import mysql.connector as sql
import os
from dotenv import load_dotenv
import requests as rq 
from tqdm import tqdm
load_dotenv()
BASE_URL = 'https://www.atptour.com/-/Hawkeye/MatchStats/Complete/'
def request_connection(DB_IDENTIFIER):
    conn = sql.connect(host=os.getenv('AWS_ENDPOINT'), user=os.getenv('AWS_USER'), password=os.getenv('AWS_PASSWORD'),
                       database=os.getenv(DB_IDENTIFIER), port=os.getenv('AWS_PORT'))
    if conn.is_connected():
        return conn, 200
    else:
        return conn, 201

def get_tourney_ids():
    query = 'select year,tourney_id,right(match_id,5) from matches_master where duration is null'
    conn, status = request_connection('AWS_BASEDB')
    cursor = conn.cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    return data

x = get_tourney_ids()
count = 0
for year,tourney_id,match_id in tqdm(x):
    URL = BASE_URL + str(year) + '/' + str(tourney_id) + '/' + str(match_id)
    time= rq.get(URL).json()['Match']['MatchTimeTotal']
    match_id = str(year) + str(tourney_id) + str(match_id)
    if time!=None:
        time = time.split(':')
        time = int(time[0]) * 60 + int(time[1])
        query = 'update matches_master set duration = %s where match_id = %s'
        params = [time,match_id]
        conn, status = request_connection('AWS_BASEDB')
        cursor = conn.cursor()
        cursor.execute(query,params)
        conn.commit()
        cursor.close()
    if time==None:
        count +=1
    if count%10==0:
        print(count)