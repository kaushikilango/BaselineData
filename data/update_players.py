import mysql.connector as sql
import pandas as pd
import time
import personal
from tqdm import tqdm
from connector import request_connection as request_connection

def update_players(data):
    conn = request_connection('baseline_main')
    cursor = conn.cursor()
    cursor.execute('SELECT puid FROM player_details')
    pids = cursor.fetchall()
    pids = [i[0] for i in pids]
    for _,row in tqdm(data.iterrows()):
        if row['pid'] not in pids:
            pd,code = personal.get_player_data(row['pid'])
            if code == 200:
                cursor.execute(
                    'INSERT INTO player_details (puid,lname,fname,birthcity,birthcountry,coach,birthdate,age,nationality,height,weight,playhand,backhand,proyear,active) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                    (pd['puid'],pd['lname'],pd['fname'],pd['birthcity'],pd['birthcountry'],pd['coach'],pd['birthdate'],pd['age'],pd['nationality'],pd['height'],pd['weight'],pd['playhand'],pd['backhand'],pd['proyear'],pd['active']))
            conn.commit()
    cursor.close()
    conn.close()
    return 'Players updated',200
