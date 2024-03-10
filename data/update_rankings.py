import time
import mysql.connector as sql
import pandas as pd
import players
from tqdm import tqdm
from dotenv import load_dotenv
import os
from connector import request_connection as request_connection

load_dotenv()

def max_date(day):
    x = day.split('-')
    x = x[0] + '-' + x[1] + '-' + str(int(x[2]) + 7)
    return x

def sgl_mens_rankings():
    current_date = time.strftime('%Y-%m-%d')
    data = players.get_player_data()
    conn = request_connection('AWS_BASEDB')
    cursor = conn.cursor()
    cursor.execute('SELECT max(rank_date) from sgl_mens_rankings')
    max_date = cursor.fetchall()
    max_date = max_date[0][0]
    if max_date == None:
        max_date = '1900-01-01'
    else:
        max_date = max_date.strftime('%Y-%m-%d')
    data.to_csv('data.csv')
    if max_date < current_date:
        for _,row in tqdm(data.iterrows()):
            if row['points_moved'] != '-':
                row['points_moved'] = int(row['points_moved'].replace(',',''))
            else:
                row['points_moved'] = None
            if row['tourneys_played'] != '-':
                row['tourneys_played'] = int(row['tourneys_played'].replace(',',''))
            else:
                row['tourneys_played'] = None
            if row['points_losing'] != '-':
                row['points_losing'] = int(row['points_losing'].replace(',',''))
            else:
                row['points_losing'] = None
            if row['points_gaining'] != '-':
                row['points_gaining'] = int(row['points_gaining'].replace(',',''))
            else:
                row['points_gaining'] = None

            cursor.execute('INSERT INTO sgl_mens_rankings (puid,name,week_rank,rank_date,points,points_moved,tourneys_played,points_losing,points_gaining) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                           (row['pid'],row['name'],row['rank'],current_date,int(row['points'].replace(',','')),
                            row['points_moved'],row['tourneys_played'],row['points_losing'],
                            row['points_gaining']))
        conn.commit()
        cursor.close()
        conn.close()
        return 'Rankings updated'
    else:
        return 100
