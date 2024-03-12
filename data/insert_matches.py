import os,requests,datetime
import pandas as pd
from matches import get_all_matches
import mysql.connector as sql
from dotenv import load_dotenv
from connector import request_connection
from tqdm import tqdm
# Load the environment variables
load_dotenv()
# Connect to the database
conn, status = request_connection('AWS_BASEDB')

def get_completed_tourneys(year=datetime.datetime.now().year):
    query = 'select tid from tourney_master where completed = 1 and live <> 1 and tdate <= now() and season = ' + str(year)
    df = pd.read_sql(query, conn)
    return df

def get_matches_count(tourney_id,year):
    query = f'select count(*) from matches_master where year = {year} and tourney_id = {tourney_id}'
    cur =  conn.cursor()
    cur.execute(query)
    return cur.fetchall()[-1][0]

def insert_matches(tourney_id,year=datetime.datetime.now().year):
    count = get_matches_count(tourney_id,year)
    if count > 0:
        pass
    else:
        data = get_all_matches(tourney_id,year)
        print('Received data')
        for _,row in tqdm(data.iterrows()):
            query = 'insert into matches_master (match_id,tourney_id,tourney_name,round,tourney_level,player1,player2,p1_id,p2_id,winner,set1,set2,set3,set4,set5,year,p1_ace,p2_ace,p1_df,p2_df,p1_svpt,p2_svpt,p1_fsin,p2_fsin,p1_fsw,p2_fsw,p1_bpsaved,p2_bpsaved,duration) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            params = [row['match_id'],row['tourney_id'],row['tourney_name'],row['round'],row['tourney_level'],row['player1'],row['player2'],row['p1_id'],row['p2_id'],row['winner'],row['set1'],row['set2'],row['set3'],row['set4'],row['set5'],row['year'],row['p1_ace'],row['p2_ace'],row['p1_df'],row['p2_df'],row['p1_svpt'],row['p2_svpt'],row['p1_fsin'],row['p2_fsin'],row['p1_fsw'],row['p2_fsw'],row['p1_bpsaved'],row['p2_bpsaved'],row['duration']]
            cur = conn.cursor()
            cur.execute(query,params)
            conn.commit()
    return

def main():
    tourneys = get_completed_tourneys()
    for _,row in tourneys.iterrows():
        insert_matches(row['tid'])
    return

if __name__ == '__main__':
    main()
    conn.close()