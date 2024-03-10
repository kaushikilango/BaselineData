import connector
import pandas as pd
from dotenv import load_dotenv
import os
import tourneys

# Load the environment variables
load_dotenv()
# Connect to the database

conn, status = connector.request_connection('AWS_BASEDB')

# Get the current month's tournaments
data = tourneys.get_month_tourneys()
# Iterate through the data and update the database

for index,row in data.iterrows():
    current_season = str(row['season'])
    current_tid = str(row['tid'])
    query = 'SELECT * FROM tourney_master WHERE season = %s and tid = %s'
    params = [current_season, current_tid]
    cursor = conn.cursor(buffered=True)
    cursor.execute(query, params)
    db_data = cursor.fetchall()
    cursor.close()
    if not db_data:
        cursor = conn.cursor()
        query = 'INSERT into tourney_master (tid, tourney_name, location, fdate, tdate, season, scores_url, draws_url, type, surface, total_prize_money, environment, sgl_draw_size, dbl_draw_size, live, completed) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        params = [row['tid'], row['tourney_name'], row['location'], row['fdate'], row['tdate'], row['season'], row['scores_url'], row['draws_url'], row['type'], row['surface'], row['total_prize_money'], row['environment'], row['sgl_draw_size'], row['dbl_draw_size'], row['live'], row['completed']]
        cursor.execute(query, params)
        conn.commit()
        cursor.close()
    else:
        cursor = conn.cursor()
        query = 'UPDATE tourney_master SET tourney_name = %s, location = %s, fdate = %s, tdate = %s, scores_url = %s, draws_url = %s, type = %s, surface = %s, total_prize_money = %s, environment = %s, sgl_draw_size = %s, dbl_draw_size = %s, live = %s, completed = %s WHERE season = %s and tid = %s'
        params = [row['tourney_name'], row['location'], row['fdate'], row['tdate'], row['scores_url'], row['draws_url'], row['type'], row['surface'], row['total_prize_money'], row['environment'], row['sgl_draw_size'], row['dbl_draw_size'], row['live'], row['completed'], row['season'], row['tid']]
        cursor.execute(query, params)
        conn.commit()
        cursor.close()
    