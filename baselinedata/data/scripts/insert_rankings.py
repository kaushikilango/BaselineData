import pandas as pd
from baselinedata.data.connector import request_connection as request_connection
import os
from tqdm import tqdm
def insert_rankings(data):
    conn, status = request_connection('AWS_BASEDB')
    query = "INSERT INTO sgl_mens_rankings_logs (puid,name,week_rank,points,points_moved,tourneys_played, points_losing,points_gaining,rank_date) values(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    cursor = conn.cursor()
    for _,row in tqdm(data.iterrows()):
        try:
            cursor.execute(query,(row['pid'],row['name'],row['rank'],row['points'],row['points_moved'],row['tourneys_played'],row['points_losing'],row['points_gaining'],row['date_of_ranking']))
        except Exception as e:
            print(e)
        conn.commit()
    cursor.close()
    conn.close()

df = pd.read_csv('baselinedata\dumps\weekend_rankings.csv')
insert_rankings(df)