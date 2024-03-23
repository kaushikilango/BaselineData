import datetime
import pandas as pd
from baselinedata.data.matches import get_all_matches
from dotenv import load_dotenv
from baselinedata.data.connector import request_connection
from tqdm import tqdm
from baselinedata.utils import logger as lg

# Load the environment variables
load_dotenv()
# Connect to the database
lg.LOG_INFO("Requesting connection to the database", "insert_matches.py", "main")
conn, status = request_connection('AWS_BASEDB')

def get_completed_tourneys(year=datetime.datetime.now().year):
    lg.LOG_INFO("Requesting completed tourneys", "insert_matches.py", "get_completed_tourneys")
    query = 'select tid from tourney_master where completed = 1 and live <> 1 and tdate <= now() and season = ' + str(year)
    try:
        df = pd.read_sql(query, conn)
    except Exception as e:
        lg.LOG_ERROR(f"Error in fetching completed tourneys. {str(e)}", "insert_matches.py", "get_completed_tourneys")
    return df

def get_matches_count(tourney_id,year):
    lg.LOG_INFO(f"Requesting matches count for tourney_id: {tourney_id} and year: {year}", "insert_matches.py", "get_matches_count")
    query = f'select count(*) from matches_master where year = {year} and tourney_id = {tourney_id}'
    try:
        cur =  conn.cursor()
        cur.execute(query)
    except Exception as e:
        lg.LOG_ERROR(f"Error in fetching matches count. {str(e)}", "insert_matches.py", "get_matches_count")
    if cur.rowcount == 0:
        lg.LOG_INFO(f"No matches found for tourney_id: {tourney_id} and year: {year}", "insert_matches.py", "get_matches_count")
        return 0
    return cur.fetchall()[-1][0]

def insert_matches(tourney_id,year=datetime.datetime.now().year):
    lg.LOG_INFO(f"Inserting matches for tourney_id: {tourney_id} and year: {year}", "insert_matches.py", "insert_matches")
    count = get_matches_count(tourney_id,year)
    if count > 0:
        lg.LOG_INFO(f'Already exists {count} matches for tourney_id: {tourney_id} and year: {year}', "insert_matches.py", "insert_matches")
        lg.LOG_INFO(f"Matches for tourney_id: {tourney_id} and year: {year} already inserted", "insert_matches.py", "insert_matches")
    else:
        data = get_all_matches(tourney_id,year)
        conn, status = request_connection('AWS_BASEDB')
        lg.LOG_INFO(f"Inserting {len(data)} matches for tourney_id: {tourney_id} and year: {year}", "insert_matches.py", "insert_matches")
        for _,row in tqdm(data.iterrows()):
            query = 'insert into matches_master (match_id,tourney_id,tourney_name,round,tourney_level,player1,player2,p1_id,p2_id,winner,set1,set2,set3,set4,set5,year,p1_ace,p2_ace,p1_df,p2_df,p1_svpt,p2_svpt,p1_fsin,p2_fsin,p1_fsw,p2_fsw,p1_bpsaved,p2_bpsaved,duration) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            params = [row['match_id'],row['tourney_id'],row['tourney_name'],row['round'],row['tourney_level'],row['player1'],row['player2'],row['p1_id'],row['p2_id'],row['winner'],row['set1'],row['set2'],row['set3'],row['set4'],row['set5'],row['year'],row['p1_ace'],row['p2_ace'],row['p1_df'],row['p2_df'],row['p1_svpt'],row['p2_svpt'],row['p1_fsin'],row['p2_fsin'],row['p1_fsw'],row['p2_fsw'],row['p1_bpsaved'],row['p2_bpsaved'],row['duration']]
            lg.LOG_INFO("Requesting connection to the database", "insert_matches.py", "main")
            if status == 200:
                cur = conn.cursor(buffered=True)
                cur.execute(query,params)
                conn.commit()
    return

tids = get_completed_tourneys().iloc[:,0].values.tolist()

for tid in tids:
    insert_matches(tid)

