import time
from tqdm import tqdm
from datetime import date, timedelta
from dotenv import load_dotenv
from baselinedata.data import players,connector
from baselinedata.utils import logger as lg
load_dotenv()

def get_monday_date_of_current_week():
    today = date.today()
    # Monday is 0, Sunday is 6
    monday_delta = today.weekday() - 0
    monday_date = today - timedelta(days=monday_delta)
    return monday_date

def sgl_mens_rankings():
    lg.LOG_INFO(f"Updating rankings", "update_rankings.py", "sgl_mens_rankings")
    data = players.get_players()
    conn,status = connector.request_connection('AWS_BASEDB')
    if status != 200:
        lg.LOG_ERROR(f"STATUS {status} Connection failed", "update_rankings.py", "sgl_mens_rankings")
        return status
    cursor = conn.cursor()
    curr_week = get_monday_date_of_current_week()
    cursor.execute('select count(*) from sgl_mens_rankings where rank_date = %s',(curr_week,))
    count = cursor.fetchall()
    if count[0][0] != 0:
        cursor.execute("INSERT INTO sgl_mens_rankings_logs (puid, name, week_rank, points, points_moved, tourneys_played, points_losing, points_gaining,rank_date) select * from sgl_mens_rankings")
        cursor.execute('truncate table sgl_mens_rankings')
        for _,row in tqdm(data.iterrows()):
            try:
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
                            (row['pid'],row['name'],row['rank'],curr_week,int(row['points'].replace(',','')),
                                row['points_moved'],row['tourneys_played'],row['points_losing'],
                                row['points_gaining']))
            except Exception as e:
                lg.LOG_ERROR(f"Error in updating rankings. {str(e)}", "update_rankings.py", "sgl_mens_rankings")
        conn.commit()
        cursor.close()
        conn.close()
        return 'Rankings updated',200
    else:
        return 'Process in-complete',100

if __name__ == '__main__':
    sgl_mens_rankings()