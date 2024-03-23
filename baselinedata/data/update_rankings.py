import time
from tqdm import tqdm
from dotenv import load_dotenv
from baselinedata.data import players,connector
from baselinedata.utils import logger as lg
load_dotenv()

def sgl_mens_rankings():
    lg.LOG_INFO(f"Updating rankings", "update_rankings.py", "sgl_mens_rankings")
    current_date = time.strftime('%Y-%m-%d')
    data = players.get_players()
    conn,status = connector.request_connection('AWS_BASEDB')
    if status != 200:
        lg.LOG_ERROR(f"STATUS {status} Connection failed", "update_rankings.py", "sgl_mens_rankings")
        return status
    cursor = conn.cursor()
    cursor.execute('SELECT date_add(max(rank_date), INTERVAL 7 DAY) from sgl_mens_rankings')
    max_date = cursor.fetchall()
    max_date = max_date[0][0]
    if max_date == None:
        lg.LOG_INFO(f"Max date not found", "update_rankings.py", "sgl_mens_rankings")
        max_date = '1900-01-01'
    else:
        max_date = max_date.strftime('%Y-%m-%d')
    if max_date < current_date:
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
                            (row['pid'],row['name'],row['rank'],current_date,int(row['points'].replace(',','')),
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