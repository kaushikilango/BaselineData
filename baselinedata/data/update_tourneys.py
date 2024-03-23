from baselinedata.data import connector, tourneys
from dotenv import load_dotenv
import datetime
from baselinedata.utils import logger as lg
# Load the environment variables
load_dotenv()
def tourney_updater():
# Connect to the database
    lg.LOG_INFO(f"Running update_tourneys.py", "update_tourneys.py", "tourney_updater")
    lg.LOG_INFO(f"Requesting connection", "update_tourneys.py", "tourney_updater")
    conn, status = connector.request_connection('AWS_BASEDB')

    # Get the current month's tournaments
    data = tourneys.get_current_tourneys()
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
        months = {'January,':1,'February,':2,'March,':3,'April,':4,'May,':5,'June,':6,'July,':7,'August,':8,'September,':9,'October,':10,'November,':11,'December,':12}
        row['fdate'] = row['fdate'].split(' ')
        fdate = str(months[row['fdate'][1]]) + '/' + row['fdate'][0] + '/' + row['fdate'][2]
        fdate = datetime.datetime.strptime(fdate, '%m/%d/%Y').strftime('%Y-%m-%d')
        row['tdate'] = row['tdate'].split(' ')
        tdate = str(months[row['tdate'][1]]) + '/' + row['tdate'][0] + '/' + row['tdate'][2]
        tdate = datetime.datetime.strptime(tdate, '%m/%d/%Y').strftime('%Y-%m-%d')
        if not db_data:
            cursor = conn.cursor()
            query = 'INSERT into tourney_master (tid, tourney_name, location, fdate, tdate, season, scores_url, draws_url, type, surface, total_prize_money, environment, sgl_draw_size, dbl_draw_size, live, completed) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            params = [row['tid'], row['tourney_name'], row['location'], fdate, tdate, row['season'], row['scores_url'], row['draws_url'], row['type'], row['surface'], row['total_prize_money'], row['environment'], row['sgl_draw_size'], row['dbl_draw_size'], row['live'], row['completed']]
            cursor.execute(query, params)
            conn.commit()
            cursor.close()
        else:
            cursor = conn.cursor()
            query = 'UPDATE tourney_master SET tourney_name = %s, location = %s, fdate = %s, tdate = %s, scores_url = %s, draws_url = %s, type = %s, surface = %s, total_prize_money = %s, environment = %s, sgl_draw_size = %s, dbl_draw_size = %s, live = %s, completed = %s WHERE season = %s and tid = %s'
            params = [row['tourney_name'], row['location'], fdate, tdate, row['scores_url'], row['draws_url'], row['type'], row['surface'], row['total_prize_money'], row['environment'], row['sgl_draw_size'], row['dbl_draw_size'], row['live'], row['completed'], row['season'], row['tid']]
            cursor.execute(query, params)
            conn.commit()
            cursor.close()
    lg.LOG_INFO(f"Tourneys updated", "update_tourneys.py", "tourney_updater")
    conn.close()
    return "Tourneys updated", 200
    