import personal,players
from tqdm import tqdm
from connector import request_connection as request_connection
from baselinedata.utils import logger as lg
def update_players():
    lg.LOG_INFO(f"Updating players", "update_players.py", "update_players")
    data = players.get_players()
    lg.LOG_INFO(f"Players received", "update_players.py", "update_players")
    lg.LOG_INFO(f"Requesting connection", "update_players.py", "update_players")
    conn,status = request_connection('AWS_BASELINE_DB')
    if status != 200:
        lg.LOG_ERROR(f"STATUS {status} Connection failed", "update_players.py", "update_players")
        return 'Connection failed',status
    lg.LOG_INFO(f"STATUS 200 Connection successful", "update_players.py", "update_players")
    cursor = conn.cursor()
    cursor.execute('SELECT puid FROM player_details')
    pids = cursor.fetchall()
    pids = [i[0] for i in pids]
    for _,row in tqdm(data.iterrows()):
        if row['pid'] not in pids:
            pd,code = personal.get_player_data(row['pid'])
            if code == 200:
                lg.LOG_INFO(f"STATUS 200 Player data received", "update_players.py", "update_players")
                cursor.execute(
                    'INSERT INTO player_details (puid,lname,fname,birthcity,birthcountry,coach,birthdate,age,nationality,height,weight,playhand,backhand,proyear,active) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                    (pd['puid'],pd['lname'],pd['fname'],pd['birthcity'],pd['birthcountry'],pd['coach'],pd['birthdate'],pd['age'],pd['nationality'],pd['height'],pd['weight'],pd['playhand'],pd['backhand'],pd['proyear'],pd['active']))
            else:
                lg.LOG_ERROR(f"STATUS {code} Player data not received", "update_players.py", "update_players")
            conn.commit()
    cursor.close()
    conn.close()
    return 'Players updated',200
