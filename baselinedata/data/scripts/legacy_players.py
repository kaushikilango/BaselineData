import pandas as pd
from baselinedata.data.connector import request_connection as request_connection
from baselinedata.data import personal
from baselinedata.utils import logger as lg
def get_players():
    conn, status = request_connection('AWS_BASEDB')
    if status != 200:
        return None, status
    cursor = conn.cursor()
    query = "select distinct winner from matches_master where winner not in (select puid from player_details)"
    cursor.execute(query)
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    return data, 200


data,status = get_players()
players = []
for i in data:
    players.append(i[0])
def get_data(players):
    conn, status = request_connection('AWS_BASEDB')
    if status != 200:
        return None, status
    cursor = conn.cursor()
    for pid in players:
        pd,code = personal.get_player_data(pid)
        print(code)
        if code == 200:
            lg.LOG_INFO(f"STATUS 200 Player data received", "update_players.py", "update_players")
            cursor.execute(
                'INSERT INTO player_details (puid,lname,fname,birthcity,birthcountry,coach,birthdate,age,nationality,height,weight,playhand,backhand,proyear,active) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                (pd['puid'].upper(),pd['lname'],pd['fname'],pd['birthcity'],pd['birthcountry'],pd['coach'],pd['birthdate'],pd['age'],pd['nationality'],pd['height'],pd['weight'],pd['playhand'],pd['backhand'],pd['proyear'],pd['active']))
            conn.commit()
        else:
            lg.LOG_ERROR(f"STATUS {code} Player data not received", "update_players.py", "update_players")
    cursor.close()
    conn.close()
    return 'Players updated',200

message,status = get_data(players)