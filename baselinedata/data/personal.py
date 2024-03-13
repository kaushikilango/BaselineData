import requests as rq
from baselinedata.utils import logger as lg
BASE_URL = 'https://www.atptour.com/en/-/www/players/hero/'

def get_player_data(pid):
    lg.LOG_INFO(f"Requesting player data for pid: {pid}", "personal.py", "get_player_data")
    URL = BASE_URL + pid + '?v=1'
    response = rq.get(URL)
    if response.status_code != 200:
        lg.LOG_ERROR(f"STATUS {response.status_code} Player data for pid: {pid} not found", "personal.py", "get_player_data")
    
    if response.status_code == 200:
        lg.LOG_INFO(f"STATUS 200 Player data for pid: {pid} received", "personal.py", "get_player_data")
        data = response.json()
        player = {'puid':pid,'lname' : data['LastName'] ,'fname' : data['FirstName'],
        'coach' : data['Coach'],'age' : data['Age'],
        'nationality' : data['Nationality'],'height' : data['HeightCm'],'weight' : data['WeightKg'],
        'playhand' : data['PlayHand']['Id'],'backhand' : data['BackHand']['Id'],'proyear' : data['ProYear'],'active': data['Active']['Id']}
        if data['BirthDate'] is not None:
            player['birthdate'] = data['BirthDate'].split('T')[0]
        else:
            player['birthdate'] = None
        if data['BirthCity'] is not None and ','  in data['BirthCity']:
            player['birthcity']  =  data['BirthCity'].split(',')[0]
            player['birthcountry'] = data['BirthCity'].split(',')[1].strip()
        else:
            player['birthcity'] = None
            player['birthcountry'] = None
        return player,response.status_code
    else:
        return None,response.status_code