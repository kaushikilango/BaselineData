import requests as rq
import pandas as pd

BASE_URL = 'https://www.atptour.com/en/-/www/players/hero/'

def get_player_data(pid):
    URL = BASE_URL + pid + '?v=1'
    response = rq.get(URL)
    if response.status_code == 200:
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