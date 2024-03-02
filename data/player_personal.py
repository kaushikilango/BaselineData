from bs4 import BeautifulSoup
import pandas as pd
import requests as rq
from tqdm import tqdm

df = pd.read_csv('C:/Users/kilan/OneDrive/Documents/Sources/BaselineData/data/connection_details.csv')
BASE_URL = 'https://www.atptour.com/en/-/www/players/hero/'
player_data = pd.DataFrame(columns = ['pid','lname','fname','birthcity','birthcountry',
'coach','birthdate','age',
'nationality','height','weight',
'playhand','backhand','proyear','active'])

for index,row in tqdm(df.iterrows()):
    URL = BASE_URL + row['pid'] + '?v=1'
    response = rq.get(URL)
    if response.status_code == 200:
        data = response.json()
        player = {'pid':row['pid'],'lname' : data['LastName'] ,'fname' : data['FirstName'],
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
        player = pd.Series(player)
        player_data = player_data._append(player,ignore_index =True)
    else:
        print(response.status_code)
    player_data.to_csv('player_information.csv')
