import os
import mysql.connector as sql
import requests as rq
import pandas as pd
from tqdm import tqdm
BASE_URL = 'https://www.atptour.com/-/Hawkeye/MatchStats/Complete/'
from dotenv import load_dotenv
load_dotenv()
def request_connection(DB_IDENTIFIER):
    conn = sql.connect(host=os.getenv('AWS_ENDPOINT'), user=os.getenv('AWS_USER'), password=os.getenv('AWS_PASSWORD'),
                       database=os.getenv(DB_IDENTIFIER), port=os.getenv('AWS_PORT'))
    if conn.is_connected():
        return conn, 200
    else:
        return conn, 201

def get_tourney_ids():
    url = 'https://www.atptour.com/en/-/tournaments/calendar/tour'
    data = rq.get(url).json()
    ids = []
    for k in data['TournamentDates']:
        for i in k['Tournaments']:
            ids.append(i['Id'])
    return ids

def get_match_data(tourney_id,season,id):
    url = BASE_URL + str(season) + '/' + str(tourney_id) + '/' + str(id)
    print(url)
    data = rq.get(url).json()
    if data!=None:
        return data,200
    else:
        return None,100
def calculate_minutes(time):
    time = time.split(':')
    return int(time[0]) * 60 + int(time[1])
def get_setdata(p1_sets,p2_sets):
    p1_scores,p2_scores = {'sets' : []},{'sets':[]}

    for k in p2_sets:
        if k['SetNumber'] == 0:
            if k['Stats']['ServiceStats']['FirstServePointsWon']['Divisor'] + k['Stats']['ServiceStats']['SecondServePointsWon']['Divisor'] == 0:
                p2_scores['svpt'] = 0
            else:
                p2_scores['svpt'] = int(100 * (k['Stats']['ServiceStats']['FirstServePointsWon']['Dividend'] + k['Stats']['ServiceStats']['SecondServePointsWon']['Dividend']) / (k['Stats']['ServiceStats']['FirstServePointsWon']['Divisor'] + k['Stats']['ServiceStats']['SecondServePointsWon']['Divisor']))
            p2_scores['aces'] = k['Stats']['ServiceStats']['Aces']['Number']
            p2_scores['dfs'] = k['Stats']['ServiceStats']['DoubleFaults']['Number']
            p2_scores['fsin'] = k['Stats']['ServiceStats']['FirstServe']['Percent']
            p2_scores['fsw'] = k['Stats']['ServiceStats']['FirstServePointsWon']['Percent']
            p2_scores['bpsaved'] = k['Stats']['ServiceStats']['BreakPointsSaved']['Percent']
        else:
            p2_scores['sets'].append(k['SetScore'])
    for k in p1_sets:
        if k['SetNumber'] == 0:
            duration = calculate_minutes(k['Stats']['Time'])
            if k['Stats']['ServiceStats']['FirstServePointsWon']['Divisor'] + k['Stats']['ServiceStats']['SecondServePointsWon']['Divisor'] == 0:
                p1_scores['svpt'] = 0
            else:
                p1_scores['svpt'] = int(100 * (k['Stats']['ServiceStats']['FirstServePointsWon']['Dividend'] + k['Stats']['ServiceStats']['SecondServePointsWon']['Dividend']) / (k['Stats']['ServiceStats']['FirstServePointsWon']['Divisor'] + k['Stats']['ServiceStats']['SecondServePointsWon']['Divisor']))
            p1_scores['aces'] = k['Stats']['ServiceStats']['Aces']['Number']
            p1_scores['dfs'] = k['Stats']['ServiceStats']['DoubleFaults']['Number']
            p1_scores['fsin'] = k['Stats']['ServiceStats']['FirstServe']['Percent']
            p1_scores['fsw'] = k['Stats']['ServiceStats']['FirstServePointsWon']['Percent']
            p1_scores['bpsaved'] = k['Stats']['ServiceStats']['BreakPointsSaved']['Percent']
        else:
            p1_scores['sets'].append(k['SetScore'])
    return (p1_scores,p2_scores,duration)


def create_database():
    df = pd.DataFrame(columns = ['match_id','tourney_id','tourney_name','round','tourney_level','player1','player2','p1_id','p2_id','set1','set2','set3','set4','set5','year','p1_ace','p2_ace','p1_df','p2_df','p1_svpt','p2_svpt','p1_fsin','p2_fsin','p1_fsw','p2_fsw','p1_bpsaved','p2_bpsaved'])
    ids = ['560']
    print(ids)
    cont_loss = 0
    count = 0
    season = 2023
    for mid in tqdm(ids):
        for i in range(128,1,-1):
            if i < 10:
                id = 'ms' + '00' + str(i)
            elif i < 100:
                id = 'ms' + '0' + str(i)
            else:
                id = 'ms' + str(i)
            data,status = get_match_data(mid,season,id)
            if status == 200:
                p1_stats,p2_stats,duration = get_setdata(data['Match']['PlayerTeam']['SetScores'],data['Match']['OpponentTeam']['SetScores'])
                sets = []
                for i,j in zip(p1_stats['sets'],p2_stats['sets']):
                    sets.append(i + '-' + j)
                while len(sets) < 5:
                    sets.append('0-0')
                row = {'match_id':str(data['Tournament']['EventYear']) + str(data['Tournament']['EventId']) + str(data['Match']['MatchId']), 
                    'tourney_id':data['Tournament']['EventId']
                    ,'tourney_name':data['Tournament']['EventDisplayName'],
                    'round':data['Match']['Round']['ShortName'],
                    'tourney_level': data['Tournament']['EventType'],
                    'player1': data['Match']['PlayerTeam1']['PlayerFirstNameFull'] + ' ' + data['Match']['PlayerTeam1']['PlayerLastName'],
                        'player2': data['Match']['PlayerTeam2']['PlayerFirstNameFull'] + ' ' + data['Match']['PlayerTeam2']['PlayerLastName'],
                        'p1_id':data['Match']['PlayerTeam1']['PlayerId'],
                        'p2_id':data['Match']['PlayerTeam2']['PlayerId'],
                        'winner':data['Match']['Winner'],
                        'set1':sets[0],
                        'set2':sets[1],
                        'set3':sets[2],
                        'set4':sets[3],
                        'set5':sets[4],
                        'year':data['Tournament']['EventYear'],
                        'p1_ace':p1_stats['aces'],
                        'p2_ace':p2_stats['aces'],
                        'p1_df':p1_stats['dfs'],
                        'p2_df':p2_stats['dfs'],
                        'p1_svpt':p1_stats['svpt'],
                        'p2_svpt':p2_stats['svpt'],
                        'p1_fsin':p1_stats['fsin'],
                        'p2_fsin':p2_stats['fsin'],
                        'p1_fsw':p1_stats['fsw'],
                        'p2_fsw':p2_stats['fsw'],
                        'p1_bpsaved':p1_stats['bpsaved'],
                        'p2_bpsaved':p2_stats['bpsaved'],
                        'duration':duration
                    }
                row = pd.Series(row)
                df = df._append(row,ignore_index = True)
                count += 1
                cont_loss = 0
            else:
                cont_loss += 1
                if cont_loss > 500:
                    break
        df = df.drop(df[df.isna().any(axis=1)].index)
        df.to_csv('matches_2000.csv',index = False)
    return df

def insert_data(df):
    conn,status = request_connection('AWS_BASEDB')
    if status == 200:
        cursor = conn.cursor()
        for _,row in tqdm(df.iterrows()):
            query = 'INSERT into matches_master (match_id, tourney_id, tourney_name, round, tourney_level, player1, player2, p1_id, p2_id, winner,set1, set2, set3, set4, set5, year, p1_ace, p2_ace, p1_df, p2_df, p1_svpt, p2_svpt, p1_fsin, p2_fsin, p1_fsw, p2_fsw, p1_bpsaved, p2_bpsaved,duration) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            params = [row['match_id'],row['tourney_id'],row['tourney_name'],row['round'],row['tourney_level'],row['player1'],row['player2'],row['p1_id'],row['p2_id'],row['winner'],row['set1'],row['set2'],row['set3'],row['set4'],row['set5'],row['year'],row['p1_ace'],row['p2_ace'],row['p1_df'],row['p2_df'],row['p1_svpt'],row['p2_svpt'],row['p1_fsin'],row['p2_fsin'],row['p1_fsw'],row['p2_fsw'],row['p1_bpsaved'],row['p2_bpsaved'],row['duration']]
            cursor.execute(query,params)
            conn.commit()
        cursor.close()

#df = create_database()
insert_data(pd.read_csv('matches_2000.csv'))
os.remove('matches_2000.csv')



