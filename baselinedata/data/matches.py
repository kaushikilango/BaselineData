import requests as rq
BASE_URL = 'https://www.atptour.com/-/Hawkeye/MatchStats/Complete/'
import pandas as pd
from baselinedata.utils import logger as lg

def get_match_data(tourney_id,season,id):
    lg.LOG_INFO(f"Requesting match data for tourney_id: {tourney_id}, season: {season} and id: {id}", "matches.py", "get_match_data")
    url = BASE_URL + str(season) + '/' + str(tourney_id) + '/' + str(id)
    data = rq.get(url).json()
    if data!=None:
        lg.LOG_INFO(f"STATUS 200 Match data for tourney_id: {tourney_id}, season: {season} and id: {id} received", "matches.py", "get_match_data")
        return data,200
    else:
        lg.LOG_ERROR(f"STATUS 100 Match data for tourney_id: {tourney_id}, season: {season} and id: {id} not found", "matches.py", "get_match_data")
        return None,100
    
def get_setdata(p1_sets,p2_sets):
    p1_scores,p2_scores = {'sets' : []},{'sets':[]}

    for k in p2_sets:
        try:
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
        except Exception as e:
            lg.LOG_ERROR(f"Error in calculating set data. {str(e)}", "matches.py", "get_setdata")
    for k in p1_sets:
        if k['SetNumber'] == 0:
            try:
                timer = k['Stats']['Time'].split(':')
                duration = int(timer[0]) * 60 + int(timer[1])
                if k['Stats']['ServiceStats']['FirstServePointsWon']['Divisor'] + k['Stats']['ServiceStats']['SecondServePointsWon']['Divisor'] == 0:
                    p1_scores['svpt'] = 0
                else:
                    p1_scores['svpt'] = int(100 * (k['Stats']['ServiceStats']['FirstServePointsWon']['Dividend'] + k['Stats']['ServiceStats']['SecondServePointsWon']['Dividend']) / (k['Stats']['ServiceStats']['FirstServePointsWon']['Divisor'] + k['Stats']['ServiceStats']['SecondServePointsWon']['Divisor']))
                p1_scores['aces'] = k['Stats']['ServiceStats']['Aces']['Number']
                p1_scores['dfs'] = k['Stats']['ServiceStats']['DoubleFaults']['Number']
                p1_scores['fsin'] = k['Stats']['ServiceStats']['FirstServe']['Percent']
                p1_scores['fsw'] = k['Stats']['ServiceStats']['FirstServePointsWon']['Percent']
                p1_scores['bpsaved'] = k['Stats']['ServiceStats']['BreakPointsSaved']['Percent']
            except Exception as e:
                lg.LOG_ERROR(f"Error in calculating set data. {str(e)}", "matches.py", "get_setdata")
        else:
            p1_scores['sets'].append(k['SetScore'])
    return (p1_scores,p2_scores,duration)

def get_matches(tourneyid,year,matchid):
    lg.LOG_INFO(f"Requesting matches for tourneyid: {tourneyid}, year: {year} and matchid: {matchid}", "matches.py", "get_matches")
    data,status = get_match_data(tourneyid,year,matchid)
    if status == 200:
        p1_stats,p2_stats,duration = get_setdata(data['Match']['PlayerTeam']['SetScores'],data['Match']['OpponentTeam']['SetScores'])
        sets = []
        for i,j in zip(p1_stats['sets'],p2_stats['sets']):
            sets.append(i + '-' + j)
        while len(sets) < 5:
            sets.append('0-0')
        row = {'match_id':str(data['Tournament']['EventYear']) + str(data['Tournament']['EventId']) + str(data['Match']['MatchId']), 
        'tourney_id':data['Tournament']['EventId']
        ,'tourney_name':data['Tournament']['TournamentName'],
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
        lg.LOG_INFO(f"Matches for tourneyid: {tourneyid}, year: {year} and matchid: {matchid} received", "matches.py", "get_matches")
        return row


def get_all_matches(tourney_id,year):
    non_exist_count = 0
    df = pd.DataFrame(columns = ['match_id','tourney_id','tourney_name','round','tourney_level','player1','player2','p1_id','p2_id','set1','set2','set3','set4','set5','year','p1_ace','p2_ace','p1_df','p2_df','p1_svpt','p2_svpt','p1_fsin','p2_fsin','p1_fsw','p2_fsw','p1_bpsaved','p2_bpsaved'])
    for i in range(1,128):
        if i < 10:
            match_id = 'MS00' + str(i)
        elif i < 100:
            match_id = 'MS0' + str(i)
        else:
            match_id = 'MS' + str(i)
        row = get_matches(tourney_id,year,match_id)
        if row != None:
            df = df._append(row,ignore_index=True)
        else:
            non_exist_count += 1
            if non_exist_count > 10:
                lg.LOG_INFO(f"Matches for tourney_id: {tourney_id} and year: {year} not found --  Maybe exceeded limit", "matches.py", "get_all_matches")
                break
    return df
