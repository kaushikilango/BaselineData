import requests as rq
BASE_URL = 'https://www.atptour.com/-/Hawkeye/MatchStats/Complete/'


def get_match_data(tourney_id,season,id):
    url = BASE_URL + str(season) + '/' + str(tourney_id) + '/' + str(id)
    data = rq.get(url).json()
    if data!=None:
        return data,200
    else:
        return None,100
    
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
        else:
            p1_scores['sets'].append(k['SetScore'])
    return (p1_scores,p2_scores,duration)

def get_matches(tourneyid,year,matchid):
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
    
    return row


