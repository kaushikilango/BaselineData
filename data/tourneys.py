from bs4 import BeautifulSoup
import requests as rq
from datetime import datetime
import calendar
import pandas as pd

URL = 'https://www.atptour.com/en/-/tournaments/calendar/tour'



def get_month_tourneys(month = None,year = None):
    data = rq.get(URL).json()
    if month == None and year == None:
        current = calendar.month_name[datetime.now().month] + ', ' + str(datetime.now().year)
    else:
        current = month + ', ' + year

    data = pd.DataFrame(columns = ['tid','tourney_name','location','scores_url','draws_url','type','surface','total_prize_money','environment','sgl_draw_size','dbl_draw_size','live','completed'])
    for i in data['TournamentDates']:
        if i['DisplayDate'] == current:
            for kilo in i['Tournaments']:

                row = {'tid':kilo['Id'],'tourney_name': kilo['Name'],'location':kilo['Location'],
                    'scores_url':kilo['ScoresUrl'],'draws_url':kilo['DrawsUrl'],'type':kilo['Type'],'surface':kilo['Surface'],
                    'total_prize_money':kilo['TotalFinancialCommitment'],'environment':kilo['IndoorOutdoor']
                    ,'sgl_draw_size':kilo['SglDrawSize'],'dbl_draw_size':kilo['DblDrawSize']}
                if kilo['IsLive']:
                    row['live'] = '1'
                else:
                    row['live'] = '0'

                if kilo['IsPastEvent']:
                    row['completed'] = '1'
                else:
                    row['completed'] = '0'
                row = pd.Series(row)
                data = data.append(row,ignore_index = True)
    return data
    
            
