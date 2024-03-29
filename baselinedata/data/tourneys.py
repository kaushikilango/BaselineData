import requests as rq
import pandas as pd
from baselinedata.utils import logger as lg

URL = 'https://www.atptour.com/en/-/tournaments/calendar/tour'

def get_current_tourneys():
    lg.LOG_INFO(f"Requesting current tourneys", "tourneys.py", "get_current_tourneys")
    data = rq.get(URL).json()
    df = pd.DataFrame(columns = ['tid','tourney_name','location','scores_url','draws_url','type','surface','total_prize_money','environment','sgl_draw_size','dbl_draw_size','live','completed'])
    for i in data['TournamentDates']:
        for kilo in i['Tournaments']:
            try:
                lg.LOG_INFO(f"STATUS 200 Current tourneys received", "tourneys.py", "get_current_tourneys")
                lg.LOG_INFO(f"Creating row for tourney: {kilo['Name']}", "tourneys.py", "get_current_tourneys")
                dates  = deformat_date(kilo['FormattedDate'])
                fdate = dates[0]
                tdate = dates[1]
                year = tdate.split(' ')[-1]
                row = {'tid':kilo['Id'],'tourney_name': kilo['Name'],'location':kilo['Location'],'fdate':fdate,'tdate':tdate,'season': year,
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
                df = df._append(row,ignore_index = True)
            except Exception as e:
                lg.LOG_ERROR(f"Error in getting current tourneys. {str(e)}", "tourneys.py", "get_current_tourneys")
    return df
    
def deformat_date(date):
    lg.LOG_INFO(f"Deformatting date: {date}", "tourneys.py", "deformat_date")
    ## 3 - 5 July, 2020
    from_date = ''
    to_date = ''
    dates = []
    x = date.split('-')
    if len(x) != 2:
        return None
    else:
        from_day,from_month,from_year = decode_date(x[0].strip())
        to_day,to_month,to_year = decode_date(x[1].strip())
        if from_month == None:
            from_month = to_month
        if from_year == None:
            from_year = to_year
    dates.append(from_day + ' ' + from_month + ', ' + from_year)
    dates.append(to_day + ' ' + to_month + ', ' + to_year)    
    return dates

def decode_date(date):
    lg.LOG_INFO(f"Decoding date: {date}", "tourneys.py", "decode_date")
    date = date.split(' ')
    if len(date) == 1:
        return date[-1],None,None
    elif len(date) == 2:
        return date[0],date[1],None
    else:
        day = date[0]
        month = date[1].strip(',')
        year = date[2].strip(',')
        return day,month,year
