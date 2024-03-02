from bs4 import BeautifulSoup
import requests as rq
from datetime import datetime
import calendar
URL = 'https://www.atptour.com/en/-/tournaments/calendar/tour'

data = rq.get(URL).json()
current = calendar.month_name[datetime.now().month] + ', ' + str(datetime.now().year)

for i in data['TournamentDates']:
    if i['DisplayDate'] == current:
        for k in i['Tournaments']:
            print(k)
