import requests as rq
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm
html = rq.get('https://www.atptour.com/en/rankings/singles?RankRange=0-5000&Region=all&DateWeek=Current%20Week').text
soup = BeautifulSoup(html,'html.parser')
tables = soup.find_all('tables')
table = soup.find('table',class_= 'desktop-table')

data = pd.DataFrame(columns = ['pid','rank','name','link','points','points_moved','tourneys_played','points_losing','points_gaining'])
for row in tqdm(table.tbody.find_all('tr')):
    columns = row.find_all('td')

    if columns !=[] and len(columns) == 8:
        rank = columns[0].text.strip()
        name = columns[1].find('li',class_ = 'name center').text.strip()
        link = columns[1].find('a')['href']
        points = columns[3].text.strip()
        pid = link.split('/')[4]
        points_moved = columns[4].text.strip()
        tourneys_played = columns[5].text.strip()
        points_losing = columns[6].text.strip()
        points_gaining = columns[7].text.strip()
    row = {'pid':pid,'rank':rank,'name':name,'link':link,'points':points,'points_moved':points_moved,'tourneys_played':tourneys_played,'points_losing':points_losing,'points_gaining':points_gaining}
    row = pd.Series(row)
    data = data._append(row,ignore_index = True)
    data.to_csv('data/updated_players.csv',index = False)