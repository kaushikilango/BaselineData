import pandas as pd
import requests as rq
from bs4 import BeautifulSoup
from tqdm import tqdm
from baselinedata.utils import logger as lg
BASE_URL = 'https://www.atptour.com/'
SGL_RANKINGS_ALL = 'en/rankings/singles/?rankRange=0-5000&Region=all&DateWeek=Current%20Week'
DB_NAME = 'Baseline'

def get_soup(url):
    lg.LOG_INFO(f"Requesting soup for url: {url}", "players.py", "get_soup")
    try:
        response = rq.get(url)
        if response.status_code != 200:
            lg.LOG_ERROR(f"STATUS {response.status_code} Soup for url: {url} not found", "players.py", "get_soup")
    except Exception as e:
        lg.LOG_ERROR(f"Error in getting soup. {str(e)}", "players.py", "get_soup")
    return BeautifulSoup(response.text, 'html.parser')

def get_players():
    lg.LOG_INFO(f"Requesting players", "players.py", "get_players")
    soup = get_soup(BASE_URL + SGL_RANKINGS_ALL)
    players = soup.find('table',class_= 'desktop-table')
    data = pd.DataFrame(columns = ['pid','rank','name','link','points','points_moved','tourneys_played','points_losing','points_gaining'])
    for row in tqdm(players.tbody.find_all('tr')):
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
        data.drop_duplicates(subset = 'pid',keep = 'first',inplace = True)
    return data