from datetime import datetime
from baselinedata.data import update_players,update_rankings,update_tourneys
from baselinedata.utils import logger as lg
#### Player Updater
'''
lg.LOG_INFO("Player Updater Started", "Player Updater", "updater.py")
start = datetime.now()
message,status_code = update_players.update_players()
print(message,status_code)
if status_code == 200:
    lg.LOG_INFO(f"STATUS 200 {message}", "Player Updater", "updater.py")
end = datetime.now()
lg.LOG_INFO(f"Player Updater Process Com in {end-start}", "Player Updater", "updater.py")
#### Rankings Updater
lg.LOG_INFO("Rankings Updater Started", "Rankings Updater", "updater.py")
start = datetime.now()
message,status_code = update_rankings.sgl_mens_rankings()
print(message,status_code)
if status_code == 200:
    lg.LOG_INFO(f"STATUS 200 {message}", "Rankings Updater", "updater.py")
end = datetime.now()
lg.LOG_INFO(f"Rankings Updater Process Com in {end-start}", "Rankings Updater", "updater.py")
#### Tournament Updater
lg.LOG_INFO("Tournament Updater Started", "Tournament Updater", "updater.py")
start = datetime.now()
message,status_code = update_tourneys.tourney_updater()
print(message,status_code)
if status_code == 200:
    lg.LOG_INFO(f"STATUS 200 {message}", "Tournament Updater", "updater.py")
else:
    lg.LOG_ERROR(f"STATUS {status_code} {message}", "Tournament Updater", "updater.py")
end = datetime.now()
#### Match Updater
'''
lg.LOG_INFO("Match Updater Started", "Match Updater", "updater.py")