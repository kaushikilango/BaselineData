from datetime import datetime
from baselinedata.data import update_players,update_rankings,update_tourneys,insert_matches
from baselinedata.utils import logger as lg
#### Player Updater
def player_updater():
    try:
        lg.LOG_INFO("Player Updater Started", "Player Updater", "updater.py")
        start = datetime.now()
        message,status_code = update_players.update_players()
        if status_code == 200:
            lg.LOG_INFO(f"STATUS 200 {message}", "Player Updater", "updater.py")
        end = datetime.now()
        lg.LOG_INFO(f"Player Updater Process Com in {end-start}", "Player Updater", "updater.py")
        return 200
    except Exception as e:
        lg.LOG_ERROR(f"Error in Player Updater. {str(e)}", "Player Updater", "updater.py")
        return 500
#### Rankings Updater
def rankings_updater():
    try:
        lg.LOG_INFO("Rankings Updater Started", "Rankings Updater", "updater.py")
        start = datetime.now()
        message,status_code = update_rankings.sgl_mens_rankings()
        if status_code == 200:
            lg.LOG_INFO(f"STATUS 200 {message}", "Rankings Updater", "updater.py")
        end = datetime.now()
        lg.LOG_INFO(f"Rankings Updater Process Com in {end-start}", "Rankings Updater", "updater.py")
        return 200
    except Exception as e:
        lg.LOG_ERROR(f"Error in Rankings Updater. {str(e)}", "Rankings Updater", "updater.py")
        return 500
#### Tournament Updater
def tournaments_updater():
    try:
        lg.LOG_INFO("Tournament Updater Started", "Tournament Updater", "updater.py")
        start = datetime.now()
        message,status_code = update_tourneys.tourney_updater()
        print(message,status_code)
        if status_code == 200:
            lg.LOG_INFO(f"STATUS 200 {message}", "Tournament Updater", "updater.py")
        else:
            lg.LOG_ERROR(f"STATUS {status_code} {message}", "Tournament Updater", "updater.py")
        end = datetime.now()
        lg.LOG_INFO(f"Tournament Updater Process Com in {end-start}", "Tournament Updater", "updater.py")
        return 200
    except Exception as e:
        lg.LOG_ERROR(f"Error in Tournament Updater. {str(e)}", "Tournament Updater", "updater.py")
        return 500
#### Match Updater

def match_inserter():
    try:
        lg.LOG_INFO("Match Updater Started", "Match Updater", "updater.py")

        tourney_ids = insert_matches.get_completed_tourneys()

        for _,row in tourney_ids.iterrows():
            start = datetime.now()
            status = insert_matches.insert_matches(row['tid'])
            if status == 200:
                lg.LOG_INFO(f"STATUS 200 {status} for tourney {row['tid']}", "Match Updater", "updater.py")
            else:
                lg.LOG_INFO(f"STATUS {status} {status} for tourney {row['tid']}", "Match Updater", "updater.py")
            end = datetime.now()
            lg.LOG_INFO(f"Match Updater Process Com in {end-start}", "Match Updater", "updater.py")
        return 200
    except Exception as e:
        lg.LOG_ERROR(f"Error in Match Updater. {str(e)}", "Match Updater", "updater.py")
        return 500
    
if __name__ == "__main__":
    
    ## Player Updater runs weekly
    current_day = datetime.now().weekday()
    current_hour = datetime.now().hour
    current_min = datetime.now().minute
    current_week = datetime.now().isocalendar()[1]
    if current_day == 1 and current_hour == 0 and current_min < 11: # Tuesday 00:00
        if current_week % 2 == 0:
            ## run tourney updater and match updater
            ## run player updater
            pass
        else:
            pass
    else:
        lg.LOG_INFO("Player Updater not authorized due to restricted timings", "Player Updater", "updater.py")
        