import os
import datetime
from baselinedata.data import connector
# Logs are divided into three levels of severity:
# 1. Debug
# 2. Info
# 3. Error
conn, status = connector.request_connection('AWS_BASEDB')
def log_insert(message,source,status):
    print('running insert log')
    cursor = conn.cursor()
    time = datetime.datetime.now()
    print(time)
    print(message)
    print(source)
    print(status)
    cursor.execute('INSERT INTO logs_main (log_time,log_message,log_source,status_code) VALUES (%s,%s,%s,%s)',(time,message,source,status))
    conn.commit()
    cursor.close()
getcwd = os.getcwd()
parent = os.path.dirname(getcwd)
target = os.path.join(parent, "BaselineData\\baselinedata\logs")

def LOG_INFO(message,process_name,file_name):
    source = process_name + ':' + file_name
    log_insert(message,source,0)
    ctime = datetime.datetime.now().strftime('%d-%d-%Y')
    target_info = os.path.join(target, "info")
    fname = "info_log_" + str(ctime) + ".log"
    file_path = os.path.join(target_info, fname)
    if os.path.isfile(file_path):
        with open(file_path, "a") as log_file:
            time = datetime.datetime.now()
            log_file.write(f"INFORMATION FROM {file_name} during execution of {process_name}: {str(time)} {message} + \n")
    else:
        with open(file_path, "w") as log_file:
            time = datetime.datetime.now()
            log_file.write(f"INFORMATION FROM {file_name} during execution of {process_name}: {str(time)} {message} + \n")
    log_file.close()
    return
def LOG_DEBUG(message,process_name,file_name):
    source = process_name + ':' + file_name
    log_insert(message,source,0)
    ctime = datetime.datetime.now().strftime('%d-%d-%Y')
    target_debug = os.path.join(target, "debug")
    fname = "debug_log_" + str(ctime) + ".log"
    file_path = os.path.join(target_debug, fname)
    if os.path.isfile(file_path):
        with open(file_path, "a") as log_file:
            time = datetime.datetime.now()
            log_file.write(f"DEBUG FROM {file_name} during execution of {process_name}: {str(time)} {message} + \n")
    else:
        with open(file_path, "w") as log_file:
            time = datetime.datetime.now()
            log_file.write(f"DEBUG FROM {file_name} during execution of {process_name}: {str(time)} {message} + \n")
    return

def LOG_ERROR(message,process_name,file_name):
    source = process_name + ':' + file_name
    log_insert(message,source,0)
    ctime = datetime.datetime.now().strftime('%d-%d-%Y')
    target_error = os.path.join(target, "error")
    fname = "error_log_" + str(ctime) + ".log"
    file_path = os.path.join(target_error, fname)
    if os.path.isfile(file_path):
        with open(file_path, "a") as log_file:
            time = datetime.datetime.now()
            log_file.write(f"ERROR FROM {file_name} during execution of {process_name}: {str(time)} {message} + \n")
    else:
        with open(file_path, "w") as log_file:
            time = datetime.datetime.now()
            log_file.write(f"ERROR FROM {file_name} during execution of {process_name}: {str(time)} {message} + \n")
    return
