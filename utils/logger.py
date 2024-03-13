import os
import datetime

# Logs are divided into three levels of severity:
# 1. Debug
# 2. Info
# 3. Error

def LOG_INFO(message,process_name,file_name):
    ctime = datetime.datetime.now().strftime('%d-%d-%Y')
    fname = "logs\info\info_log_" + str(ctime) + ".log"
    log_file = open(fname, "a")
    time = datetime.datetime.now()
    log_file.write(f"INFORMATION FROM {file_name} during execution of {process_name}: {str(time)} {message} + \n")
    log_file.close()
    return

def LOG_DEBUG(message,process_name,file_name):
    ctime = datetime.datetime.now().strftime('%d-%d-%Y')
    fname = "logs\debug\debug_log_" + str(ctime) + ".log"
    log_file = open(fname, "a")
    time = datetime.datetime.now()
    log_file.write(f"DEBUG FROM {file_name} during execution of {process_name}: {str(time)}  {message} + \n")
    log_file.close()
    return

def LOG_ERROR(message,process_name,file_name):
    ctime = datetime.datetime.now().strftime('%d-%d-%Y')
    fname = "logs\error\error_log_" + str(ctime) + ".log"
    log_file = open(fname, "a")
    time = datetime.datetime.now()
    log_file.write(f"ERROR FROM {file_name} during execution of {process_name}: {str(time)}  {message} + \n")
    log_file.close()
    return

LOG_INFO("This is an information message","test_process","test_file")