import os
import mysql.connector as sql
from dotenv import load_dotenv
from baselinedata.utils import logger as lg
load_dotenv()

def request_connection(DB_IDENTIFIER):
    lg.LOG_INFO("Requesting connection to the database", "connector.py", "request_connection")
    try:
        conn = sql.connect(host=os.getenv('AWS_ENDPOINT'), user=os.getenv('AWS_USER'), password=os.getenv('AWS_PASSWORD'),
                       database=os.getenv(DB_IDENTIFIER), port=os.getenv('AWS_PORT'))
        if conn.is_connected():
            lg.LOG_INFO("STATUS: 200 Connection to the database established", "connector.py", "request_connection")
            return conn, 200
    except Exception as e:
        message = str(e)
        lg.LOG_ERROR(f"Connection to the database failed. {message}", "connector.py", "request_connection")
        return None, 201

request_connection('AWS_BASEDB')