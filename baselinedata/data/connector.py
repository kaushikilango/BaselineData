import os
import mysql.connector as sql
from dotenv import load_dotenv
load_dotenv()

def request_connection(DB_IDENTIFIER):
    try:
        conn = sql.connect(host=os.getenv('AWS_ENDPOINT'), user=os.getenv('AWS_USER'), password=os.getenv('AWS_PASSWORD'),
                       database=os.getenv(DB_IDENTIFIER), port=os.getenv('AWS_PORT'))
        if conn.is_connected():
            return conn, 200
    except Exception as e:
        message = str(e)
        return message, 201