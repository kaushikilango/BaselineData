import mysql.connector
import pandas as pd
## Server Details
db = mysql.connector.connect(host = 'localhost',
database = 'Baseline',
user = 'root',
password = '12345',auth_plugin='mysql_native_password')

cursor = db.cursor()


df = pd.read_csv('C:/Users/kilan/OneDrive/Documents/Sources/BaselineData/data/player_information.csv',index_col=False,dtype = object)
df = df.where(pd.notnull(df),None)
for i,row in df.iterrows():
    sql = "INSERT INTO master_player_profile values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    row = tuple(list(row)[1:])
    cursor.execute(sql,tuple(row))
    db.commit()
