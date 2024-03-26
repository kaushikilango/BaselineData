import pandas as pd
from baselinedata.data.connector import request_connection as request_connection
from tqdm import tqdm
data = pd.read_csv(r"C:\Users\kilan\OneDrive\Documents\Sources\BaselineData\baselinedata\data\scripts\worldcities.csv")
conn, status = request_connection('AWS_BASEDB')
for _,row in tqdm(data.iterrows()):
    query = f"INSERT INTO geo_master (country,lat,lng,city,iso2,iso3) values('{row['country']}',{row['lat']},{row['lng']},'{row['city']}','{row['iso2']}','{row['iso3']}')"
    cursor = conn.cursor()
    try:
        cursor.execute(query)
    except Exception as e:
        print(e)
    conn.commit()
cursor.close()
conn.close()
print("Data inserted")
    