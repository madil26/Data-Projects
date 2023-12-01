import requests
import random
import json
from datetime import datetime
import psycopg2

cities = ('London','New York', 'Hong Kong', 'Sydney', 'Paris', 'Tokyo', 'Dubai', 'Los Angeles', 'Rome', 'Casablanca','Mexico City','Islamabad','Moscow','Bogota',
          'Miami','Cape Town', 'Nairobi')
def getWeather():
    parameters = {'key':'insertkey','q':random.choice(cities),'aqi':'yes'}
    result = requests.get('http://api.weatherapi.com/v1/current.json',parameters)
    if result.status_code == 200:
        json_data = result.json()
        file_name = str(datetime.now().date()) + '.json'
        with open (file_name,'w') as output_file:
            json.dump(json_data,output_file,indent=4)
    else:
        print(result.text)

def formatWeather():

    try:
        conn = psycopg2.connect(
            host='localhost',
            user='postgres',
            password='033389',
            port='5432',
            dbname='weather_data'
        )
        print("Connection successful!")
    except psycopg2.Error as e:
        print(f"Unable to connect to database: {e}")
    cursor = conn.cursor()

    # Check if the table exists
    cursor.execute("""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_name = 'weather_data'
        );
    """)
    table_exists = cursor.fetchone()[0]
    if not table_exists:
        cursor.execute("""
            CREATE TABLE weather_data (
                city VARCHAR(255),
                region VARCHAR(255),
                country VARCHAR(255),
                temperature numeric(8),
                wind VARCHAR(255),
                uv VARCHAR(255),
                time VARCHAR(255)
            );
        """)
        conn.commit()
        print("Table created successfully!")

    f = open(str(datetime.now().date()) + '.json')
    data = json.load(f)
    city = data["location"]["name"]
    region = data["location"]["region"]
    country = data["location"]["country"]
    temp = data["current"]["temp_f"]
    wind = data["current"]["wind_mph"]
    uv = data["current"]["uv"]
    time = data["location"]["localtime"]
    cursor.execute("""
        INSERT INTO weather_data (city,region,country,temperature,wind,uv,time) 
        VALUES (%s,%s,%s,%s,%s,%s,%s);
    """, (city,region,country,temp,wind,uv,time))


    conn.commit()
    cursor.close()
    conn.close()


if __name__ == '__main__':
    for i in range(50):        
        getWeather()
        formatWeather()
