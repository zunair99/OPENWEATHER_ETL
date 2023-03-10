# Importing the libraries
import os
import requests
import pandas as pd
import json 
from sqlalchemy import create_engine
import psycopg2

def initialize_api_key():
    # Set environment variables
    api_key = os.environ.get('OWA_API_KEY')
    return api_key

def clean_worldcities():

    # Importing the dataset to map the cities
    worldcities = pd.read_csv('dags\data\worldcities.csv')
    
    # Dropping duplicates and null values
    worldcities.drop_duplicates(subset=['city_ascii', 'lat','lng'], inplace=True)
    worldcities.drop(['iso2','admin_name','capital'], axis=1, inplace=True)
    worldcities.dropna(axis=0,how='any',inplace=True)

    # Sampling 100 cities for testing
    worldcities = worldcities.sample(n=100, random_state=1).reset_index(drop=True)

    return worldcities

# Extracting the data
def extract_openmap():

    # Initialize variables
    worldcities = clean_worldcities()
    api_key = initialize_api_key()

    # Initialize empty dataframes
    df_main = pd.DataFrame()
    df_coord = pd.DataFrame()
    df_weather = pd.DataFrame()

    # Loop through worldcities to extract Weather API data based on latitude and longitude coordinates
    for i in range(len(worldcities)):

        # Get weather data, output in imperial units (Fahrenheit)
        request = requests.get("https://api.openweathermap.org/data/2.5/weather?lat={latitude}&lon={longitude}&units=imperial&appid={api_key}".format(latitude=worldcities['lat'][i], longitude=worldcities['lng'][i], api_key=api_key))
        
        # Convert to json
        js = json.loads(request.text)

        # Append data to dataframes
        df_main = df_main.append(pd.DataFrame.from_dict(js['main'], orient='index').transpose())
        df_coord = df_coord.append(pd.DataFrame.from_dict(js['coord'], orient='index').transpose())
        df_weather = df_weather.append(pd.DataFrame.from_dict(js['weather'][0], orient='index').transpose())

        # Concatenate dataframes
        df_full = pd.concat([df_main, df_coord, df_weather], axis=1)
    
    # Rename columns
    df_full.rename(columns={'id': 'weather_id', 'main': 'weather_main', 'description': 'weather_description'}, inplace=True)
    
    # Drop columns
    df_full.drop('icon', axis=1, inplace=True)
    
    return df_full

def join_to_worldcities():
    openmap_df = extract_openmap()

    # Join openmap_df to worldcities
    worldcities = clean_worldcities()
    df = pd.merge(worldcities, openmap_df, left_on=['lat','lng'], right_on=['lat','lon'], how='left')
    
    # Drop columns
    df.drop(['lon'], axis=1, inplace=True)
    
    # Rname and reorder columns
    df.rename(columns={'id': 'city_id'}, inplace=True)
    df = df.loc[:,['city_id','city_ascii','city','country','lat','lng','iso3','population','weather_id','weather_main','weather_description','temp','feels_like','temp_min','temp_max','pressure','humidity','sea_level','grnd_level']]
    
    return df
d = join_to_worldcities()
d.dtypes

def load_to_postgres():
    df = join_to_worldcities()
    conn_string = "postgresql://postgres:postgres@localhost:5432/postgres"
    engine = create_engine(conn_string)
    conn = engine.connect()
    conn1 = psycopg2.connect(
        database="postgres",
        user="postgres",
        password="postgres",
        host="localhost",
        port='5432'
    )
    conn1.autocommit = True
    cursor = conn1.cursor()
    cursor.execute('drop table if exists openweather')
    sql = """
        CREATE TABLE openweather (
            city_id int,
            city_ascii varchar(255),
            city varchar(255),
            country varchar(255),
            lat float,
            lng float,
            iso3 varchar(255),
            population float,
            weather_id int,
            weather_main varchar(255),
            weather_description varchar(255),
            temp float,
            feels_like float,
            temp_min float,
            temp_max float,
            pressure float,
            humidity float,
            sea_level float,
            grnd_level float
        )
    """
    cursor.execute(sql)
    df.to_sql('openweather', conn, if_exists='append', index=False)
    conn1.commit()
    conn1.close()