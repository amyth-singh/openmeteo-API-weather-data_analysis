import openmeteo_requests
import requests_cache
import pandas as pd 
import matplotlib.pyplot as plt
from retry_requests import retry
from datetime import datetime
import yaml
import mysql.connector
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, DateTime, Float, Date, TIMESTAMP

def fetch_data_api(url, start_date, end_date, latitude, longitude):
    cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)
    params = {
        "latitude": latitude, "longitude": longitude,
        "start_date": start_date, "end_date": end_date,
        "daily": ["temperature_2m_max", "temperature_2m_min", "temperature_2m_mean"]
    }
    responses = openmeteo.weather_api(url, params=params)
    response = responses[0]

    # Process daily historical data from API
    daily = response.Daily()
    max_temp = daily.Variables(0).ValuesAsNumpy()
    min_temp = daily.Variables(1).ValuesAsNumpy()
    avg_temp = daily.Variables(2).ValuesAsNumpy()

    data = {
        "date": pd.date_range(
            start=pd.to_datetime(daily.Time(), unit="s", utc=True),
            end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=daily.Interval()),
            inclusive="left"
        ),
        "max_temp": max_temp, "min_temp": min_temp, "avg_temp": avg_temp
    }

    df = pd.DataFrame(data=data)
    return df

###############################################################################

def process_data(df):
    df['date'] = pd.to_datetime(df['date'])
    df['year'] = df['date'].dt.year
    df['year'] = pd.to_datetime(df['year'], format='%Y')
    avg_temp_by_year = df.groupby('year')['max_temp'].mean()
    df['avg_temp_for_the_year'] = df['year'].map(avg_temp_by_year)

    # Additional processing steps
    df[['max_temp', 'min_temp', 'avg_temp']] = df[['max_temp', 'min_temp', 'avg_temp']].astype(float).round(3)
    df[['f_max_temp', 'f_min_temp', 'f_avg_temp']] = ((df[['max_temp', 'min_temp', 'avg_temp']] * 9/5) + 32).astype(float).round(3)
    df[['max_temp_change', 'min_temp_change', 'avg_temp_change']] = df[['max_temp', 'min_temp', 'avg_temp']].diff().astype(float).round(3)

    return df

#####################################################################

def export_to_csv(df, file_name):
    df['csv_creation_time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df.to_csv(file_name, index=False)
    print(f"Data exported to {file_name} successfully.")

####################################################################

def get_database_credentials():
    try:
        with open("config.yaml", 'r') as stream:
            credentials = yaml.safe_load(stream)
            return credentials['database']
    except FileNotFoundError:
        print("Config file not found!")
        return None

def get_schema_from_file():
    try:
        with open("schema.yaml", 'r') as stream:
            schema = yaml.safe_load(stream)
            return schema
    except FileNotFoundError:
        print("Schema file not found!")
        return None

def table_exists(cursor, table_name):
    cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
    return cursor.fetchone() is not None

def create_table():
    db_credentials = get_database_credentials()

    if db_credentials:
        schema = get_schema_from_file()
        if schema:
            try:
                conn = mysql.connector.connect(**db_credentials)
                cursor = conn.cursor()
                table_name = next(iter(schema))
                if not table_exists(cursor, table_name):
                    columns = schema[table_name]
                    create_table_query = f"CREATE TABLE {table_name} ("
                    for column in columns:
                        column_name = column['name']
                        column_type = column['type']
                        create_table_query += f"{column_name} {column_type}, "
                    create_table_query = create_table_query[:-2]
                    create_table_query += ")"
                    cursor.execute(create_table_query)
                    print("Table created successfully")
                else:
                    print("Table already exists")
            except mysql.connector.Error as e:
                print(f"Error creating table: {e}")
            finally:
                if conn.is_connected():
                    cursor.close()
                    conn.close()
                    print("MySQL connection is closed")

##################################################################

def load_df_to_mysql(processed_df, table_name, db_config):
    connection_string = f"mysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    engine = create_engine(connection_string)
    processed_df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
    print(f"DataFrame successfully loaded into MySQL table '{table_name}'")

###########################################################

def main():
    original_df = fetch_data_api("https://archive-api.open-meteo.com/v1/archive", "2021-03-19", "2024-03-19", 52.52, 13.41)
    processed_df = process_data(original_df)
    print(processed_df.head())
    export_to_csv(processed_df, "weather_data_output.csv")
    create_table()
    db_config = get_database_credentials()
    load_df_to_mysql(processed_df, 'weather_data', db_config)

if __name__ == "__main__":
    main()


