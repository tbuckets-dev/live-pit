import fastf1
import pandas as pd
import os
from datetime import datetime

fastf1.Cache.enable_cache(os.path.join(os.path.dirname(__file__), '../../data/cache'))

def ingest_race_data(race_params):

    year = race_params['year']
    race = race_params['race']
    driver = race_params['driver']

    session = fastf1.get_session(year, race, 'R')
    session.load(laps=True, telemetry=False, weather=False)

    laps = session.laps.pick_driver(driver)
    df = laps[['LapNumber', 'Compound', 'LapTime']]

    df['LapTime_Sec'] = df['LapTime'].dt.total_seconds()
    df['LapTime'] = df['LapTime'].astype('int64')
    df['driver_code'] = driver
    df['race_name'] = race
    df['year'] = year

    print(df)

ingest_race_data({'year': 2025, 'race': 'Silverstone', 'driver': 'HAM'})