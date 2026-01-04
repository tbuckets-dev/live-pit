import fastf1
import pandas as pd
import os
from datetime import datetime
import argparse
import pandera.pandas as pa
from pandera import Column, Check

# Parse the arguments
parser = argparse.ArgumentParser()
parser.add_argument('--year', type=int, required=True, help='The year of the race')
parser.add_argument('--race', type=str, required=True, help='The name of the race')
parser.add_argument('--driver', type=str, required=True, help='The driver code')
args = parser.parse_args()

# Add the cache directory to the cache
fastf1.Cache.enable_cache(os.path.join(os.path.dirname(__file__), '../../data/cache'))

# Define the expected schema for the race data
race_data_schema = pa.DataFrameSchema(
    {
        'LapNumber': Column(float, Check.in_range(1, 100)),
        'Compound': Column(str, Check.isin(['SOFT', 'MEDIUM', 'HARD', 'INTERMEDIATE', 'WET'])),
        'LapTime': Column(int),
        'LapTime_Sec': Column(float, Check.gt(0)),
        'driver_code': Column(str),
        'race_name': Column(str),
        'year': Column(int)
    }
)

# Function to ingest the race data
def ingest_race_data(year, race, driver):

    # Get the session data
    session = fastf1.get_session(year, race, 'R')
    session.load(laps=True, telemetry=False, weather=False)

    # Get the laps data
    laps = session.laps.pick_drivers([driver])
    df = pd.DataFrame(laps[['LapNumber', 'Compound', 'LapTime']])

    # Convert the LapTime to seconds
    df['LapTime_Sec'] = df['LapTime'].dt.total_seconds()
    df['LapTime'] = df['LapTime'].astype('int64')

    # Add the driver code and race name and year
    df['driver_code'] = driver
    df['race_name'] = race
    df['year'] = year

    # Return the dataframe. This is used for initial testing and validation.
    print(df)
    return df

# Ingest the race data
df = ingest_race_data(args.year, args.race, args.driver)

# Validate the dataframe
try:
    race_data_schema.validate(df)
    print("Dataframe is valid")
except pa.errors.SchemaErrors as exc:
    print(f"Validation failed with errors: {exc.failure_cases}")

df.to_csv(f'../../data/raw/race_data_{args.year}_{args.race}_{args.driver}.csv', index=False)
