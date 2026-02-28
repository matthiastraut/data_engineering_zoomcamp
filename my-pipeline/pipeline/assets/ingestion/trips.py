"""@bruin
name: ingestion.trips
type: python
image: python:3.12
connection: gcp-default

materialization:
  type: table
  strategy: append

columns:
  - name: VendorID
    type: integer
  - name: pickup_datetime
    type: timestamp
    checks:
    - name: not_null
  - name: dropoff_datetime
    type: timestamp
  - name: passenger_count
    type: integer
  - name: trip_distance
    type: float
  - name: RatecodeID
    type: integer
  - name: store_and_fwd_flag
    type: string
  - name: pickup_location_id
    type: integer
  - name: dropoff_location_id
    type: integer
  - name: payment_type
    type: integer
  - name: fare_amount
    type: float
  - name: extra
    type: float
  - name: mta_tax
    type: float
  - name: tip_amount
    type: float
  - name: tolls_amount
    type: float
  - name: improvement_surcharge
    type: float
  - name: total_amount
    type: float
  - name: congestion_surcharge
    type: float
  - name: trip_type
    type: integer
  - name: taxi_type
    type: string
  - name: extracted_at
    type: timestamp

@bruin"""

import os
import json
import requests
import pandas as pd
import io
from datetime import datetime
from dateutil.relativedelta import relativedelta

def materialize():
    # Fetch environment variables managed by Bruin
    start_date_str = os.getenv("BRUIN_START_DATE")
    end_date_str = os.getenv("BRUIN_END_DATE")
    
    # Parse Bruin variables
    vars_json = os.getenv("BRUIN_VARS", "{}")
    pipeline_vars = json.loads(vars_json)
    taxi_types = pipeline_vars.get("taxi_types", ["yellow", "green"])
    
    # Use 2024 as default if not specified, based on user context
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d") if start_date_str else datetime(2024, 1, 1)
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d") if end_date_str else datetime(2024, 1, 31)
    
    # Cap the end_date at Nov 2025 as recommended in README
    data_availability_limit = datetime(2024, 1, 31)
    if end_date > data_availability_limit:
        print(f"Capping end_date from {end_date.strftime('%Y-%m-%d')} to 2025-11-30")
        end_date = data_availability_limit
    
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    all_dfs = []

    # Iterate through each taxi type and each month in the window
    for taxi_type in taxi_types:
        current_date = start_date.replace(day=1)
        while current_date <= end_date:
            year = current_date.year
            month = f"{current_date.month:02d}"
            
            file_name = f"{taxi_type}_tripdata_{year}-{month}.parquet"
            url = f"{base_url}/{file_name}"
            
            print(f"Fetching data from: {url}")
            try:
                response = requests.get(url)
                response.raise_for_status()
                
                # Load parquet data into pandas
                df = pd.read_parquet(io.BytesIO(response.content))

                # Normalize columns: NYC Taxi uses different prefixes (tpep/lpep) for yellow/green cabs
                rename_map = {
                    'tpep_pickup_datetime': 'pickup_datetime',
                    'tpep_dropoff_datetime': 'dropoff_datetime',
                    'lpep_pickup_datetime': 'pickup_datetime',
                    'lpep_dropoff_datetime': 'dropoff_datetime',
                    'PULocationID': 'pickup_location_id',
                    'DOLocationID': 'dropoff_location_id',
                }
                df = df.rename(columns=rename_map)

                # Ensure only the columns we need are kept to save memory and match destination schema
                # (Removing columns that aren't common across both taxi types or aren't in our schema)
                required_cols = [
                    'VendorID', 'pickup_datetime', 'dropoff_datetime', 'passenger_count',
                    'trip_distance', 'RatecodeID', 'store_and_fwd_flag', 'pickup_location_id',
                    'dropoff_location_id', 'payment_type', 'fare_amount', 'extra', 'mta_tax',
                    'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount',
                    'congestion_surcharge'
                ]
                
                # Check for columns and fill with None if they don't exist (e.g. trip_type)
                if 'trip_type' in df.columns:
                    required_cols.append('trip_type')
                else:
                    df['trip_type'] = None
                    required_cols.append('trip_type')

                df = df[required_cols].copy()
                
                # Add metadata columns
                df['taxi_type'] = taxi_type
                df['extracted_at'] = datetime.now()
                
                all_dfs.append(df)
                print(f"Successfully fetched {len(df)} rows for {taxi_type} in {year}-{month}")
            except Exception as e:
                print(f"Failed to fetch {url}: {e}")
                
            current_date += relativedelta(months=1)

    if not all_dfs:
        print("No data found for the given criteria.")
        return pd.DataFrame()

    # Union all dataframes
    final_df = pd.concat(all_dfs, ignore_index=True)
    
    return final_df


