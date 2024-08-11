# pip install supabase pandas requests

# Import Libraries
import os
import sys
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import geopandas as gpd
from pymongo import MongoClient
from dotenv import load_dotenv
from sklearn.preprocessing import StandardScaler, OneHotEncoder
import math
# Add the 'scripts' directory to the Python path
sys.path.append(os.path.abspath(os.path.join('..', 'scripts')))
import extract_to_mongodb as etm
import db_utils as dbu
import pickle
import time
from tqdm import tqdm
collection_name = os.getenv('COLLECTION_NAME_FEATUREENGINEERED_FINAL')
naturalearth_lowres = os.getenv('NATURALEARTH_SHAPEFILE_PATH')
# CACHE_FILE = 'geocodecache.pkl'
print(f"Collection Name: {collection_name}")
Load the Data
# Load the cleaned data
df_hist = dbu.load_all_data_from_mongodb(collection_name)
df_historical=df_hist
# df_historical.shape
# df_historical.head(5)
# df_historical.info()
# Normalize the data to ensure consistency
df_historical['rep_date'] = pd.to_datetime(df_historical['rep_date']).dt.strftime('%Y-%m-%d')

# Convert 'rep_date' back to datetime to filter by month and year
df_historical['rep_date'] = pd.to_datetime(df_historical['rep_date'])

# Filter for records from July to December 2023
df_historical = df_historical[(df_historical['rep_date'].dt.year == 2023) & 
                              (df_historical['rep_date'].dt.month >= 7) & 
                              (df_historical['rep_date'].dt.month <= 12)]


df_historical.head(5)
# df_historical = df_historical.reset_index()
# df_historical = (df_historical.merge((df_historical[['rep_date']].drop_duplicates(ignore_index=True).rename_axis('time_idx'))\
#                      .reset_index(), on = ['rep_date'])).drop("rep_date", axis=1)
df_historical.head(5)
cols_to_keep = ['locality','elev', 'month', 'day', 'year', 'cfb', 'temp', 'wd', 'rh', 'pcuring', 'ros', 'hfi', 'tfc0', 'sfl', 'bui', 'cfl', 'sfc0', 'dmc', 'sfc', 'bfc', 'tfc', 'isi']
df_historical = df_historical[cols_to_keep]
# df_historical['cfb'] = df_historical['cfb'].astype(float)
df_historical['fire_occurrence'] = (df_historical['cfb'] > 0).astype(int)
def create_lagged_features(df, features, lags, fill_method='ffill_bfill'):
    for feature in features:
        for lag in lags:
            df[f'{feature}_lag{lag}'] = df[feature].shift(lag)
            if fill_method == 'ffill_bfill':
                df[f'{feature}_lag{lag}'] = df[f'{feature}_lag{lag}'].fillna(method='ffill').fillna(method='bfill')
            elif fill_method == 'ffill':
                df[f'{feature}_lag{lag}'] = df[f'{feature}_lag{lag}'].fillna(method='ffill')
            elif fill_method == 'bfill':
                df[f'{feature}_lag{lag}'] = df[f'{feature}_lag{lag}'].fillna(method='bfill')
    return df


# Define lag features
features_to_lag = ['cfb', 'dmc', 'temp', 'tfc', 'ros', 'pcuring', 'bfc', 'hfi']
lags = [1, 2, 3, 5, 6, 7]

# Create lagged features
df_historical = create_lagged_features(df_historical, features_to_lag, lags)

# Drop rows with NaN values created by lagging
# df_historical.dropna(inplace=True)
num_rows_with_nan = df_historical[cols].isnull().sum(axis=1).sum()
print(f"Number of rows with NaN values: {num_rows_with_nan}")
df_historical.head(5)
duplicates = df.duplicated()
print("Duplicate rows:")
print(df[duplicates])
df_historical.isnull().sum()

df_historical.to_csv('historical_07_12.csv', index=False)
# Save the cleaned data to mongodb
# dbu.insert_df_only_to_mongodb(df_historical, 'wildfire_collection_2023_07_12')
import pandas as pd
import requests
from supabase import create_client, Client

# Create a Supabase client
supabase_url = 'https://owflvfhmlpletxyjyhwv.supabase.co'
supabase_key = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im93Zmx2ZmhtbHBsZXR4eWp5aHd2Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3MjI4ODQ0MzksImV4cCI6MjAzODQ2MDQzOX0.cuJoRQNwckv6fAIHKZnBjTW2akcqo9iMA5qz8ocS62A'
supabase_client = Client(supabase_url, supabase_key)

# Insert the JSON data into the Supabase table
# Create the table in Supabase
table_name = 'historical_07_12'
# Function to map pandas dtypes to SQL types
def map_dtype_to_sql(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return "INTEGER"
    elif pd.api.types.is_float_dtype(dtype):
        return "FLOAT"
    elif pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "TIMESTAMP"
    else:
        return "TEXT"


# Generate SQL CREATE TABLE query
columns_sql = ",\n    ".join([f"{col} {map_dtype_to_sql(dtype)}" for col, dtype in zip(df_historical.columns, df_historical.dtypes)])
create_table_query = f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    {columns_sql}
);
"""
print(create_table_query)
# Use the Supabase REST API to execute the SQL query
headers = {
    "apikey": supabase_key,
    "Authorization": f"Bearer {supabase_key}",
    "Content-Type": "application/json"
}

# API endpoint for executing SQL in Supabase
sql_endpoint = f"{supabase_url}/rest/v1/rpc/execute_sql"

# Execute the SQL query
response = requests.post(sql_endpoint, json={"sql": create_table_query}, headers=headers)

# Convert DataFrame to dictionary records
data_to_insert = df_historical.to_dict(orient="records")
# Convert 'bui' column to integer
df_historical['bui'] = df_historical['bui'].astype(int)

# Convert DataFrame to dictionary records
data_to_insert = df_historical.to_dict(orient="records")

# Insert the data into the table
insert_response = supabase_client.table(table_name).insert(data_to_insert).execute()
