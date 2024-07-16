# db_utils.py
import pandas as pd
import geopandas as gpd
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Get environment variables
db_name = os.getenv('DB_NAME')
mongo_uri = os.getenv('MONGO_URI')
app_name = os.getenv('APP_NAME')
contact_email = os.getenv('CONTACT_EMAIL')
    
def insert_df_only_to_mongodb(df, collection_name):
    try:
        # Create a MongoDB client
        client = MongoClient(mongo_uri)
        
        # Connect to the database
        db = client[db_name]
        
        # Insert data into the collection
        collection = db[collection_name]
        collection.insert_many(df.to_dict(orient='records'))
        # print(f"Inserted {len(df)} records into the collection '{collection_name}'.")

    except ConnectionFailure as e:
        print(f"Could not connect to MongoDB: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Close the connection
        client.close()
        print("Connection closed.")

def insert_data_to_mongodb(csv_path, collection_name):
    try:
        # Create a MongoDB client
        client = MongoClient(mongo_uri)
        
        # Connect to the database
        db = client[db_name]

        # Read the CSV file into a DataFrame
        df = pd.read_csv(csv_path)

        # Insert the data into MongoDB
        collection = db[collection_name]
        collection.insert_many(df.to_dict(orient='records'))

        # print(f"Data from '{csv_path}' inserted into MongoDB collection '{collection_name}' successfully.")

    except ConnectionFailure as e:
        print(f"Could not connect to MongoDB: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

def read_data_from_mongodb(collection_name, query={}):
    try:
        # Create a MongoDB client
        client = MongoClient(mongo_uri)
        
        # Connect to the database
        db = client[db_name]

        # Read data from the specified collection
        collection = db[collection_name]
        data = pd.DataFrame(list(collection.find(query)))

        # print(f"Data from MongoDB collection '{collection_name}' read successfully.")
        return data

    except ConnectionFailure as e:
        print(f"Could not connect to MongoDB: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

def load_data_from_mongodb(collection_name):
    try:
        # Create a MongoDB client
        client = MongoClient(mongo_uri)
        
        # Connect to the database
        db = client[db_name]

        # Read data from the specified collection
        collection = db[collection_name]
        data = list(collection.find())
        df = pd.DataFrame(data)
        gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.lon, df.lat))
        gdf.set_crs(epsg=4326, inplace=True)
        return gdf

    except ConnectionFailure as e:
        print(f"Could not connect to MongoDB: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

def load_all_data_from_mongodb(collection_name):
    try:
        # Create a MongoDB client
        client = MongoClient(mongo_uri)
        
        # Connect to the database
        db = client[db_name]

        # Read data from the specified collection
        collection = db[collection_name]
        data = list(collection.find())
        df = pd.DataFrame(data)
        
        return df

    except ConnectionFailure as e:
        print(f"Could not connect to MongoDB: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

def insert_dataframe_to_mongodb(df, collection_name):
    try:
        # Create a MongoDB client
        client = MongoClient(mongo_uri)
        
        # Connect to the database
        db = client[db_name]
        
        # Drop the index
        df = df.reset_index(drop=True)
        
        # Convert geometry to GeoJSON
        df['geometry'] = df['geometry'].apply(lambda x: x.__geo_interface__)
        
        # Convert DataFrame to a list of dictionaries
        data = df.to_dict(orient='records')
        
        # Insert data into the collection
        collection = db[collection_name]
        collection.insert_many(data)

        # print(f"Inserted {len(data)} records into the collection '{collection_name}'.")

    except ConnectionFailure as e:
        print(f"Could not connect to MongoDB: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Close the connection
        client.close()
        print("Connection closed.")