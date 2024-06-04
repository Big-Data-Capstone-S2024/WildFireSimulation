import zipfile
import os
import json
import requests
from pymongo import MongoClient
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import shapefile
from pymongo.errors import ConnectionFailure
import logging
import time
from dotenv import load_dotenv
import os
from bson import BSON

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def list_zip_files(base_url):
    """Lists all ZIP files in the directory at the given base URL."""
    response = requests.get(base_url)
    response.raise_for_status()
    html_content = response.text

    # Assuming the server lists files as links in the HTML
    soup = BeautifulSoup(html_content, 'html.parser')
    zip_files = [urljoin(base_url, link.get('href')) for link in soup.find_all('a') if link.get('href').endswith('.zip')]
    return zip_files

def download_zip(url, save_path):
    """Downloads a ZIP file from a URL to the specified local path."""
    response = requests.get(url)
    with open(save_path, 'wb') as file:
        file.write(response.content)

def extract_zip(zip_path, extract_to):
    """Extracts the ZIP file to the specified directory."""
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        shp_files = [file for file in zip_ref.namelist() if file.endswith('.shp')]
        zip_ref.extractall(extract_to, members=shp_files)
        
def shape_record_to_geojson(shape_record):
    """Convert a shapefile record to a GeoJSON feature."""
    geom = shape_record.shape.__geo_interface__
    properties = shape_record.record.as_dict()
    feature = {
        "type": "Feature",
        "geometry": geom,
        "properties": properties
    }
    return feature

def connect_to_mongo(uri, retries=5, delay=5):
    for attempt in range(1, retries + 1):
        try:
            logger.info(f"Attempt {attempt}: Connecting to MongoDB...")
            client = MongoClient(uri, serverSelectionTimeoutMS=5000)
            client.server_info()  # Force connection on a request as the connect=True parameter of MongoClient seems to be useless here
            logger.info("Connected to MongoDB successfully.")
            return client
        except ConnectionFailure as e:
            logger.error(f"Attempt {attempt} failed: {e}")
            if attempt < retries:
                logger.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                logger.error("All retry attempts failed.")
                raise

def insert_data_to_mongodb(data, db_name, collection_name, mongo_uri):
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]
    print('inserting....')
    collection.insert_many(data)

def filter_zip_files_by_year(zip_files, start_year, end_year):
    """Filters ZIP files based on the year range in their names."""
    filtered_files = []
    for file_url in zip_files:
        # Extract year from file name, assuming format 'YYYY_...'
        file_name = os.path.basename(file_url)
        try:
            year = int(file_name.split('_')[0])
            if start_year <= year <= end_year:
                filtered_files.append(file_url)
        except ValueError:
            pass
    return filtered_files

# Function to read shapefiles and insert data into MongoDB
def read_shapefile_and_insert(shapefile_path, collection):
    print('reading shapefile')
    print(shapefile_path)
    for filename in os.listdir(shapefile_path):
        with shapefile.Reader(os.path.join(shapefile_path, filename)) as shp:
            fields = [field[0] for field in shp.fields[1:]]  # Extract field names
            for sr in shp.shapeRecords():
                record = dict(zip(fields, sr.record))  # Create a dictionary from field names and record values
                record['geometry'] = sr.shape.__geo_interface__  # Add geometry data to the record
                collection.insert_one(record)  # Insert record into MongoDB collection

# Load environment variables from the .env file
load_dotenv()

# Access the variables
base_url = os.getenv('BASE_URL')
extract_to = os.getenv('EXTRACT_TO')
db_name = os.getenv('DB_NAME')
collection_name = os.getenv('COLLECTION_NAME')
mongo_uri = os.getenv('MONGO_URI')
start_year = os.getenv('START_YEAR')
end_year = os.getenv('END_YEAR')

client = connect_to_mongo(mongo_uri)

# Access the database and collection
db = client[db_name]
# @TODO make the collection with the corresponding year
collection = db[collection_name]

# Steps
zip_files = list_zip_files(base_url)
filtered_zip_files = filter_zip_files_by_year(zip_files, int(start_year), int(end_year))

for zip_url in filtered_zip_files:
    zip_path = os.path.join('downloads', os.path.basename(zip_url))
    os.makedirs('downloads', exist_ok=True)
    download_zip(zip_url, zip_path)
    extract_zip(zip_path, extract_to) 
    read_shapefile_and_insert(extract_to, collection)
