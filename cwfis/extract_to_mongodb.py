import zipfile
import os
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

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables from the .env file
load_dotenv()

def download_zip(url, save_path):
    """Downloads a ZIP file from a URL to the specified local path."""
    response = requests.get(url)
    with open(save_path, 'wb') as file:
        file.write(response.content)

def extract_zip(zip_path, extract_to):
    """Extracts the ZIP file to the specified directory."""
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)

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

def list_zip_files(base_url):
    """Lists all ZIP files in the directory at the given base URL."""
    response = requests.get(base_url)
    response.raise_for_status()
    html_content = response.text

    # Assuming the server lists files as links in the HTML
    soup = BeautifulSoup(html_content, 'html.parser')
    zip_files = [urljoin(base_url, link.get('href')) for link in soup.find_all('a') if link.get('href').endswith('.zip')]
    return zip_files

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
def read_shapefile_and_insert(extract_to, collection, year):
    shapefile_base = year + "_hotspots"  # Adjust as necessary
    required_files = [f"{shapefile_base}.shp", f"{shapefile_base}.shx", f"{shapefile_base}.dbf"]
    
    # List the files in the extraction directory
    extracted_files = os.listdir(extract_to)
    print("Extracted files:", extracted_files)
    
    for required_file in required_files:
        if required_file not in extracted_files:
            raise FileNotFoundError(f"Missing required shapefile component: {required_file}")
    
    shp = shapefile.Reader(os.path.join(extract_to, shapefile_base))
    for sr in shp.shapeRecords():
        record = sr.record.as_dict()
        collection.insert_one(record)
        print("len(record):", len(record))

def main():
    base_url = os.getenv('BASE_URL')
    start_year = os.getenv("START_YEAR")
    end_year = os.getenv("END_YEAR")
    
    # Get the directory where the current script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    extract_to = script_dir
    
    # List and filter zip files
    zip_files = list_zip_files(base_url)
    filtered_zip_files = filter_zip_files_by_year(zip_files, int(start_year), int(end_year))

    # MongoDB setup
    mongo_uri = os.getenv("MONGO_URI")
    mongo_db = os.getenv("DB_NAME")
    mongo_collection = os.getenv("COLLECTION_NAME")
    
    client = MongoClient(mongo_uri)
    db = client[mongo_db]
    # Create a collection for each year
    for zip_url in filtered_zip_files:
        year = os.path.basename(zip_url).split('_')[0]
        collection = db[mongo_collection + f"_{year}"]
        print(f"Creating collection for year {year}")
        read_shapefile_and_insert(extract_to, collection, year)

if __name__ == "__main__":
    main()
