# config.py
# base_url = input("Enter the base URL: ") #https://cwfis.cfs.nrcan.gc.ca/downloads/hotspots/archive/
base_url ='https://cwfis.cfs.nrcan.gc.ca/downloads/hotspots/archive/'
extract_to = 'extracted_data'
db_name = 'wildfire_db'
collection_name = 'wildfire_collection'
mongo_uri = 'mongodb+srv://wildfiremongodb:passwordatlas@wildfirecluster.vmiph7u.mongodb.net/mongo'
# start_year = int(input("Enter start year: "))
# end_year = int(input("Enter end year: "))
start_year = 2013
end_year = 2013


