# Sparkify: Amazon Redshift Data Warehouse with 
## Background:
The music user base in Sparkify's streaming service has been growing, necessitating the move of the data to the cloud. The user activity logs and the song metadata (both in JSON format) are already stored on S3. 
### Raw Datasets
**Song Dataset**
The song data is from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.
- song_data/A/B/C/TRABCEI128F424C983.json
- song_data/A/A/B/TRAABJL12903CDCF1A.json  
And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.  

{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
  
**Log Dataset**
The Spariky streaming app generates activity logs partitioned by year and month. For example, here are filepaths to two files in this dataset.
- log_data/2018/11/2018-11-12-events.json
- log_data/2018/11/2018-11-13-events.json
  
And below is an example of what the data in a log file, 2018-11-12-events.json, looks like.  
<img src="log-data.png">   

# ETL pipeline 
This pipeline extracts data from S3, stages the data in Redshift, and transforms the data into a set of dimensional tables for the analytics team to continue finding insights in what songs the users are listening to. 
## Running the ETL pipeline
1. Complete the configuration file dwh.cfg making sure to include the AWS key and secret
1. If the Redshift cluster has not already been set up, run the CreateDatabase.ipynb Jupyter Notebook. **Note:  This include code to delete the cluster**
 - The CreateDatabase Notebook includes code to set up the cluster, the IAM role, and the database
  - This notebook also opens the TCP port for external connections and tests the connection 

1. Set up the database schema by running the create_tables.py file
 - This script will **drop tables if they already exist** and create the 2 staging tables and the 5 analytics tables
 - The create_tables.py file will call SQL queries in the sql_queries.py file
 - The analytics tables are distributed using the join keys Songplay. songplay_id is the distribution key with start_time, user_id, song_id, and artist_id as sort keys. While the Dimension tables are small enough to be stored with the ALL distribution method , distributing all tables allows for growth and reduces the need for shuffling on joins

1. Once the database is set up run the ely.py file
 - The ETL script will first load the data from S# into staging tables
 - Once the staging tables have been populated the ETL script will generate the 5 analytics tables based on the data in the staging tables
1. Finally use the QA_Data.ipynb notebook to verify that the data has been successfully and loaded into the data warehouse. This notebook provides a quick look at the data as well as a few sample analytics queries. 
 - Popular songs by user gender
 - Activity by day of the week
 - Average number of songs streamed per user
 - Distribution of the number of songs per user

  
