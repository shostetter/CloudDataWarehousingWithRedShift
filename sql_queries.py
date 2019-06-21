import configparser


# CONFIG FILE VARIABLES
config = configparser.ConfigParser()
config.read('dwh.cfg')


# MAKE SURE TABLES DON'T EXIST BEFOR CREATING 
staging_events_table_drop = "DROP TABLE IF EXISTS stg_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS stg_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;" 
user_table_drop = "DROP TABLE IF EXISTS dim_user;"
song_table_drop = "DROP TABLE IF EXISTS dim_song;"
artist_table_drop = "DROP TABLE IF EXISTS dim_artist;"
time_table_drop = "DROP TABLE IF EXISTS dim_time;"

# CREATE STAGING TABLES
staging_events_table_create= ("""
CREATE TABLE stg_events (
artist varchar,
auth varchar,
firstname varchar,
gender varchar(1),
iteminsession int, 
lastname  varchar,
length decimal, 
level varchar,
location varchar,
method varchar(3),
page  varchar,
registration decimal, 
sessionid int,
song  varchar,
status varchar,
ts varchar,
useragent varchar,
userid int
)
""")

staging_songs_table_create = ("""
CREATE TABLE stg_songs (
num_songs int,
artist_id varchar, 
artist_latitude float, 
artist_longitude float,
artist_location varchar, 
artist_name varchar, 
song_id varchar, 
title varchar, 
duration float,
year int
)
""")

# CREATE DIMENSIONAL TABLES
songplay_table_create = ("""
CREATE TABLE songplay (
songplay_id INT IDENTITY(0,1) PRIMARY KEY not null distkey, 
start_time timestamp not null sortkey,  
user_id int not null, 
level varchar, 
song_id varchar not null, 
artist_id varchar not null, 
session_id int, 
location varchar, 
user_agent varchar
)
""")

user_table_create = ("""
CREATE TABLE dim_user (
user_id int PRIMARY KEY  not null   sortkey, 
first_name varchar, 
last_name varchar, 
gender varchar(1), 
level varchar
)
""")

song_table_create = ("""
CREATE TABLE dim_song (
song_id varchar  PRIMARY KEY  not null  sortkey,  
title varchar, 
artist_id varchar not null, 
year int, 
duration float
)
""")

artist_table_create = ("""
CREATE TABLE dim_artist (
artist_id varchar PRIMARY KEY  not null   sortkey,  
name varchar, 
location varchar, 
lattitude float, 
longitude float
)
""")

time_table_create = ("""
CREATE TABLE dim_time (
start_time timestamp  PRIMARY KEY  not null  sortkey,  
hour int, 
day int, 
week int, 
month int, 
year int, 
weekday varchar
)
""")

staging_events_copy = ("""
COPY stg_events FROM {}
CREDENTIALS 'aws_iam_role={}' 
REGION 'us-west-2' 
FORMAT AS JSON {};
""").format(config.get('S3', 'LOG_DATA'), 
            config.get('IAM_ROLE', 'ARN'),
            config.get('S3','LOG_JSONPATH')
           )

staging_songs_copy = ("""
COPY stg_songs 
FROM {}
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2' 
FORMAT AS JSON 'auto';
""").format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# POPULATE DIMENSIONAL TABLES
songplay_table_insert = ("""
INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT  
timestamp 'epoch' + ts::float/1000 * interval '1 second' as start_time,
userid as user_id, 
level, 
song_id, 
artist_id, 
sessionid as session_id, 
location, 
useragent as user_agent
FROM stg_events e  
JOIN stg_songs s 
    ON e.artist = s.artist_name AND e.song = s.title
WHERE userid is not null
""")

user_table_insert = ("""
INSERT INTO dim_user (user_id, first_name, last_name, gender, level)
SELECT DISTINCT 
userid, 
firstname,
lastname, 
gender, 
level
FROM stg_events e 
WHERE e.page='NextSong'
AND userid is not null
""")

song_table_insert = ("""
INSERT INTO dim_song (song_id, title, artist_id, year, duration)
SELECT DISTINCT 
song_id,  
title, 
artist_id, 
year, 
duration 
FROM stg_songs s
""")

artist_table_insert = ("""
INSERT INTO dim_artist (artist_id, name, location, lattitude, longitude)
SELECT DISTINCT 
artist_id, 
artist_name, 
artist_location, 
artist_latitude, 
artist_longitude
FROM stg_songs s
""")

time_table_insert = ("""

INSERT INTO dim_time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT 
start_time,  
date_part('hour', start_time) as hour, 
date_part('day', start_time) as  day, 
date_part('week',start_time) as week, 
date_part('month', start_time) as month, 
date_part('year', start_time) as year, 
date_part('dow', start_time) as  weekday
FROM songplay
""")

# QUERIES; CALLED FROM create_tables.py AND etl.py
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
