import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table"
user_table_drop = "DROP TABLE IF EXISTS user_table"
song_table_drop = "DROP TABLE IF EXISTS song_table"
artist_table_drop = "DROP TABLE IF EXISTS artist_table"
time_table_drop = "DROP TABLE IF EXISTS time_table"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events(
    eventId INTEGER IDENTITY(0,1) PRIMARY KEY,
    artist VARCHAR,
    auth VARCHAR, 
    firstName VARCHAR,
    gender VARCHAR,   
    itemInSession INTEGER,
    lastName VARCHAR,
    length FLOAT,
    level VARCHAR, 
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration BIGINT,
    sessionId INTEGER,
    song VARCHAR,
    status INTEGER,
    ts BIGINT,
    userAgent VARCHAR,
    userId INTEGER
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    song_id VARCHAR PRIMARY KEY,
    num_songs INTEGER, 
    artist_id VARCHAR, 
    artist_latitude FLOAT, 
    artist_longitude FLOAT, 
    artist_location VARCHAR, 
    artist_name VARCHAR, 
    title VARCHAR, 
    duration FLOAT, 
    year INTEGER
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay_table (
    songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP, 
    user_id INTEGER, 
    level VARCHAR, 
    song_id VARCHAR, 
    artist_id VARCHAR, 
    session_id INTEGER, 
    location VARCHAR, 
    user_agent VARCHAR
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS user_table (
    user_id INTEGER PRIMARY KEY, 
    first_name VARCHAR, 
    last_name VARCHAR, 
    gender VARCHAR, 
    level VARCHAR
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS song_table (
    song_id VARCHAR PRIMARY KEY, 
    title VARCHAR, 
    artist_id VARCHAR, 
    year INTEGER, 
    duration FLOAT

)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist_table (
    artist_id VARCHAR PRIMARY KEY, 
    name VARCHAR, 
    location VARCHAR, 
    latitude FLOAT, 
    longitude FLOAT
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time_table (
    start_time TIMESTAMP PRIMARY KEY, 
    hour INTEGER, 
    day INTEGER, 
    week INTEGER, 
    month INTEGER, 
    year INTEGER, 
    weekday INTEGER
)
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    format as json {}
""").format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
    copy staging_songs FROM {}
    credentials  'aws_iam_role={}'
    region 'us-west-2'
    format as json 'auto' 
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay_table(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT timestamp 'epoch' + se.ts/1000 * interval '1 second' AS start_time,
                se.userId AS user_id,
                se.level AS level,
                ss.song_id AS song_id,
                ss.artist_id AS artist_id,
                se.sessionId AS session_id,
                se.location AS location,
                se.userAgent AS user_agent
FROM staging_songs ss
JOIN staging_events se ON se.artist = ss.artist_name AND se.song = ss.title
WHERE se.page='NextSong';
""")

user_table_insert = ("""
INSERT INTO user_table(user_id, first_name, last_name, gender, level)
SELECT DISTINCT userId AS user_id,
                firstName AS first_name,
                lastName AS last_name,
                gender AS gender,
                level AS level
FROM staging_events
WHERE page='NextSong';
""")

song_table_insert = ("""
INSERT INTO song_table(song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id AS song_id,
                title AS title,
                artist_id AS artist_id,
                year AS year,
                duration AS duration
FROM staging_songs;
""")

artist_table_insert = ("""
INSERT INTO artist_table(artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id AS artist_id,
                artist_name AS name,
                artist_location AS location,
                artist_latitude AS latitude,
                artist_longitude AS longitude
FROM staging_songs;
""")

time_table_insert = ("""
INSERT INTO time_table(start_time, hour, day, week, month, year, weekday)
SELECT distinct s.start_time AS start_time,
                EXTRACT(hour from s.start_time) AS hour,
                EXTRACT(day from s.start_time) AS day,
                EXTRACT(week from s.start_time) AS week,
                EXTRACT(month from s.start_time) AS month,
                EXTRACT(year from s.start_time) AS year,
                EXTRACT(weekday from s.start_time) AS weekday
FROM songplay_table s;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
