import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
LOG_DATA = config.get("S3", "LOG_DATA")
SONG_DATA = config.get("S3", "SONG_DATA")
ARN = config.get("IAM_ROLE", "ARN")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS times"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events(
    artist varchar,
    auth varchar,
    first_name varchar,
    gender char,
    item_in_session int,
    last_name varchar,
    length numeric,
    level varchar,
    location text,
    method varchar,
    page varchar,
    registration numeric,
    session_id int,
    song varchar,
    status varchar,
    ts numeric,
    user_agent text,
    user_id int
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    num_songs int,
    artist_id varchar(25),
    artist_latitude varchar,
    artist_longitude varchar,
    artist_location text,
    artist_name varchar,
    song_id varchar,
    title varchar,
    duration numeric,
    year int
);
""")

# Fact table
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
    songplay_id int identity(0,1) primary key,
    start_time timestamp,
    user_id int,
    level varchar,
    song_id varchar(25),
    artist_id varchar(25),
    session_id varchar(25),
    location text,
    user_agent text

);
""")

# Dimension table
song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
    song_id varchar(25) primary key,
    title varchar,
    artist_id varchar(25),
    year int,
    duration numeric
);
""")

# Dimension table
user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
    user_id int primary key,
    first_name varchar,
    last_name varchar,
    gender char,
    level varchar
);
""")

# Dimension table
artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
    artist_id varchar(25) primary key,
    name varchar,
    location varchar,
    latitude varchar,
    longitude varchar
);
""")

# Dimension table
time_table_create = ("""
CREATE TABLE IF NOT EXISTS times(
    start_time timestamp primary key,
    hour int,
    day int,
    week int,
    month int,
    year int,
    weekday varchar
);
""")

# STAGING TABLES
# Need to specify region 'us-west-2' in order to get log file from S3 
staging_events_copy = ("""
COPY staging_events
FROM {}
IAM_ROLE {}
FORMAT as json {}
REGION 'us-west-2';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
COPY staging_songs
FROM {}
IAM_ROLE {}
FORMAT as json 'auto'
REGION 'us-west-2';
""").format(SONG_DATA, ARN)
# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT  TIMESTAMP 'epoch'+ e.ts/1000*interval '1 second' AS start_time,
        e.user_id, 
        e.level, 
        s.song_id, 
        s.artist_id, 
        e.session_id, 
        e.location, 
        e.user_agent
    FROM staging_events e 
    INNER JOIN staging_songs s 
    ON (e.song = s.title AND e.artist = s.artist_name)
    WHERE e.page = 'NextSong' AND e.user_id IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO songs(song_id, title, artist_id, year, duration)
SELECT song_id, title, artist_id, year, duration
FROM staging_songs
WHERE song_id IS NOT NULL
""")

user_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level) 
SELECT user_id, first_name, last_name, gender, level
FROM staging_events
WHERE page = 'NextSong'
""")


artist_table_insert = ("""
INSERT INTO artists(artist_id, name, location, latitude, longitude)
SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs
""")

time_table_insert = ("""
INSERT INTO times(start_time, hour, day, week, month, year, weekday)
SELECT 
        start_time, 
        EXTRACT(hour from start_time) AS hour, 
        EXTRACT(day from start_time) AS day, 
        EXTRACT(week from start_time) AS week, 
        EXTRACT(month from start_time) AS month, 
        EXTRACT(year from start_time) AS year, 
        EXTRACT(weekday from start_time) AS weekday
    FROM songplays
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
