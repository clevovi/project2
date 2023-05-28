import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stage_event"
staging_songs_table_drop = "DROP TABLE IF EXISTS stage_song"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS stage_event (
artist TEXT,
auth TEXT,
first_name TEXT,
gender TEXT,
item_in_session TEXT,
last_name TEXT,
length FLOAT,
level TEXT,
location TEXT,
method TEXT,
page TEXT,
registration FLOAT,
session_id TEXT,
song TEXT,
status INTEGER,
ts BIGINT,
user_agent TEXT,
user_id TEXT
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS stage_song (
song_id TEXT,
title TEXT,
duration FLOAT,
year INTEGER,
artist_id TEXT,
artist_name TEXT,
artist_latitude TEXT,
artist_longitude TEXT,
artist_location TEXT,
num_songs INTEGER
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay (
songplay_id INTEGER IDENTITY(1,1)PRIMARY KEY,
start_time TIMESTAMP NOT NULL SORTKEY,
user_id TEXT DISTKEY,
level TEXT,
song_id TEXT,
artist_id TEXT,
session_id TEXT,
location TEXT,
user_agent TEXT
) diststyle key;
""")

#start_time TIMESTAMP NOT NULL SORTKEY,
user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
user_id TEXT PRIMARY KEY SORTKEY,
first_name TEXT,
last_name TEXT,
gender TEXT,
level TEXT
) diststyle all;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS song (
song_id TEXT PRIMARY KEY SORTKEY,
title TEXT,
artist_id TEXT,
year INTEGER,
duration FLOAT
)diststyle all;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist (
artist_id TEXT PRIMARY KEY SORTKEY,
name TEXT,
location TEXT,
latitude TEXT,
longitude TEXT
) diststyle all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
start_time TIMESTAMP PRIMARY KEY SORTKEY,
hour INTEGER,
day TEXT,
week TEXT,
month TEXT,
year INTEGER,
weekday TEXT
)diststyle all;
""")

# STAGING TABLES

staging_events_copy = (
"""
copy stage_event from {}
credentials 'aws_iam_role={}'
json {};
"""
).format(
config.get("S3", "LOG_DATA"),
config.get("IAM_ROLE", "ARN"),
config.get("S3", "LOG_JSONPATH"))

staging_songs_copy = (
"""
copy stage_song from {}
credentials 'aws_iam_role={}'
json 'auto';
"""
).format(config.get("S3", "SONG_DATA"), config.get("IAM_ROLE", "ARN"))


# FINAL TABLES


songplay_table_insert = ("""
INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) SELECT 
    TIMESTAMP 'epoch' + (se.ts/1000 * INTERVAL '1 second'),
    se.user_id,
    se.level,
    so.song_id,
    so.artist_id,
    se.session_id,
    se.location,
    se.user_agent
    FROM stage_event se
    LEFT JOIN stage_song so ON
    se.song = so.title AND
    se.artist = so.artist_name AND
    ABS(se.length - so.duration) <2
    WHERE
    se.page = 'NextSong'   
""")

user_table_insert = ("""
INSERT INTO users SELECT DISTINCT (user_id)
    user_id,
    first_name,
    last_name,
    gender,
    level,
    FROM stage_event
""")

song_table_insert = ("""
INSERT INTO song SELECT DISTINCT (song_id)
    song_id,
    title,
    artist_id,
    year,
    duration
    FROM stage_song
""")

artist_table_insert = ("""
INSERT INTO artist SELECT DISTINCT (artist_id)
    artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
    FROM stage_song
""")

time_table_insert = ("""
INSERT INTO time
    WITH temp_time AS (SELECT TIMESTAMP 'epoch' + (ts/1000 * INTERVAL '1 second') as ts FROM stage_event)
    SELECT DISTINCT
    ts,
    extract(hour from ts),
    extract(day from ts),
    extract(week from ts),
    extract(year from ts),
    extract(weekday from ts)
    FROM temp_time
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
