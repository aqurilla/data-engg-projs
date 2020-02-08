import configparser

config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP queries
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_tracks_table_drop = "DROP TABLE IF EXISTS staging_tracks;"

trackplay_table_drop = "DROP TABLE IF EXISTS trackplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
track_table_drop = "DROP TABLE IF EXISTS tracks;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
timetb_table_drop = "DROP TABLE IF EXISTS timetb;"

# Staging tables create
staging_events_table_create= ("""
    CREATE TABLE staging_events (
        artist VARCHAR(500),
        auth VARCHAR(50),
        firstName VARCHAR(200),
        gender CHAR(1),
        itemInSession INTEGER,
        lastName VARCHAR(200),
        length DECIMAL(10,5),
        tier VARCHAR(50),
        location VARCHAR(300),
        method VARCHAR(50),
        page VARCHAR(300),
        registration DECIMAL(10,5),
        sessionId INTEGER,
        track VARCHAR(500),
        status INTEGER,
        ts VARCHAR(50),
        userAgent VARCHAR(500),
        userId INTEGER
    );
""")

staging_tracks_table_create = ("""
    CREATE TABLE staging_tracks (
        num_tracks INTEGER,
        artist_id VARCHAR(50),
        artist_latitude DECIMAL(10,5),
        artist_longitude DECIMAL(10,5),
        artist_location VARCHAR(500),
        artist_name VARCHAR(500),
        track_id VARCHAR(50),
        title VARCHAR(500),
        duration DECIMAL(10,5),
        year INTEGER
    );
""")

# Dimension tables
users_table_create = ("""
    CREATE TABLE IF NOT EXISTS users
    (user_id INTEGER PRIMARY KEY,
     first_name VARCHAR(200) NOT NULL,
     last_name VARCHAR(200) NOT NULL,
     gender CHAR(1),
     tier VARCHAR(10) NOT NULL
     )
""")

tracks_table_create = ("""
    CREATE TABLE IF NOT EXISTS tracks
    (track_id VARCHAR(20) PRIMARY KEY,
     title VARCHAR(200) NOT NULL SORTKEY,
     artist_id VARCHAR(20) NOT NULL DISTKEY REFERENCES artists(artist_id),
     year INTEGER NOT NULL,
     duration DECIMAL(10, 5) NOT NULL
     )
""")

artists_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists
    (artist_id VARCHAR(200) PRIMARY KEY,
     name VARCHAR(500) NOT NULL SORTKEY,
     location VARCHAR(300),
     latitude DECIMAL(10,5),
     longitude DECIMAL(10,5)
     )
""")

timetb_table_create = ("""
    CREATE TABLE IF NOT EXISTS timetb
    (start_time TIMESTAMP NOT NULL PRIMARY KEY SORTKEY,
     hour INTEGER NOT NULL,
     day INTEGER NOT NULL,
     week INTEGER NOT NULL,
     month INTEGER NOT NULL,
     year INTEGER NOT NULL,
     weekday VARCHAR(20) NOT NULL
     )
""")

# Fact table
trackplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS trackplays
    (trackplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
     start_time TIMESTAMP NOT NULL REFERENCES timetb(start_time),
     user_id INTEGER NOT NULL REFERENCES users(user_id),
     tier VARCHAR(10),
     track_id VARCHAR(50) REFERENCES tracks(track_id),
     artist_id VARCHAR(200) REFERENCES artists(artist_id),
     session_id INTEGER,
     location VARCHAR(300),
     user_agent VARCHAR(500)
     )
""")

# Copy data to staging tables
staging_events_copy = ('''
    copy staging_events from {}
    region 'us-west-2'
    iam_role {}
    compupdate off statupdate off
    format as json {}
    timeformat as 'epochmillisecs'
''').format(config.get('S3', 'log_data'), config.get('IAM_ROLE', 'arn'), config.get('S3', 'log_jsonpath'))

staging_tracks_copy = ('''
    copy staging_tracks from {}
    region 'us-west-2'
    iam_role {}
    compupdate off statupdate off
    format as json 'auto'
''').format(config.get('S3', 'song_data'), config.get('IAM_ROLE', 'arn'))


# Other table inserts
# trackplays
trackplays_table_insert = ("""
    INSERT INTO trackplays(
        start_time,
        user_id,
        tier,
        track_id,
        artist_id,
        session_id,
        location,
        user_agent  
    )
    SELECT DISTINCT
        e.ts AS start_time,
        e.userId AS user_id,
        e.tier AS tier,
        t.track_id AS track_id,
        t.artist_id AS artist_id,
        e.sessionId AS session_id,
        e.location AS location,
        e.userAgent AS user_agent
    FROM staging_events e
    JOIN staging_tracks t 
    ON e.track = t.title
    AND e.artist = t.artist_name
    WHERE e.page == 'NextSong';
""")

# users
users_table_insert = ("""
    INSERT INTO users(
        user_id,
        first_name,
        last_name,
        gender,
        tier
    )
    SELECT DISTINCT
        e.userId AS user_id,
        e.firstName AS first_name,
        e.lastName AS last_name,
        e.gender AS gender,
        e.tier AS tier 
    FROM staging_events e
    WHERE e.page == 'NextSong'
    AND e.user_id IS NOT NULL
    ON CONFLICT (user_id) DO NOTHING;
""")

# tracks
tracks_table_insert = ("""
    INSERT INTO tracks(
        track_id,
        title,
        artist_id,
        year,
        duration
    )
    SELECT DISTINCT
        t.track_id AS track_id,
        t.title AS title,
        t.artist_id AS artist_id,
        t.year AS year,
        t.duration AS duration
    FROM staging_tracks t
    AND t.track_id IS NOT NULL
    ON CONFLICT (track_id) DO NOTHING;
""")

# artists
artists_table_insert = ("""
    INSERT INTO artists(
        artist_id,
        name,
        location,
        latitude,
        longitude
    )
    SELECT DISTINCT
        t.artist_id AS artist_id,
        t.artist_name AS name,
        t.artist_location AS location,
        t.artist_latitude AS latitude,
        t.artist_longitude AS longitude
    FROM staging_tracks t
    AND t.artist_id IS NOT NULL
    ON CONFLICT (artist_id) DO NOTHING;
""")

# timetb
timetb_table_insert = ("""
    INSERT INTO timetb(
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekday
    )
    SELECT DISTINCT
        e.start_time AS start_time,
        EXTRACT(hour FROM start_time) AS hour,
        EXTRACT(day FROM start_time) AS day,
        EXTRACT(week FROM start_time) AS week,
        EXTRACT(month FROM start_time) AS month,
        EXTRACT(year FROM start_time) AS year,
        EXTRACT(dayofweek FROM start_time) AS weekday,
    FROM staging_events e
    ON CONFLICT (start_time) DO NOTHING;
""")


# Count Queries
count_staging_events = ('''
    SELECT COUNT(*) FROM staging_events;
''')

count_staging_tracks = ('''
    SELECT COUNT(*) FROM staging_tracks;
''')

count_trackplays = ('''
    SELECT COUNT(*) FROM trackplays;
''')

count_tracks = ('''
    SELECT COUNT(*) FROM tracks;
''')

count_users = ('''
    SELECT COUNT(*) FROM users;
''')

count_artists = ('''
    SELECT COUNT(*) FROM artists;
''')

count_timetb = ('''
    SELECT COUNT(*) FROM timetb;
''')


# Query Lists
drop_queries = [
    staging_events_table_drop, 
    staging_tracks_table_drop, 
    trackplay_table_drop, 
    user_table_drop, 
    track_table_drop, 
    artist_table_drop, 
    timetb_table_drop
]

create_queries = [
    staging_events_table_create,
    staging_tracks_table_create,
    tracks_table_create,
    artists_table_create,
    users_table_create,
    timetb_table_create,
    trackplay_table_create
]

copy_queries = [
    staging_events_copy,
    staging_tracks_copy
]

insert_queries = [
    users_table_insert,
    artists_table_insert,
    tracks_table_insert,
    timetb_table_insert,
    trackplays_table_insert
]

count_queries = [
    count_staging_events,
    count_staging_tracks,
    count_users,
    count_artists,
    count_tracks,
    count_tb
]