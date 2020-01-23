# SQL query repository to be used for ETL processes

# CREATE tables
# Fact table
trackplays_table_create = ("""
    CREATE TABLE IF NOT EXISTS trackplays
    (trackplay_id int PRIMARY KEY,
     start_time timestamp REFERENCES timetb(start_time),
     user_id int NOT NULL REFERENCES users(user_id),
     tier text,
     track_id text REFERENCES tracks(track_id),
     artist_id text REFERENCES artists(artist_id),
     session_id int,
     location text,
     user_agent text
     )
""")

# Dimension tables
users_table_create = ("""
    CREATE TABLE IF NOT EXISTS users
    (user_id int PRIMARY KEY,
     first_name text NOT NULL,
     last_name text NOT NULL,
     gender text,
     tier text
     )
""")

tracks_table_create = ("""
    CREATE TABLE IF NOT EXISTS tracks
    (track_id text PRIMARY KEY,
     title text NOT NULL,
     artist_id text NOT NULL REFERENCES artists(artist_id),
     year int,
     duration numeric NOT NULL
     )
""")

artists_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists
    (artist_id text PRIMARY KEY,
     name text NOT NULL,
     location text,
     latitude numeric,
     longitude numeric
     )
""")

timetb_table_create = ("""
    CREATE TABLE IF NOT EXISTS timetb
    (start_time timestamp PRIMARY KEY,
     hour int,
     day int,
     week int,
     month int,
     year int,
     weekday text
     )
""")

# Drop tables
trackplays_table_drop = "DROP TABLE IF EXISTS trackplays"
users_table_drop = "DROP TABLE IF EXISTS users"
tracks_table_drop = "DROP TABLE IF EXISTS tracks"
artists_table_drop = "DROP TABLE IF EXISTS artists"
timetb_table_drop = "DROP TABLE IF EXISTS timetb"

# Insert records
# trackplays
trackplays_table_insert = ("""
    INSERT INTO trackplays(
        trackplay_id,
        start_time,
        user_id,
        tier,
        track_id,
        artist_id,
        session_id,
        location,
        user_agent
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (trackplay_id) DO NOTHING;
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
    VALUES (%s, %s, %s, %s, %s)
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
    VALUES (%s, %s, %s, %s, %s)
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
    VALUES (%s, %s, %s, %s, %s)
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
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (start_time) DO NOTHING;
""")

# Query for track search
track_search = ("""
    SELECT tracks.track_id AS track_id, tracks.artist_id as artist_id
    FROM tracks JOIN artists ON tracks.artist_id = artists.artist_id
    WHERE tracks.title = %s AND
    artists.name = %s AND
    tracks.duration = %s
""")

# Query lists
create_table_queries = [timetb_table_create, users_table_create, artists_table_create, tracks_table_create, trackplays_table_create]

drop_table_queries = [trackplays_table_drop, users_table_drop, tracks_table_drop, artists_table_drop, timetb_table_drop]
