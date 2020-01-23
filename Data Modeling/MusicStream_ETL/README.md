# ETL pipelines for MusicStream using Python and PostgreSQL

This project utilizes song and activity log data from a streaming app called MusicStream. The data is initially in the form of JSON logs, and is located in the `data` folder.  The project involves the creation of a database schema, and the development of an ETL pipeline for loading data from the log files into the database.

Information from the data files are loaded into the following tables:
 - `users` - User information
 - `tracks` - Track information
 - `artists` - Artist information
 - `trackplays` - Information for each music track played
 - `time` - Table with detailed timestamp information

This database enables extraction of detailed information on user usage metrics and behavior.

## Instructions
The ETL pipeline can be run by the following:
```
python create_tables.py
python etl.py
```
`create_tables.py` is run initially to setup the database, followed by `etl.py`.
`sql_queries.py` contains queries that are used in both `create_tables.py` and `etl.py`.


## Data Modeling

A star schema was utilized for modeling the `musicstreamdb` database, with 1 fact table (`trackplays`) and 4 dimensional tables (`users`, `songs`, `artists`, and `timetb` ).

### Track plays table

Name: `trackplays`
Info: These are the records from log data that are associated with each play of a track

| Column | Attrib | Info |
| ------ | ---- | ----------- |
| `trackplay_id` | `INT PRIMARY KEY` | ID of log entry | 
| `start_time` | `TIMESTAMP` | Timestamp of play start |
| `user_id` | `INT NOT NULL REFERENCES users(user_id)` | ID of user responsible for log entry |
| `tier` | `TEXT` | Whether free or premium tier customer |
| `track_id` | `TEXT REFERENCES tracks(track_id)` | ID of track played |
| `artist_id` | `TEXT REFERENCES artists(artist_id)` | ID of song artist |
| `session_id` | `INT` | Session_id of the user |
| `location` | `TEXT` | Location of log generation event  |
| `user_agent` | `TEXT` | User agent of app (browser/device etc.) |


### Users table

Name: `users`
Info: Data on app users from the log files

| Column | Attrib | Info |
| ------ | ---- | ----------- |
| `user_id` | `INT PRIMARY KEY` | User ID |
| `first_name` | `TEXT NOT NULL` | First name of user |
| `last_name` | `TEXT NOT NULL` | Last name of user |
| `gender` | `TEXT` | Gender of user (M/F) |
| `tier` | `TEXT` | Whether free or premium tier customer |


### Tracks table

Name: `tracks`
Info: Data on tracks from the song data files

| Column | Attrib | Info |
| ------ | ---- | ----------- |
| `track_id` | `TEXT PRIMARY KEY` | Track ID | 
| `title` | `TEXT NOT NULL` | Title of track |
| `artist_id` | `TEXT NOT NULL REFERENCES artists(artist_id)` | Artist ID |
| `year` | `INT` | Release year |
| `duration` | `NUMERIC NOT NULL` | Track duration |


### Artists table

Name: `artists`
Info: Data on artists from the song data files

| Column | Attrib | Info |
| ------ | ---- | ----------- |
| `artist_id` | `TEXT PRIMARY KEY` | Artist ID |
| `name` | `TEXT NOT NULL` | Artist name |
| `location` | `TEXT` | Artist location |
| `latitude` | `NUMERIC` | Location latitude |
| `longitude` | `NUMERIC` | Location longitude |

### Time table

Name: `timetb`
Info: Detailed timestamp data from play log files

| Column | Attrib | Info |
| ------ | ---- | ----------- |
| `start_time` | `TIMESTAMP PRIMARY KEY` | Timestamp of log play start time |
| `hour` | `INT` | Hour from timestamp  |
| `day` | `INT` | Day from timestamp |
| `week` | `INT` | Week of year from timestamp |
| `month` | `INT` | Month from timestamp |
| `year` | `INT` | Year from timestamp |
| `weekday` | `TEXT` | Weekday name from timestamp |




