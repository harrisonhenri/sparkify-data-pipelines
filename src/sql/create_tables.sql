DROP TABLE IF EXISTS staging_events
DROP TABLE IF EXISTS staging_songs
DROP TABLE IF EXISTS song_plays
DROP TABLE IF EXISTS users
DROP TABLE IF EXISTS songs
DROP TABLE IF EXISTS artists
DROP TABLE IF EXISTS times

CREATE TABLE IF NOT EXISTS staging_events (
    artist_name VARCHAR,
    user_auth VARCHAR,
    user_first_name VARCHAR,
    user_gender CHAR(1),
    item_in_session INTEGER NOT NULL,
    user_last_name VARCHAR,
    length FLOAT,
    user_level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration FLOAT,
    session_id INTEGER,
    song_name VARCHAR,
    status INTEGER,
    ts BIGINT NOT NULL,
    user_agent VARCHAR,
    user_id INTEGER);

CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs INTEGER NOT NULL,
    artist_id VARCHAR NOT NULL,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location TEXT,
    artist_name VARCHAR NOT NULL,
    song_id INTEGER IDENTITY(0,1) PRIMARY KEY,
    title VARCHAR NOT NULL,
    duration FLOAT NOT NULL,
    year INTEGER NOT NULL);

CREATE TABLE IF NOT EXISTS song_plays (
    song_play_id INTEGER IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    user_id INTEGER,
    level VARCHAR,
    song_id INTEGER,
    artist_id VARCHAR,
    session_id INTEGER,
    location VARCHAR,
    user_agent VARCHAR);

CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    first_name VARCHAR,
    last_name VARCHAR,
    gender CHAR(1),
    level VARCHAR);

CREATE TABLE IF NOT EXISTS songs (
    song_id INTEGER PRIMARY KEY,
    title VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL,
    year INTEGER NOT NULL,
    duration FLOAT NOT NULL);

CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR PRIMARY KEY,
    name VARCHAR,
    location VARCHAR,
    latitude FLOAT,
    longitude FLOAT);

CREATE TABLE IF NOT EXISTS times (
    start_time TIMESTAMP PRIMARY KEY,
    hour INTEGER,
    day INTEGER,
    week INTEGER,
    month INTEGER,
    year INTEGER,
    weekday VARCHAR);