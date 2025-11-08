REGISTER '/opt/pig/lib/piggybank.jar';

cleaned_data = LOAD 'outputs/cleaned_data'
  USING PigStorage('\t') AS (
    track_id:chararray,
    artists:chararray,
    album_name:chararray,
    track_name:chararray,
    popularity:int,
    duration_ms:long,
    explicit:boolean,
    danceability:double,
    energy:double,
    key:int,
    loudness:double,
    mode:int,
    speechiness:double,
    acousticness:double,
    instrumentalness:double,
    liveness:double,
    valence:double,
    tempo:double,
    time_signature:int,
    track_genre:chararray
  );

-- Simple Query 1: What is the most popular Country song by Zach Bryan that has a danceability score > 0.5?

-- Filter the tracks to get Zach Bryan's Country songs with danceability > 0.5
zach_country_tracks = FILTER cleaned_data BY (artists MATCHES '.*Zach Bryan.*' AND track_genre == 'country' AND danceability > 0.5);

-- Order the results by popularity decreasing
zach_sorted_tracks = ORDER zach_country_tracks BY popularity DESC;

-- Limit the output to top 5
zach_top5 = LIMIT zach_sorted_tracks 5;
-- DUMP top5_zach;

-- Select only the required fields
zach_bryan_result = FOREACH zach_top5 GENERATE
    track_name,
    artists,
    album_name,
    popularity,
    danceability;
-- DUMP zach_bryan_result;

STORE zach_bryan_result INTO 'outputs/pig_simple_query1_results' USING PigStorage('\t');



-- Simple Query 2: What are the track genres with high average valence?

-- Group the tracks by genre
grouped_by_genre = GROUP cleaned_data BY track_genre;

-- Find average valence
avg_valence = FOREACH grouped_by_genre GENERATE group AS track_genre, AVG(cleaned_data.valence) AS avg_valence;

-- Filter to take only tracks with high average valence
high_valence = FILTER avg_valence BY avg_valence > 0.6;

-- Order the results by average valence descending
ordered_valence = ORDER high_valence BY avg_valence DESC;

-- Limit the output to top 10
top10 = LIMIT ordered_valence 10;
-- DUMP top10;

STORE top10 INTO 'outputs/pig_simple_query2_results' USING PigStorage('\t');
