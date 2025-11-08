CREATE EXTERNAL TABLE IF NOT EXISTS spotify_tracks (
  track_id STRING,
  artists STRING,
  album_name STRING,
  track_name STRING,
  popularity INT,
  duration_ms BIGINT,
  explicit BOOLEAN,
  danceability DOUBLE,
  energy DOUBLE,
  `key` INT,
  loudness DOUBLE,
  mode INT,
  speechiness DOUBLE,
  acousticness DOUBLE,
  instrumentalness DOUBLE,
  liveness DOUBLE,
  valence DOUBLE,
  tempo DOUBLE,
  time_signature INT,
  track_genre STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/hive/data/cleaned_data';


-- Simple Query 1: What is the most popular Country song by Zach Bryan that has a danceability score > 0.5?
INSERT OVERWRITE LOCAL DIRECTORY '/lab/outputs/hive_simple_query1_results'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
SELECT track_name, artists, album_name, popularity, danceability
FROM spotify_tracks
WHERE artists LIKE '%Zach Bryan%'
  AND track_genre = 'country'
  AND danceability > 0.5
ORDER BY popularity DESC
LIMIT 5;



-- Simple Query 2: What are the track genres with high average valence?
INSERT OVERWRITE LOCAL DIRECTORY '/lab/outputs/hive_simple_query2_results'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
SELECT track_genre, AVG(valence) AS avg_valence
FROM spotify_tracks
GROUP BY track_genre
HAVING AVG(valence) > 0.6
ORDER BY avg_valence DESC
LIMIT 10;