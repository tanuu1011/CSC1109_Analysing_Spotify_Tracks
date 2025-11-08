DROP TABLE IF EXISTS spotify_tracks;

CREATE EXTERNAL TABLE spotify_tracks (
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
LOCATION '/user/hive/data/cleaned_data/cleaned_data/';


-- Complex Query 1: Hive Function (Aggregate)
-- What is the average speechiness and popularity by genre?
-- This query is to find which genres of tracks have a high speech content and are also popular on average.
INSERT OVERWRITE LOCAL DIRECTORY '/lab/outputs/hive_complex_query1_results'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
SELECT
  track_genre,
  ROUND(AVG(speechiness), 3) AS avg_speechiness,
  ROUND(AVG(popularity), 2) AS avg_popularity,
  COUNT(track_id) AS total_tracks
FROM spotify_tracks
GROUP BY track_genre
HAVING COUNT(track_id) > 20
ORDER BY avg_speechiness DESC
LIMIT 20;



-- Complex Query 2: Join Statement
-- This query is to check if high valence tracks are more danceable
CREATE TABLE IF NOT EXISTS genre_valence AS
SELECT track_genre, AVG(valence) AS avg_valence
FROM spotify_tracks
GROUP BY track_genre;

CREATE TABLE IF NOT EXISTS genre_danceability AS
SELECT track_genre, AVG(danceability) AS avg_danceability
FROM spotify_tracks
GROUP BY track_genre;

INSERT OVERWRITE LOCAL DIRECTORY '/lab/outputs/hive_complex_query2_results'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
SELECT
  v.track_genre,
  ROUND(v.avg_valence, 3) AS avg_valence,
  ROUND(d.avg_danceability, 3) AS avg_danceability
FROM genre_valence v
JOIN genre_danceability d
ON v.track_genre = d.track_genre
ORDER BY v.avg_valence DESC;



-- Complex Query 3: Sampling
-- This query takes a 10% sample of the tracks to identify which artists create energetic music under which genre
INSERT OVERWRITE LOCAL DIRECTORY '/lab/outputs/hive_complex_query3_results'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
SELECT
    artists,
    track_genre,
    ROUND(AVG(energy), 2) AS avg_energy,
    ROUND(AVG(popularity), 2) AS avg_popularity
FROM spotify_tracks
TABLESAMPLE(BUCKET 1 OUT OF 10 ON track_id)
GROUP BY artists, track_genre
ORDER BY avg_energy DESC
LIMIT 10;