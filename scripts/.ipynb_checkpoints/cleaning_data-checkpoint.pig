REGISTER '/opt/pig/lib/piggybank.jar';

-- Using CSVExcelStorage to correctly handle the commas within quotes
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE');

-- Load the csv file and display the dataset
spotify_tracks = LOAD 'data/spotify_tracks.csv' 
  USING CSVLoader() AS (
    index:int,
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
-- DUMP spotify_tracks 20

-- Count rows before cleaning
-- count_original = FOREACH (GROUP spotify_tracks ALL) GENERATE COUNT(spotify_tracks);
-- DUMP count_original;


-- Remove the Header
tracks_no_header = FILTER spotify_tracks BY track_id != 'track_id';
 

-- Remove Index column as it's not meaningful
tracks_no_index = FOREACH tracks_no_header GENERATE
  track_id, artists, album_name, track_name, popularity, duration_ms,
  explicit, danceability, energy, key, loudness, mode,
  speechiness, acousticness, instrumentalness, liveness, valence,
  tempo, time_signature, track_genre;


-- Remove rows with Null/missing values and check the new row count
tracks_no_nulls = FILTER tracks_no_index BY (
  (track_id IS NOT NULL AND track_id != '') AND (artists IS NOT NULL AND artists != '') AND (album_name IS NOT NULL AND album_name != '') AND (track_name IS NOT NULL AND track_name != '') AND
  (popularity IS NOT NULL) AND (duration_ms IS NOT NULL) AND (explicit IS NOT NULL) AND (danceability IS NOT NULL) AND
  (energy IS NOT NULL) AND (key IS NOT NULL) AND (loudness IS NOT NULL) AND (mode IS NOT NULL) AND (speechiness IS NOT NULL) AND
  (acousticness IS NOT NULL) AND (instrumentalness IS NOT NULL) AND (liveness IS NOT NULL) AND (valence IS NOT NULL) AND
  (tempo IS NOT NULL) AND (time_signature IS NOT NULL) AND (track_genre IS NOT NULL AND track_genre != '')
);
-- count_without_nulls = FOREACH (GROUP tracks_no_nulls ALL) GENERATE COUNT(tracks_no_nulls);
-- DUMP count_without_nulls;

-- Remove any whitespaces
tracks_trimmed = FOREACH tracks_no_nulls GENERATE
  TRIM(track_id),
  TRIM(artists),
  TRIM(album_name),
  TRIM(track_name),
  popularity,
  duration_ms,
  explicit,
  danceability,
  energy,
  key,
  loudness,
  mode,
  speechiness,
  acousticness,
  instrumentalness,
  liveness,
  valence,
  tempo,
  time_signature,
  TRIM(track_genre);


--  Remove duplicate rows and check the new row count
unique_tracks = DISTINCT tracks_trimmed;
-- count_unique_tracks = FOREACH (GROUP unique_tracks  ALL) GENERATE COUNT(unique_tracks);
-- DUMP count_unique_tracks;


STORE unique_tracks INTO 'outputs/cleaned_data' USING PigStorage('\t');
