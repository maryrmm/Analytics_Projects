/*MAIN FILE*/


-- MERGED ALL DATA

  -- For combining monthly into a full year
  CREATE OR REPLACE TABLE `apt-passage-408907.BikeShare_TripData.YearlyData`
  AS
  SELECT * FROM `apt-passage-408907.BikeShare_TripData.2210_Oct`
  UNION ALL
  SELECT * FROM `apt-passage-408907.BikeShare_TripData.2211_Nov`
  UNION ALL
  SELECT * FROM `apt-passage-408907.BikeShare_TripData.2212_Dec`
  UNION ALL
  SELECT * FROM `apt-passage-408907.BikeShare_TripData.2301_Jan`
  UNION ALL
  SELECT * FROM `apt-passage-408907.BikeShare_TripData.2302_Feb`
  UNION ALL
  SELECT * FROM `apt-passage-408907.BikeShare_TripData.2303_Mar`
  UNION ALL
  SELECT * FROM `apt-passage-408907.BikeShare_TripData.2304_Apr`
  UNION ALL
  SELECT * FROM `apt-passage-408907.BikeShare_TripData.2305_May`
  UNION ALL
  SELECT * FROM `apt-passage-408907.BikeShare_TripData.2306_Jun`
  UNION ALL
  SELECT * FROM `apt-passage-408907.BikeShare_TripData.2307_Jul`
  UNION ALL
  SELECT * FROM `apt-passage-408907.BikeShare_TripData.2308_Aug`
  UNION ALL
  SELECT * FROM `apt-passage-408907.BikeShare_TripData.2309_Sep`;


-- NEW VARIABLES

-- Add ‘rental_length’ or rental duration
  -- For testing
  SELECT started_at, ended_at, ROUND(TIMESTAMP_DIFF(ended_at, started_at, MINUTE)) AS rental_length
  FROM `apt-passage-408907.BikeShare_TripData.YearlyData`;
  -- Add the new variable
  ALTER TABLE `apt-passage-408907.BikeShare_TripData.YearlyData`
  ADD COLUMN rental_length FLOAT64;
  -- Update the new columns with values
  UPDATE `apt-passage-408907.BikeShare_TripData.YearlyData`
  SET rental_length = ROUND(TIMESTAMP_DIFF(ended_at, started_at, MINUTE))
  WHERE ride_id IS NOT NULL;


-- Add ‘distance_in_kilometers’
  -- Creating the function
  CREATE OR REPLACE FUNCTION `apt-passage-408907.BikeShare_TripData.distance_in_km`
  (
    start_lat FLOAT64,
    end_lat FLOAT64,
    start_lng FLOAT64,
    end_lng FLOAT64
  )
  RETURNS FLOAT64
  LANGUAGE js AS """
    // Constants
    const R = 6367.45;  // Kilometers

    // Convert latitude and longitude from degrees to radians
    const dLat = (end_lat - start_lat) * Math.PI / 180;
    const dLon = (end_lng - start_lng) * Math.PI / 180;

    // Haversine formula
    const a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
              Math.cos(start_lat * Math.PI / 180) * Math.cos(end_lat * Math.PI / 180) *
              Math.sin(dLon / 2) * Math.sin(dLon / 2);

    const c = 2 * Math.asin(Math.min(Math.sqrt(a), 1));

    // Calculate distance
    const distance = R * c;

    return distance;
  """;


  -- Add the new variable
  ALTER TABLE `apt-passage-408907.BikeShare_TripData.YearlyData`
  ADD COLUMN distance_in_km FLOAT64;
  -- Update the new columns with values
  UPDATE `apt-passage-408907.BikeShare_TripData.YearlyData`
  SET distance_in_km = `apt-passage-408907.BikeShare_TripData.distance_in_km`(start_lat, end_lat, start_lng, end_lng)
  WHERE ride_id IS NOT NULL;

-- Add a year-quarter column
  -- Add the new variable
  ALTER TABLE `apt-passage-408907.BikeShare_TripData.YearlyData`
  ADD COLUMN quarter_new STRING;
  -- Update the new columns with values
  UPDATE `apt-passage-408907.BikeShare_TripData.YearlyData`
  SET quarter_new = FORMAT_TIMESTAMP('%Y Q%Q', started_at)
  WHERE ride_id IS NOT NULL;

-- Add a year-month column
  -- Add the new variable
  ALTER TABLE `apt-passage-408907.BikeShare_TripData.YearlyData`
  ADD COLUMN start_year_month STRING;
  -- Update the new columns with values extracted from 'started_at'
  UPDATE `apt-passage-408907.BikeShare_TripData.YearlyData`
  SET start_year_month = FORMAT_TIMESTAMP('%Y %m', started_at)
  WHERE ride_id IS NOT NULL;

-- Add a weekpart column
  -- Add the new variable
  ALTER TABLE `apt-passage-408907.BikeShare_TripData.YearlyData`
  ADD COLUMN weekpart STRING;
  -- Update the new columns with values
  UPDATE `apt-passage-408907.BikeShare_TripData.YearlyData`
  SET
    weekpart =
      CASE
        WHEN EXTRACT(DAYOFWEEK FROM started_at) BETWEEN 2 AND 6 THEN 'weekday'
        ELSE 'weekend'
      END
  WHERE ride_id IS NOT NULL;

-- Add a new column for the day the trip was taken
  -- Add the new variable
  ALTER TABLE `apt-passage-408907.BikeShare_TripData.YearlyData`
  ADD COLUMN name_of_day STRING;
  -- Update the new columns with values
  UPDATE `apt-passage-408907.BikeShare_TripData.YearlyData`
  SET name_of_day = FORMAT_DATE('%A', DATE(started_at))
  WHERE ride_id IS NOT NULL;

-- Add a new column for the time/hour the trip was taken
  -- Add the new variable
  ALTER TABLE `apt-passage-408907.BikeShare_TripData.YearlyData`
  ADD COLUMN timeofday_start STRING,
  ADD COLUMN timeofday_end STRING;
  -- Update the new columns with values
  UPDATE `apt-passage-408907.BikeShare_TripData.YearlyData`
  SET
  timeofday_start = FORMAT_TIMESTAMP('%H:00', TIMESTAMP(started_at)),
  timeofday_end = FORMAT_TIMESTAMP('%H:00', TIMESTAMP(ended_at))
  WHERE ride_id IS NOT NULL;


-- CLEANING

-- Checks for duplicates
  SELECT ride_id, COUNT(*)
  FROM `apt-passage-408907.BikeShare_TripData.YearlyData`
  GROUP BY ride_id
  HAVING COUNT(*) > 1;

  SELECT ride_id,
  FROM  `apt-passage-408907.BikeShare_TripData.YearlyData` AS yc
  WHERE ride_id = "6.32E+15";

-- Remove duplicates
  -- Made a temp file to delete rows where row_num > 1 (keeping only the first occurrence of each ride_id)
  CREATE OR REPLACE TABLE `apt-passage-408907.BikeShare_TripData.temp`
  AS
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY ride_id ORDER BY ride_id) AS row_num
  FROM `apt-passage-408907.BikeShare_TripData.YearlyData`;

-- Delete rows where row_num > 1 (keeping only the first occurrence of each ride_id)
  DELETE FROM `apt-passage-408907.BikeShare_TripData.temp`
  WHERE row_num > 1;

-- Check membership and bike type
  -- Check for distinct values in a column
  SELECT DISTINCT member_casual
  FROM `apt-passage-408907.BikeShare_TripData.temp`;

  SELECT DISTINCT rideable_type
  FROM `apt-passage-408907.BikeShare_TripData.temp`;

-- Check for leading and trailing spaces in a column
  SELECT member_casual
  FROM `apt-passage-408907.BikeShare_TripData.temp`
  WHERE LENGTH(member_casual) != LENGTH(TRIM(member_casual));

-- Check missing data on latitude and longitude
  SELECT
    COUNT(CASE WHEN start_lat IS NULL THEN 1 END) AS null_start_lat_count,
    COUNT(CASE WHEN start_lng IS NULL THEN 1 END) AS null_start_lng_count,
    COUNT(CASE WHEN end_lat IS NULL THEN 1 END) AS null_end_lat_count,
    COUNT(CASE WHEN end_lng IS NULL THEN 1 END) AS null_end_lng_count
  FROM
  `apt-passage-408907.BikeShare_TripData.temp`;

-- Delete missing location data
  DELETE FROM `apt-passage-408907.BikeShare_TripData.temp`
  WHERE end_lat IS NULL OR end_lng IS NULL;

-- Check rental_length
  SELECT
    COUNT(rental_length) AS rental_count,
    AVG(rental_length) AS avg_rental,
    MIN(rental_length) AS min_rental,
    MAX(rental_length) AS max_rental
  FROM `apt-passage-408907.BikeShare_TripData.temp`;

-- Remove rental_length values <= 0
  DELETE FROM `apt-passage-408907.BikeShare_TripData.temp`
  WHERE rental_length <= 0;


/*STATIONS FILE*/


-- MERGED ALL DATA
  -- For combining stations data into a full year
  CREATE TABLE `apt-passage-408907.BikeShare_TripData.YearlyStationsData` AS
  SELECT * FROM `apt-passage-408907.BikeShare_TripData.Station_2210_2211_3`
  UNION ALL
  SELECT * FROM `apt-passage-408907.BikeShare_TripData.Station_2212_2303_2`
  UNION ALL
  SELECT * FROM `apt-passage-408907.BikeShare_TripData.Station_2304_2305`
  UNION ALL
  SELECT * FROM `apt-passage-408907.BikeShare_TripData.Station_2306`
  UNION ALL
  SELECT * FROM `apt-passage-408907.BikeShare_TripData.Station_2307`
  UNION ALL
  SELECT * FROM `apt-passage-408907.BikeShare_TripData.Station_2308`
  UNION ALL
  SELECT * FROM `apt-passage-408907.BikeShare_TripData.Station_2309`;


/*CLEANING*/


-- Remove duplicates from ride ids, made a temp stations data
  CREATE OR REPLACE TABLE `apt-passage-408907.BikeShare_TripData.tempstations`
  AS
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY ride_id ORDER BY ride_id) AS row_num
  FROM `apt-passage-408907.BikeShare_TripData.YearlyStationsData`;


-- Delete rows where row_num > 1 (keeping only the first occurrence of each ride_id)
  DELETE FROM `apt-passage-408907.BikeShare_TripData.tempstations`
  WHERE row_num > 1;

-- Check missing data on station names
  SELECT
    COUNT(CASE WHEN start_station_name IS NULL THEN 1 END) AS start_station_name,
    COUNT(CASE WHEN start_station_id IS NULL THEN 1 END) AS start_station_name,
    COUNT(CASE WHEN end_station_name IS NULL THEN 1 END) AS start_station_name,
    COUNT(CASE WHEN end_station_id IS NULL THEN 1 END) AS start_station_name,
  FROM
  `apt-passage-408907.BikeShare_TripData.tempstations`;

-- Drop unnecessary columns
  ALTER TABLE `apt-passage-408907.BikeShare_TripData.tempstations`
  DROP COLUMN start_station_id;

  ALTER TABLE `apt-passage-408907.BikeShare_TripData.tempstations`
  DROP COLUMN end_station_id;

-- Add station names in Main file
  ALTER TABLE `apt-passage-408907.BikeShare_TripData.temp`
  ADD COLUMN start_station_name STRING,
  ADD COLUMN end_station_name STRING;

-- Merge stations vars in the temp file by using LEFT JOIN
  UPDATE `apt-passage-408907.BikeShare_TripData.temp` AS y
  SET
    start_station_name = s.start_station_name,
    end_station_name = s.end_station_name
  FROM
    `apt-passage-408907.BikeShare_TripData.tempstations` AS s
  WHERE
    y.ride_id = s.ride_id;


/*FINAL FILES*/


-- Create a new table without duplicates
  CREATE OR REPLACE TABLE `apt-passage-408907.BikeShare_TripData.YearlyCleanedFinal` AS
  SELECT *
  FROM `apt-passage-408907.BikeShare_TripData.temp`;

  CREATE OR REPLACE TABLE `apt-passage-408907.BikeShare_TripData.YearlyStationsFinal` AS
  SELECT *
  FROM `apt-passage-408907.BikeShare_TripData.tempstations`;


/*DATA RUNS*/


-- Trips by Membership Type

  SELECT
    COUNT (*) AS Total,
    COUNTIF(member_casual = 'member') AS member_trips,
    COUNTIF(member_casual = 'casual') AS casual_trips
  FROM `apt-passage-408907.BikeShare_TripData.YearlyCleanedFinal`;

-- Trips by Quarter

  SELECT
    quarter_new,
    COUNT (*) AS Total,
    COUNTIF(member_casual = 'member') AS member_trips,
    COUNTIF(member_casual = 'casual') AS casual_trips
  FROM `apt-passage-408907.BikeShare_TripData.YearlyCleanedFinal`
  GROUP BY
    quarter_new
  ORDER BY
    quarter_new;

-- Trips by Month

  SELECT
    start_year_month,
    COUNT (*) AS Total,
    COUNTIF(member_casual = 'member') AS member_trips,
    COUNTIF(member_casual = 'casual') AS casual_trips
  FROM `apt-passage-408907.BikeShare_TripData.YearlyCleanedFinal`
  GROUP BY
    start_year_month
  ORDER BY
    start_year_month;

-- Bike Type by Membership

  SELECT
    membership2,
    rideable_type,
    COUNT(*) AS count_rideable,
    100 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY membership2) AS percent_of_total
  FROM `apt-passage-408907.BikeShare_TripData.YearlyCleanedFinal`
  GROUP BY membership2, rideable_type
  ORDER BY membership2 DESC;

-- Weekpart by Membership

  SELECT
    membership2,
    weekpart,
    COUNT(*) AS count_rideable,
    100 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY membership2) AS percent_of_total
  FROM `apt-passage-408907.BikeShare_TripData.YearlyCleanedFinal`
  GROUP BY membership2, weekpart
  ORDER BY membership2 DESC;

-- Weekpart and Time of Day by Membership

  SELECT
    membership2,
    weekpart,
    timeofday_start,
    COUNT(*) AS count_rideable,
  FROM `apt-passage-408907.BikeShare_TripData.YearlyCleanedFinal`
  GROUP BY membership2, weekpart,timeofday_start
  ORDER BY membership2 DESC, weekpart,timeofday_start;

-- Average Rental and Distance by Membership

  SELECT
    membership2,
    AVG(rental_length),
    AVG(distance_in_km),
    COUNT(*) AS count_rideable,
  FROM `apt-passage-408907.BikeShare_TripData.YearlyCleanedFinal`
  GROUP BY membership2
  ORDER BY membership2;

-- Day by Membership

  SELECT
    name_of_day,
    membership2,
    COUNT(*) AS count_rideable,
  FROM `apt-passage-408907.BikeShare_TripData.YearlyCleanedFinal`
  GROUP BY membership2, name_of_day
  ORDER BY
    CASE  name_of_day
      WHEN 'Sunday' THEN 1
      WHEN 'Monday' THEN 2
      WHEN 'Tuesday' THEN 3
      WHEN 'Wednesday' THEN 4
      WHEN 'Thursday' THEN 5
      WHEN 'Friday' THEN 6
      ELSE 7
    END;

-- Avg Rental Length by Weekpart by Membership

  SELECT
    membership2,
    weekpart,
    AVG(rental_length) AS rental_length
  FROM `apt-passage-408907.BikeShare_TripData.YearlyCleanedFinal`
  GROUP BY membership2, weekpart
  ORDER BY membership2 DESC, weekpart;

-- Total Count, Avg Rental Length by Bike Type by Membership

  SELECT
    membership2,
    rideable_type,
    COUNT(*) AS count_trips,  
    AVG(rental_length) AS rental_length,
  FROM `apt-passage-408907.BikeShare_TripData.YearlyCleanedFinal`
  GROUP BY membership2, rideable_type
  ORDER BY membership2 DESC, rideable_type DESC;


-- Stations

  SELECT
    membership2,
    start_station_name,
    COUNT(*) AS count_rideable
  FROM `apt-passage-408907.BikeShare_TripData.YearlyCleanedFinal`
  WHERE membership2 = "Member" AND start_station_name IS NOT NULL
  GROUP BY membership2, start_station_name
  ORDER BY count_rideable DESC
  LIMIT 5;

  SELECT
    membership2,
    end_station_name,
    COUNT(*) AS count_rideable
  FROM `apt-passage-408907.BikeShare_TripData.YearlyCleanedFinal`
  WHERE membership2 = "Member" AND end_station_name IS NOT NULL
  GROUP BY membership2, end_station_name
  ORDER BY count_rideable DESC
  LIMIT 5;

  SELECT
    membership2,
    start_station_name,
    COUNT(*) AS count_rideable
  FROM `apt-passage-408907.BikeShare_TripData.YearlyCleanedFinal`
  WHERE membership2 = "Casual User" AND start_station_name IS NOT NULL
  GROUP BY membership2, start_station_name
  ORDER BY count_rideable DESC
  LIMIT 5;

  SELECT
    membership2,
    end_station_name,
    COUNT(*) AS count_rideable
  FROM `apt-passage-408907.BikeShare_TripData.YearlyCleanedFinal`
  WHERE membership2 = "Casual User" AND end_station_name IS NOT NULL
  GROUP BY membership2, end_station_name
  ORDER BY count_rideable DESC
  LIMIT 5;
