-- =====================================================
-- Cyclistic Bike Share Data Analysis
-- Author: Kyle Tokishi
-- Tool: Google BigQuery
-- Project: Cyclistic Case Study
-- =====================================================


-- =====================================================
-- 1. VERIFY DATASET SIZE
-- Confirm that all monthly datasets were combined correctly
-- =====================================================

SELECT
  COUNT(*) AS total_rides
FROM cyclistic_2025_all;



-- =====================================================
-- 2. CHECK FOR DUPLICATE RIDE IDS
-- Each ride_id should represent a unique trip
-- =====================================================

SELECT
  ride_id,
  COUNT(*) AS duplicate_count
FROM cyclistic_2025_all
GROUP BY ride_id
HAVING COUNT(*) > 1;



-- =====================================================
-- 3. VALIDATE RIDER TYPES
-- Confirm the dataset contains the expected categories
-- =====================================================

SELECT
  member_casual,
  COUNT(*) AS rides
FROM cyclistic_2025_all
GROUP BY member_casual;



-- =====================================================
-- 4. CHECK FOR MISSING STATION VALUES
-- Identify how many rides have missing station names
-- =====================================================

SELECT
  COUNT(*) AS total_rides,
  COUNTIF(start_station_name IS NULL) AS null_start_station,
  COUNTIF(end_station_name IS NULL) AS null_end_station
FROM cyclistic_2025_all;



-- =====================================================
-- 5. CREATE CLEANED DATASET
-- Remove invalid rows and create new analysis variables
-- =====================================================

CREATE OR REPLACE TABLE cleaned_trips AS
SELECT
  *,
  
  -- Calculate ride duration in minutes
  TIMESTAMP_DIFF(ended_at, started_at, MINUTE) AS ride_length_minutes,

  -- Extract day of week (numeric)
  EXTRACT(DAYOFWEEK FROM started_at) AS day_of_week,

  -- Extract month
  EXTRACT(MONTH FROM started_at) AS month,

  -- Extract weekday name
  FORMAT_TIMESTAMP('%A', started_at) AS weekday_name

FROM cyclistic_2025_all

WHERE
  ended_at > started_at
  AND started_at IS NOT NULL
  AND ended_at IS NOT NULL;



-- =====================================================
-- 6. VALIDATE CLEANED DATASET
-- Check ride duration statistics
-- =====================================================

SELECT
  MIN(ride_length_minutes) AS min_ride_length,
  MAX(ride_length_minutes) AS max_ride_length,
  AVG(ride_length_minutes) AS avg_ride_length
FROM cleaned_trips;



-- =====================================================
-- 7. TOTAL RIDES AND AVERAGE RIDE LENGTH BY RIDER TYPE
-- Compare casual riders vs members
-- =====================================================

SELECT
  member_casual,
  COUNT(*) AS rides,
  AVG(ride_length_minutes) AS avg_ride_length
FROM cleaned_trips
GROUP BY member_casual;



-- =====================================================
-- 8. RIDE FREQUENCY BY DAY OF WEEK
-- Identify weekday vs weekend behavior
-- =====================================================

SELECT
  weekday_name,
  member_casual,
  COUNT(*) AS rides
FROM cleaned_trips
GROUP BY weekday_name, member_casual
ORDER BY rides DESC;



-- =====================================================
-- 9. AVERAGE RIDE LENGTH BY DAY OF WEEK
-- Compare ride duration patterns
-- =====================================================

SELECT
  weekday_name,
  member_casual,
  AVG(ride_length_minutes) AS avg_ride_length
FROM cleaned_trips
GROUP BY weekday_name, member_casual
ORDER BY avg_ride_length DESC;



-- =====================================================
-- 10. RIDE FREQUENCY BY MONTH
-- Identify seasonal trends
-- =====================================================

SELECT
  month,
  member_casual,
  COUNT(*) AS rides
FROM cleaned_trips
GROUP BY month, member_casual
ORDER BY rides DESC;



-- =====================================================
-- 11. BIKE TYPE USAGE BY RIDER TYPE
-- Compare classic vs electric bike usage
-- =====================================================

SELECT
  rideable_type,
  member_casual,
  COUNT(*) AS rides,

  ROUND(
    COUNT(*) * 100.0 /
    SUM(COUNT(*)) OVER (PARTITION BY rideable_type),
    2
  ) AS percent_share

FROM cleaned_trips

GROUP BY rideable_type, member_casual
ORDER BY rideable_type;
