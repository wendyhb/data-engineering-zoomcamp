## Q0

CREATE MATERIALIZED VIEW latest_dropoff_time AS
    WITH t AS (
        SELECT MAX(tpep_dropoff_datetime) AS latest_dropoff_time
        FROM trip_data
    )
    SELECT taxi_zone.Zone as taxi_zone, latest_dropoff_time
    FROM t, trip_data
    JOIN taxi_zone
        ON trip_data.DOLocationID = taxi_zone.location_id
    WHERE trip_data.tpep_dropoff_datetime = t.latest_dropoff_time;

## Q1

CREATE MATERIALIZED VIEW trip_time_stats AS
WITH trip_durations AS (
    SELECT 
        pulocationid,
        dolocationid,
        EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) AS trip_duration
    FROM 
        trip_data
)
SELECT 
    pickup_zone.Zone AS pickup_zone,
    dropoff_zone.Zone AS dropoff_zone,
    AVG(trip_duration) AS avg_trip_time,
    MIN(trip_duration) AS min_trip_time,
    MAX(trip_duration) AS max_trip_time,
FROM 
    trip_durations
JOIN 
    taxi_zone AS pickup_zone ON trip_durations.pulocationid = pickup_zone.location_id
JOIN 
    taxi_zone AS dropoff_zone ON trip_durations.dolocationid = dropoff_zone.location_id
GROUP BY 
    pickup_zone.Zone, dropoff_zone.Zone;

## Answer: Yorkville East, Steinway
CREATE MATERIALIZED VIEW highest_avg_trip_time AS
SELECT *
FROM trip_time_stats
ORDER BY avg_trip_time DESC
LIMIT 1;

## Bonus

CREATE MATERIALIZED VIEW trip_anomalies AS
SELECT
    pickup_zone,
    dropoff_zone,
    ROUND(avg_trip_time, 0) AS avg_trip_time,
    ROUND(min_trip_time, 0) AS min_trip_time,
    ROUND(max_trip_time, 0) AS max_trip_time
FROM
    trip_time_stats
WHERE
    avg_trip_time*9 < max_trip_time;


## Q2

CREATE MATERIALIZED VIEW trip_time_stats_2 AS
WITH trip_durations AS (
    SELECT 
        pulocationid,
        dolocationid,
        EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) AS trip_duration
    FROM 
        trip_data
)
SELECT 
    pickup_zone.Zone AS pickup_zone,
    dropoff_zone.Zone AS dropoff_zone,
    AVG(trip_duration) AS avg_trip_time,
    MIN(trip_duration) AS min_trip_time,
    MAX(trip_duration) AS max_trip_time,
    COUNT(*) AS cnt
FROM 
    trip_durations
JOIN 
    taxi_zone AS pickup_zone ON trip_durations.pulocationid = pickup_zone.location_id
JOIN 
    taxi_zone AS dropoff_zone ON trip_durations.dolocationid = dropoff_zone.location_id
GROUP BY 
    pickup_zone.Zone, dropoff_zone.Zone;

## Answer: 1
SELECT *
FROM trip_time_stats_2
ORDER BY avg_trip_time DESC
LIMIT 1;

## Q3
## Answer: LaGuardia Airport, Lincoln Square East, JFK Airport

CREATE MATERIALIZED VIEW most_pu_17hr_before_latest_pu AS
WITH t AS (
    SELECT MAX(tpep_pickup_datetime) AS latest_pickup_time
    FROM trip_data
)
SELECT 
    COUNT(*) AS cnt,
    taxi_zone.Zone as pickup_zone
FROM t, trip_data
JOIN taxi_zone ON trip_data.PULocationID = taxi_zone.location_id
WHERE tpep_pickup_datetime > t.latest_pickup_time - interval '17 hour'
GROUP BY pickup_zone
ORDER BY cnt DESC
LIMIT 5;