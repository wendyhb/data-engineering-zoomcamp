CREATE OR REPLACE EXTERNAL TABLE dtc-de-course-412616.ny_taxi.green_taxi_data
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://mage-zoomcamp-wenqi/green_taxi_data.parquet']
);

# Q1 840k rows
SELECT count(*) FROM dtc-de-course-412616.ny_taxi.green_taxi_data;

# MATERIALIZE
CREATE OR REPLACE TABLE dtc-de-course-412616.ny_taxi.green_taxi_non_partitioned
AS SELECT * FROM dtc-de-course-412616.ny_taxi.green_taxi_data;

# Q2 0 MB and 6.4 MB
SELECT COUNT(DISTINCT(pulocation_id)) FROM dtc-de-course-412616.ny_taxi.green_taxi_data;
SELECT COUNT(DISTINCT(pulocation_id)) FROM dtc-de-course-412616.ny_taxi.green_taxi_non_partitioned;

# Q3 1,622
SELECT COUNT(*)
FROM dtc-de-course-412616.ny_taxi.green_taxi_data
WHERE fare_amount = 0;

# Q4 What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime?
# ANSWER Partition by lpep_pickup_datetime Cluster on PUlocationID

# Partition
CREATE OR REPLACE TABLE dtc-de-course-412616.ny_taxi.green_taxi_partitioned
PARTITION BY lpep_pickup_date AS
SELECT * FROM dtc-de-course-412616.ny_taxi.green_taxi_data;

# Compare partitioned vs non
# Q5 12.82 MB for non-partitioned table and 1.12 MB for the partitioned table
SELECT DISTINCT pulocation_id FROM dtc-de-course-412616.ny_taxi.green_taxi_non_partitioned
WHERE DATE(lpep_pickup_date) BETWEEN '2022-06-01' AND '2022-06-30';

SELECT DISTINCT pulocation_id FROM dtc-de-course-412616.ny_taxi.green_taxi_partitioned
WHERE DATE(lpep_pickup_date) BETWEEN '2022-06-01' AND '2022-06-30';

# Q6 Where is the data stored in the External Table you created?
# GCP Bucket

# Q7 False, it is often not best for small data as it can add costs

# Q8 BONUS

SELECT count(*) FROM dtc-de-course-412616.ny_taxi.green_taxi_non_partitioned
