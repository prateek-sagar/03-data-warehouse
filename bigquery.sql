CREATE OR REPLACE EXTERNAL TABLE
`terraform-demo-454545.nyc_taxi.yellow_trip_data`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://nyc_taxi_bucket-demo/yellow_tripdata_2024-*.parquet']
);


CREATE OR REPLACE TABLE
`terraform-demo-454545.nyc_taxi.yellow_trip_data_table`
AS
SELECT *
FROM `terraform-demo-454545.nyc_taxi.yellow_trip_data`;

SELECT count(*) FROM `terraform-demo-454545.nyc_taxi.yellow_trip_data`;

SELECT count(*) FROM `terraform-demo-454545.nyc_taxi.yellow_trip_data_table`;

SELECT COUNT(*) AS zero_fare_records
FROM `terraform-demo-454545.nyc_taxi.yellow_trip_data`
WHERE fare_amount = 0;

SELECT COUNT(DISTINCT(VendorID)) FROM `terraform-demo-454545.nyc_taxi.yellow_trip_data`;

CREATE OR REPLACE TABLE `terraform-demo-454545.nyc_taxi.yellow_trip_data_partitioned`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS (
  SELECT * FROM `terraform-demo-454545.nyc_taxi.yellow_trip_data_table`
);

SELECT count(*) FROM  `terraform-demo-454545.nyc_taxi.yellow_trip_data_table`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';


SELECT count(*) FROM `terraform-demo-454545.nyc_taxi.yellow_trip_data_partitioned`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';
