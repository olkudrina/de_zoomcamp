CREATE or replace external table `taxi_tripdata.external_yellow_tripdata`
OPTIONS (
  format='parquet',
  uris = ['gs://taxi_tripdata/yellow*']
);


CREATE TABLE taxi_tripdata.yellow_tripdata AS (
  SELECT * FROM taxi_tripdata.external_yellow_tripdata
);
