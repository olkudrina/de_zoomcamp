/*Question 1: What is count of records for the 2024 Yellow Taxi Data?*/
select count(*) from taxi_tripdata.yellow_tripdata;

/*Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?*/
select count(distinct `PULocationID`) from taxi_tripdata.yellow_tripdata;
select count(distinct `PULocationID`) from taxi_tripdata.external_yellow_tripdata;

/*Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery.
Now write a query to retrieve the PULocationID and DOLocationID on the same table?*/
select `PULocationID` from taxi_tripdata.yellow_tripdata;
select `PULocationID`, `DOLocationID` from taxi_tripdata.yellow_tripdata;

/*How many records have a fare_amount of 0?*/
select count(*) from taxi_tripdata.yellow_tripdata
where fare_amount = 0;

/*What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime 
and order the results by VendorID (Create a new table with this strategy)*/
create or replace table taxi_tripdata.optimized_yellow_tripdata
partition by date(tpep_dropoff_datetime)
cluster by VendorID
as
select * from taxi_tripdata.yellow_tripdata;

/*Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive)
Use the materialized table you created earlier in your from clause and note the estimated bytes.
Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values?*/
select count(distinct `VendorID`) from taxi_tripdata.yellow_tripdata
where cast(tpep_dropoff_datetime as date) between '2024-03-01' and '2024-03-15';