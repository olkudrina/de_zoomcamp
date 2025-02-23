CREATE or replace external table `data.green_tripdata`
OPTIONS (
  format='csv',
  uris = ['gs://analytics-engineering-zoomcamp/green/*']
);

CREATE or replace external table `data.yellow_tripdata`
OPTIONS (
  format='csv',
  uris = ['gs://analytics-engineering-zoomcamp/yellow/*']
);

CREATE or replace external table `data.fhv`
OPTIONS (
  format='csv',
  uris = ['gs://analytics-engineering-zoomcamp/fhv/*']
);