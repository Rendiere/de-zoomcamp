# Setting up Postgres and PGAdmin using Docker


## Setup

Start up the docker images
```
docker-compose up -d
```


**Create a database**
Connect to `localhost:8080` and pgadmin will come up. Log in with admin@admin.com as username and password root. 

Then create a database through the PGAdmin UI with the following details:
* host: pgdatabase
* port: 5432
* username: root
* password: root

Run the ingestion script
```
python ingest_data.py \
--user root \
--password root \
--host localhost \
--port 5432 \
--db ny_taxi \
--table_name yellow_taxi_trips \
--url "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"
```

This will load data into the `yellow_taxi_trips` table under the `public` schema. 

You can then confirm the data is in the table through pgadmin