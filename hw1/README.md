## Data Engineering Zoomcamp

# Question 1. Understanding Docker images

'''bash
docker run -it --rm --entrypoint=bash python:3.13
'''


'''
uv run python ingest_data_parquet.py --pg-user=postgres --pg-pass=postgres --pg-host=localhost --pg-port=5433 --pg-db=ny_taxi --target-table=green_taxi_trips
'''

# Question 3. Counting short trips
'''
SELECT COUNT(*) FROM green_taxi_trips WHERE green_taxi_trips.lpep_pickup_datetime >= '2025-11-01' AND green_taxi_trips.lpep_pickup_datetime < '2025-12-01' AND green_taxi_trips.trip_distance <= 1
'''

# Question 4. Longest trip for each day
'''
SELECT trip_distance, lpep_pickup_datetime FROM green_taxi_trips WHERE trip_distance <= 100 ORDER BY trip_distance DESC LIMIT 5
'''

# Question 5. Biggest pickup zone
'''
SELECT "PULocationID", SUM(total_amount) AS sum_amount FROM green_taxi_trips WHERE lpep_pickup_datetime >= '2025-11-18' AND lpep_pickup_datetime < '2025-
 11-19' GROUP BY "PULocationID" ORDER BY sum_amount DESC LIMIT 5
'''


# Question 6. Largest tip
'''
SELECT "DOLocationID", tip_amount FROM green_taxi_trips WHERE "
 PULocationID" = 74 ORDER BY tip_amount DESC LIMIT 5
'''

