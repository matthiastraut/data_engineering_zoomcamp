# HW3

## SQL Queries

SELECT COUNT(DISTINCT(PULocationID)) FROM `project-df2ba729-605a-4554-917.zoomcamp_hw3.yellow-taxi-2024-materialized`

SELECT COUNT(*) FROM `project-df2ba729-605a-4554-917.zoomcamp_hw3.yellow-taxi-2024` WHERE fare_amount=0

CREATE OR REPLACE TABLE `project-df2ba729-605a-4554-917.zoomcamp_hw3.yellow-taxi-2024-partition` PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `project-df2ba729-605a-4554-917.zoomcamp_hw3.yellow-taxi-2024`
WHERE tpep_dropoff_datetime >= '2024-03-01' AND tpep_dropoff_datetime <= '2024-03-15'

SELECT DISTINCT(VendorID) FROM `project-df2ba729-605a-4554-917.zoomcamp_hw3.yellow-taxi-2024-partition`

