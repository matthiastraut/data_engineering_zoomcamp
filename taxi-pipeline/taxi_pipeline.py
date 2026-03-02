import dlt
from dlt.sources.rest_api import rest_api_source


@dlt.source
def nyc_taxi_source():
    """Definition for the NYC taxi data REST API source."""
    return rest_api_source({
        "client": {
            "base_url": "https://us-central1-dlthub-analytics.cloudfunctions.net/",
        },
        "resources": [
            {
                "name": "taxi_data",
                "endpoint": {
                    "path": "data_engineering_zoomcamp_api",
                    "paginator": {
                        "type": "page_number",
                        "page_param": "page",
                        "base_page": 1,
                        "total_path": None,
                    },
                    "data_selector": "$",
                },
            }
        ]
    })


if __name__ == "__main__":
    # Define the pipeline
    pipeline = dlt.pipeline(
        pipeline_name="taxi_pipeline",
        destination="duckdb",
        dataset_name="ny_taxi_data",
    )

    # Run the pipeline with the source - use 'replace' to avoid duplicates during dev
    load_info = pipeline.run(nyc_taxi_source(), write_disposition="replace")

    # Output the result
    print(load_info)
