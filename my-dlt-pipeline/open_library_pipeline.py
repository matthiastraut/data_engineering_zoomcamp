"""A `dlt` pipeline to ingest data from the Open Library REST API."""

import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig


@dlt.source
def open_library_rest_api_source():
    """Define dlt resources from Open Library REST API endpoints."""

    # Using a list of ISBNs as an example
    isbns = "ISBN:0451526538,ISBN:0385472579,ISBN:9780596515898"

    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://openlibrary.org/",
        },
        "resources": [
            {
                "name": "open_library_books",
                "endpoint": {
                    "path": "api/books",
                    "params": {
                        "bibkeys": isbns,
                        "format": "json",
                        "jscmd": "data",
                    },
                    # The API returns a dictionary keyed by ISBN.
                    # We select the root ($) to get the whole dictionary.
                    "data_selector": "$",
                },
            }
        ],
    }

    yield from rest_api_resources(config)


@dlt.transformer(data_from=open_library_rest_api_source().open_library_books)
def flatten_books(books_dict):
    """
    Open Library returns a dictionary like {'ISBN:123': {...}, 'ISBN:456': {...}}.
    This transformer flattens it into individual records.
    """
    if isinstance(books_dict, dict):
        for bibkey, details in books_dict.items():
            # Add the bibkey to the record for convenience
            details["_bibkey"] = bibkey
            yield details
    else:
        # If it's already a list or something else, just yield as is
        yield books_dict


# Create the pipeline
pipeline = dlt.pipeline(
    pipeline_name='open_library_pipeline',
    destination='duckdb',
    dataset_name='open_library_data',
    progress="log",
)


if __name__ == "__main__":
    # Run the pipeline with the transformer to flatten the results
    # We combine the source and the transformer using the pipe operator
    load_info = pipeline.run(open_library_rest_api_source() | flatten_books)
    print(load_info)  # noqa: T201
