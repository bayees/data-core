from typing import Generator, Any, Dict, List
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources
import dlt
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from datetime import datetime, timedelta
import os

@dlt.source
def home_assistant_source(
    start_date: str = None,
    end_date: str = None,
    home_assistant_host: str = dlt.secrets.value,
    home_assistant_port: str = dlt.secrets.value,
    home_assistant_token: str = dlt.secrets.value,
):
    if end_date:
        end_date = (datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")

    @dlt.resource()
    def entities() -> Generator[List[Dict[str, Any]], Any, Any]:
        """A seed list of repositories to fetch"""
        yield [
            {"name": "device_tracker.chriphone15"},
            {"name": "device_tracker.christian_iphone "},
        ]

    config: RESTAPIConfig = {
        "client": {
            "base_url": f"http://{home_assistant_host}:{home_assistant_port}",
            "auth": BearerTokenAuth(token=home_assistant_token),
        },
        "resource_defaults": {
            "write_disposition": "append",
            "table_format": "delta",
        },
        "resources": [
            {
                "name": "states",
                "endpoint": {
                    "path": "/api/history/period/{start_date}?filter_entity_id={entity}&end_time={end_date}",
                    "params": {
                        "start_date": start_date,
                        "end_date": end_date,
                        "entity": {
                            "type": "resolve",
                            "resource": "entities",
                            "field": "name",
                        },
                    },
                },
            },
            entities(),
        ],
    }

    yield from rest_api_resources(config)

def run_source() -> None:
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(
        pipeline_name="home_assistant_pipeline", 
        destination='filesystem', 
        dataset_name="home_assistant_data"
    )

    # run the pipeline with your parameters
    load_info = pipeline.run(home_assistant_source('2025-09-18', '2025-09-19'))

    # pretty print the information on data that was loaded
    print(load_info)  # noqa: T201


if __name__ == "__main__":
    run_source()