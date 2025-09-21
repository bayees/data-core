from typing import Generator, Any, Dict, List
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources
import dlt
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from datetime import datetime, timedelta

@dlt.source
def home_assistant_source(
    start_date: str = None,
    end_date: str = None,
    home_assistant_host: str = dlt.secrets.value,
    home_assistant_port: str = dlt.secrets.value,
    home_assistant_token: str = dlt.secrets.value,
):
    @dlt.resource(write_disposition="append")
    def entities() -> Generator[List[Dict[str, Any]], Any, Any]:
        """A seed list of entities to fetch"""
        yield [
            {"name": "device_tracker.chriphone15"},
            {"name": "device_tracker.christian_iphone "},
        ]

    @dlt.resource(write_disposition="append", primary_key=["entity_id", "last_changed"])
    def states(
        last_updated=dlt.sources.incremental(
            "last_updated",
            initial_value=(datetime.now() - timedelta(days=180)).strftime("%Y-%m-%dT%H:%M:%S"),
            last_value_func=max
        )
    ):
        """Fetch states with incremental loading support using dlt.sources.incremental"""

        # Get the start value from incremental state
        actual_start_date = last_updated.start_value

        # Determine if this is initial load or incremental
        if last_updated.start_value == last_updated.initial_value:
            # Override with provided start_date if this is the initial load
            if start_date:
                actual_start_date = start_date
            print(f"Initial full load: fetching data from {actual_start_date}")
        else:
            print(f"Incremental load: fetching data since {actual_start_date}")

        # Determine end date
        if end_date:
            actual_end_date = (datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            actual_end_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

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
                            "start_date": actual_start_date,
                            "end_date": actual_end_date,
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

        # Process the data and add a last_updated field for incremental tracking
        for items in rest_api_resources(config):
            # The API returns nested arrays, so we need to flatten them
            if isinstance(items, list):
                for item_group in items:
                    if isinstance(item_group, list):
                        for item in item_group:
                            # Add last_updated field for incremental tracking
                            if "last_changed" in item:
                                item["last_updated"] = item["last_changed"]
                            elif "last_updated" in item:
                                pass  # Already has the field
                            else:
                                # Fallback to current timestamp if neither field exists
                                item["last_updated"] = datetime.now().isoformat()
                            yield item
                    elif isinstance(item_group, dict):
                        # Add last_updated field for incremental tracking
                        if "last_changed" in item_group:
                            item_group["last_updated"] = item_group["last_changed"]
                        elif "last_updated" not in item_group:
                            item_group["last_updated"] = datetime.now().isoformat()
                        yield item_group
            elif isinstance(items, dict):
                # Add last_updated field for incremental tracking
                if "last_changed" in items:
                    items["last_updated"] = items["last_changed"]
                elif "last_updated" not in items:
                    items["last_updated"] = datetime.now().isoformat()
                yield items

        print(f"Incremental state will be updated to: {last_updated.last_value}")

    return states

def run_source() -> None:
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(
        pipeline_name="home_assistant_pipeline",
        destination='filesystem',
        dataset_name="home_assistant"
    )

    # run the pipeline with your parameters
    load_info = pipeline.run(home_assistant_source())

    # pretty print the information on data that was loaded
    print(load_info)  # noqa: T201


if __name__ == "__main__":
    run_source()