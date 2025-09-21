import dlt
from dlt.sources.rest_api import rest_api_resources, RESTAPIConfig
from typing import Generator, List, Dict, Any
import duckdb

@dlt.source
def rebrickable_source(
    rebrickable_base_url: str = dlt.secrets.value,
    rebrickable_api_key: str = dlt.secrets.value,
    duckdb_endpoint: str = dlt.secrets.value,
    duckdb_key_id: str = dlt.secrets.value,
    duckdb_secret: str = dlt.secrets.value,
    duckdb_region: str = dlt.secrets.value,
    duckdb_use_ssl: bool = dlt.secrets.value,
    duckdb_url_style: str = dlt.secrets.value,
):

    @dlt.resource()
    def inventory_sets() -> Generator[List[Dict[str, Any]], Any, Any]:
        """A seed list of repositories to fetch"""
        with duckdb.connect() as con:
            con.sql(f"""create secret s3_secret (
                    type S3, 
                    key_id '{duckdb_key_id}', 
                    secret '{duckdb_secret}', 
                    region '{duckdb_region}',
                    endpoint '{duckdb_endpoint}',
                    use_ssl '{duckdb_use_ssl}',
                    url_style '{duckdb_url_style}'
            );""")

            d = con.sql("select distinct fields__set_number as set_num from delta_scan('s3://prod/base/grist/sets')").pl().to_dicts()
        yield d

    config: RESTAPIConfig = {
        "client": {
            "base_url": rebrickable_base_url,
            "headers": {
                "Authorization": f"key {rebrickable_api_key}",
            },
            "paginator": {
                "type": "single_page",
            },
            
        },
        "resource_defaults": {
            "write_disposition": "append",
            "table_format": "delta",
        },
        "resources": [
            {
                "name": "lego_sets",
                "endpoint": {
                    "path": "/lego/sets/{lego_set}/",
                    "params": {
                        "lego_set": {
                            "type": "resolve",
                            "resource": "inventory_sets",
                            "field": "set_num",
                        },
                    },
                },
                "write_disposition": "merge",
                "primary_key": "set_num",
            },
            {
                "name": "lego_parts",
                "endpoint": {    
                    "path": "/lego/sets/{lego_set}/parts/",
                    "params": {
                        "lego_set": {
                            "type": "resolve",
                            "resource": "inventory_sets",
                            "field": "set_num",
                        },
                    },
                },
                "max_table_nesting": 1,
                "include_from_parent": ["set_num"],
                "write_disposition": "merge",
                "primary_key": "id",
            },
            {
                "name": "lego_minifigs",
                "endpoint": { 
                    "path": "/lego/sets/{lego_set}/minifigs/",
                    "params": {
                        "lego_set": {
                            "type": "resolve",
                            "resource": "inventory_sets",
                            "field": "set_num",
                        },
                    },
                },
                "max_table_nesting": 1,
                "include_from_parent": ["set_num"],
                "write_disposition": "merge",
                "primary_key": "id",
            },
            {
                "name": "lego_alternates",
                "endpoint": {
                    "path": "/lego/sets/{lego_set}/alternates/",
                    "params": {
                        "lego_set": {
                            "type": "resolve",
                            "resource": "inventory_sets",
                            "field": "set_num",
                        },
                    },
                },
                "max_table_nesting": 1,
                "include_from_parent": ["set_num"],
                "write_disposition": "merge",
                "primary_key": "set_num",
            },
            inventory_sets(),
        ],
    }

    yield from rest_api_resources(config)

def run_source() -> None:
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(
        pipeline_name="rebrickable_pipeline",
        destination='filesystem',
        dataset_name="rebrickable"
    )

    # run the pipeline with your parameters
    load_info = pipeline.run(rebrickable_source())