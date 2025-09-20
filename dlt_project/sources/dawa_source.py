import dlt
from dlt.sources.rest_api import rest_api_resources, RESTAPIConfig
from typing import Generator, List, Dict, Any
import duckdb

@dlt.source
def dawa_source(
    dawa_base_url: str = dlt.secrets.value,
    duckdb_endpoint: str = dlt.secrets.value,
    duckdb_key_id: str = dlt.secrets.value,
    duckdb_secret: str = dlt.secrets.value,
    duckdb_region: str = dlt.secrets.value,
    duckdb_use_ssl: bool = dlt.secrets.value,
    duckdb_url_style: str = dlt.secrets.value,
):

    @dlt.resource()
    def locations() -> Generator[List[Dict[str, Any]], Any, Any]:
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

            d = con.sql("select latitude_degrees, longitude_degrees from read_parquet('s3://prod/reporting/dagster/location.parquet') where dawa_enriched = 0 and latitude_degrees is not null and longitude_degrees is not null").pl().to_dicts()
        yield d

    config: RESTAPIConfig = {
        "client": {
            "base_url": dawa_base_url,
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
                "name": "dawa_locations",
                "endpoint": {
                    "path": "/adgangsadresser/reverse?x={longitude_degrees}&y={latitude_degrees}",
                    "params": {
                        "longitude_degrees": {
                            "type": "resolve",
                            "resource": "locations",
                            "field": "longitude_degrees",
                        },
                        "latitude_degrees": {
                            "type": "resolve",
                            "resource": "locations",
                            "field": "latitude_degrees",
                        },
                    },
                },
                "include_from_parent": ["latitude_degrees", "longitude_degrees"],
                "write_disposition": "merge",
                "primary_key": ["_locations_latitude_degrees", "_locations_longitude_degrees"],
            },
            locations(),
        ],
    }

    yield from rest_api_resources(config)