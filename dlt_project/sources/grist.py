from dlt.sources.rest_api import RESTAPIConfig,rest_api_resources
import dlt

table_names = ["All", "Balance", "Interests", "Team_meetings", "Sets", "Missing_pieces"]

def create_table_resource(table_name):
    return {
        "name": table_name.lower(),
        "endpoint": {
            "path": "/docs/{doc_id}/tables/{table_id}/records",
            "params": {
                "doc_id": {
                    "type": "resolve",
                    "resource": "tables",
                    "field": "_docs_urlId",
                },
                "table_id": {
                    "type": "resolve",
                    "resource": "tables",
                    "field": "id",
                }
            },
        },
        "processing_steps": [
            {"filter": lambda x: x["_tables_id"] == table_name},
        ],
        "include_from_parent": ["id"],
    }

@dlt.source
def grist_source(
    grist_base_url: str = dlt.secrets.value,
    grist_api_token: str = dlt.secrets.value,
):

    config: RESTAPIConfig = {
        "client": {
            "base_url": grist_base_url,
            "headers": {
                "Authorization": f'Bearer {grist_api_token}'
            },
            "paginator": {
                "type": "single_page",
            },
        },
        "resource_defaults": {
            "primary_key": "id",
            "write_disposition": "merge",
            "table_format": "delta",
        },
        "resources": [
            {
                "name": "orgs",
                "endpoint": {
                    "path": "orgs",
                },
            },
            {
                "name": "workspaces",
                "endpoint": {
                    "path": "orgs/{org_id}/workspaces",
                    "params": {
                        "org_id": {
                            "type": "resolve",
                            "resource": "orgs",
                            "field": "id",
                        },
                    },
                },
                "include_from_parent": ["id"],
            },
            {
                "name": "docs",
                "endpoint": {
                    "path": "workspaces/{workspace_id}",
                    "data_selector": "docs",
                    "params": {
                        "workspace_id": {
                            "type": "resolve",
                            "resource": "workspaces",
                            "field": "id",
                        },
                    },
                },
                "include_from_parent": ["id"],
            },
            {
                "name": "tables",
                "endpoint": {
                    "path": "/docs/{doc_id}/tables",
                    "params": {
                        "doc_id": {
                            "type": "resolve",
                            "resource": "docs",
                            "field": "id",
                        },
                    },
                },
                "include_from_parent": ["urlId"],
            },
        ] 
        + [create_table_resource(table_name) for table_name in table_names]
    }
        
    yield from rest_api_resources(config)


def run_source() -> None:
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(
        pipeline_name="grist_pipeline",
        destination='filesystem',
        dataset_name="grist_data"
    )

    # run the pipeline with your parameters
    load_info = pipeline.run(grist_source())

    # pretty print the information on data that was loaded
    print(load_info)  # noqa: T201

if __name__ == "__main__":
    run_source()