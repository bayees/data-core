import dlt
import requests

entities = [
    #{ "name": "completed_info", "root": "completed_info", "primary_key": "id" },
    { "name": "collaborators", "root": "collaborators", "primary_key": "id" },
    #{ "name": "collaborator_states", "root": "collaborator_states", "primary_key": "id" },
    #{ "name": "day_orders", "root": "day_orders", "primary_key": "id" },
    { "name": "filters", "root": "filters", "primary_key": "id" },
    { "name": "items", "root": "items", "primary_key": "id" },
    { "name": "labels", "root": "labels", "primary_key": "id" },
    { "name": "live_notifications", "root": "live_notifications", "primary_key": "id" },
    #{ "name": "locations", "root": "locations", "primary_key": "id" },
    #{ "name": "notes", "root": "notes", "primary_key": "id" },
    #{ "name": "project_notes", "root": "project_notes", "primary_key": "id" },
    { "name": "projects", "root": "projects", "primary_key": "id" },
    { "name": "reminders", "root": "reminders", "primary_key": "id" },
    { "name": "sections", "root": "sections", "primary_key": "id" },
    #{ "name": "settings_notifications", "root": "settings_notifications", "primary_key": "id" },
    #{ "name": "stats", "root": "stats", "primary_key": "id" },
    #{ "name": "user", "root": "user", "primary_key": "id" },
    #{ "name": "user_plan_limits", "root": "user_plan_limits", "primary_key": "id" },
    #{ "name": "user_settings", "root": "user_settings", "primary_key": "id" },
    #{ "name": "workspace_users", "root": "workspace_users", "primary_key": "id" },
]

@dlt.source
def todoist_source(
    todoist_url: str = dlt.secrets.value,
    todoist_token: str = dlt.secrets.value
):

    def get_todoist_entity(
            entity,
            sync_token: dlt.sources.incremental=dlt.sources.incremental("sync_token", "*")
        ):
        url = todoist_url + "/sync"
        headers = {
            "Authorization": f"Bearer {todoist_token}",
        }
        data = {
            "sync_token": sync_token.last_value,
            "resource_types": [entity["name"]]
        }
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        root = response.json()
        yield [{"sync_token": root["sync_token"], **entity} for entity in root[entity["root"]]]

    for entity in entities:
        yield dlt.resource(
            get_todoist_entity,
            name=entity["name"],
            write_disposition="merge",
            table_format="delta",
            primary_key=entity["primary_key"]
        )(entity)

    @dlt.resource(
        write_disposition="merge",
        table_format="delta",
        primary_key="id"
    )
    def sync_labels(self) -> dlt.sources.incremental:
        return self.read_resource("labels")


def run_source() -> None:
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(
        pipeline_name="spiir_pipeline",
        destination='filesystem',
        dataset_name="spiir_data"
    )

    # run the pipeline with your parameters
    load_info = pipeline.run(todoist_source())

    # pretty print the information on data that was loaded
    print(load_info)  # noqa: T201


if __name__ == "__main__":
    run_source()