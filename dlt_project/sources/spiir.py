from typing import Any
import dlt
import requests
import json
import lxml.html # https://brennan.io/2016/03/02/logging-in-with-requests/

@dlt.source
def spiir_source(
    spiir_base_url = dlt.secrets.value,  # type: ignore[assignment]
    spiir_email = dlt.secrets.value,  # type: ignore[assignment]
    spiir_password = dlt.secrets.value  # type: ignore[assignment]
) -> Any:
    """
    The source is the API of Something

    Generator:
    The generator function handles the schema contract and is flexible
    for both the datatype of all existing columns and for new columns

    Returns:
    dlt Resource
    """

    @dlt.resource(
        table_format="delta",
        write_disposition="replace",
        primary_key="id",
    )
    def transactions():
        try:
            with requests.Session() as s:
                # Get login page
                login = s.get(f'{spiir_base_url}/log-ind')
                login.raise_for_status()
                
                login_html = lxml.html.fromstring(login.text)
                hidden_inputs = login_html.xpath(r'//form//input[@type="hidden"]')
                form = {x.attrib["name"]: x.attrib["value"] for x in hidden_inputs}
                form['email'] = spiir_email
                form['password'] = spiir_password

                # Perform login
                login_response = s.post(f'{spiir_base_url}/log-ind', data=form, allow_redirects=True)
                login_response.raise_for_status()

                # Get transactions
                response = s.get(
                    f'{spiir_base_url}/Profile/ExportAllPostingsToJson',
                    headers={'Accept': 'application/json'}
                )
                response.raise_for_status()

            # Handle UTF-8 BOM
            response_data = json.loads(response.text.encode().decode('utf-8-sig'))
            yield response_data

        except requests.RequestException as e:
            raise Exception(f"Failed to fetch data from SPIIR: {str(e)}")
        except json.JSONDecodeError as e:
            raise Exception(f"Failed to parse SPIIR response: {str(e)}")
        except Exception as e:
            raise Exception(f"Unexpected error in SPIIR source: {str(e)}")

    return transactions

def run_source() -> None:
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(
        pipeline_name="spiir_pipeline",
        destination='filesystem',
        dataset_name="spiir_data"
    )

    # run the pipeline with your parameters
    load_info = pipeline.run(spiir_source())

    # pretty print the information on data that was loaded
    print(load_info)  # noqa: T201


if __name__ == "__main__":
    run_source()