import json
from typing import Any
from pendulum import datetime
from airflow.decorators import dag, task
from cube_provider.operators.cube import CubeQueryOperator

@dag(
    start_date=datetime(2023, 1, 1),
    schedule=None,
    default_args={"retries": 1, "cube_conn_id": "cube_default"},
    tags=["cube", "query", "example"],
)
def cube_query_workflow():
    """
    ### Cube example DAG

    Showcases the Cube provider package's operators.

    To run this example, create a Cube connection with:
    - id: cube_default
    - type: generic
    - host: https://api.url.cubecloudapp.dev (can be reached out from the Cube Overview `REST API` field)
    - password: 6f24a5a0e82065bcd8a91ea03a3e8vqlz5hk7 (can be reached out from the CUBEJS_API_SECRET env)
    - extra: {"security_context": {"expiresIn": "7d"}}
    """

    query_op = CubeQueryOperator(
        task_id = "query_op",
        query = {
            "limit": 5000,
            "dimensions": ["MajesticMillion.Domain"],
            "order": { "MajesticMillion.Domain": "asc" }
        }
    )

    @task()
    def print_op(data: Any):
        """
        #### Print task
        A simple Print task which takes the data and just prints it out.
        """

        print(f"Fetched data: {data}")

    print_op(query_op.output)

cube_query_workflow()
