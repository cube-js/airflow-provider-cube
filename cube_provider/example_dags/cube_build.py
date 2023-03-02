import json
from typing import Any
from pendulum import datetime
from airflow.decorators import dag, task
from cube_provider.operators.cube import CubeBuildOperator

@dag(
    start_date=datetime(2023, 1, 1),
    schedule=None,
    default_args={"retries": 1, "cube_conn_id": "cube_default"},
    tags=["cube", "build", "example"],
)
def cube_build_workflow():
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

    build_op = CubeBuildOperator(
        task_id = "build_op",
        headers = {},
        selector = {
            "contexts": [
                { "securityContext": { "tenant": "t1" } },
                { "securityContext": { "tenant": "t2" } },
                { "securityContext": { "tenant": "t3" } },
                { "securityContext": { "tenant": "t4" } },
            ],
            "timezones": ["UTC"],
            "datasources": ["default"],
            "cubes": ["MajesticMillion"],
            "preAggregations": ["MajesticMillion.ManualAgg"],
        },
        complete = False,
        wait = 10,
    )

    @task()
    def print_op(data: Any):
        """
        #### Print task
        A simple Print task which takes the data and just prints it out.
        """

        print(f"Total order value is: {data}")

    print_op(build_op.output)

cube_build_workflow()
