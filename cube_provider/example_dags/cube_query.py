# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

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
            "measures": [
                "ECommerce.totalQuantity",
                "ECommerce.totalProfit"
            ],
            "dimensions": [
                "ECommerce.productName"
            ],
            "timeDimensions": [
            {
                "dimension": "ECommerce.orderDate",
                "dateRange": [
                    "2020-01-01 00:00:00.000",
                    "2020-03-30 22:50:50.999"
                ],
                "granularity": "quarter"
            }
            ]
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
