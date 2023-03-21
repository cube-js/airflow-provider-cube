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

"""
Unittest module to test Operators.
Requires the json, unittest, pytest, and requests-mock Python libraries.
Run test: `pytest -s tests/operators/test_cube_operator.py`
"""

import os
import json
import pytest
import logging
import requests_mock
from unittest import mock
from cube_provider.operators.cube import (
    CubeBaseOperator,
    CubeQueryOperator,
    CubeBuildOperator,
)

log = logging.getLogger(__name__)

AIRFLOW_CONN_CUBE_TEST = json.dumps(
    {
        "conn_type": "generic",
        "host": "https://example.cube.dev",
        "password": "23dff8b29cf20df38a4c78dfaf689fa55916add4d27ee3dd9ba75d14732aafa3",
        "extra": {"security_context": {"expiresIn": "7d"}},
    }
)


@mock.patch.dict("os.environ", AIRFLOW_CONN_CUBE_TEST=AIRFLOW_CONN_CUBE_TEST)
class TestCubeBaseOperator:
    """
    Test Cube Base Operator.
    """

    def test_get(self):
        with requests_mock.Mocker() as m:
            m.get("https://example.cube.dev/api", json={"data": "mocked response"})
            op = CubeBaseOperator(
                task_id="base_op",
                cube_conn_id="cube_test",
                endpoint="/api",
                method="GET",
            )
            payload = op.execute(context={})
            assert payload["data"] == "mocked response"

    def test_get_error(self):
        with requests_mock.Mocker() as m:
            m.get("https://example.cube.dev/api", status_code=400)
            op = CubeBaseOperator(
                task_id="base_op",
                cube_conn_id="cube_test",
                endpoint="/api",
                method="GET",
            )
            with pytest.raises(
                Exception,
                match="GET request to the https://example.cube.dev/api failed with the reason: None",
            ):
                op.execute(context={})

    def test_post(self):
        with requests_mock.Mocker() as m:
            m.post("https://example.cube.dev/api", json={"data": "mocked response"})
            op = CubeBaseOperator(
                task_id="base_op",
                cube_conn_id="cube_test",
                endpoint="/api",
                method="POST",
            )
            payload = op.execute(context={})
            assert payload["data"] == "mocked response"

    def test_post_error(self):
        with requests_mock.Mocker() as m:
            m.post("https://example.cube.dev/api", status_code=400)
            op = CubeBaseOperator(
                task_id="base_op",
                cube_conn_id="cube_test",
                endpoint="/api",
                method="POST",
            )
            with pytest.raises(
                Exception,
                match="POST request to the https://example.cube.dev/api failed with the reason: None",
            ):
                op.execute(context={})


@mock.patch.dict("os.environ", AIRFLOW_CONN_CUBE_TEST=AIRFLOW_CONN_CUBE_TEST)
class TestCubeQueryOperator:
    """
    Test Cube Query Operator.
    """

    def test_timeout(self):
        with requests_mock.Mocker() as m:
            m.get(
                "https://example.cube.dev/cubejs-api/v1/load?query=%7B%7D",
                json={"data": "mocked response"},
                status_code=200,
            )
            op = CubeQueryOperator(
                task_id="query_op",
                cube_conn_id="cube_test",
                query={},
                timeout=-1,
            )
            with pytest.raises(
                Exception, match="Cube API took longer than -1 seconds."
            ):
                op.execute(context={})

    def test_continue_wait(self):
        with requests_mock.Mocker() as m:
            m.get(
                "https://example.cube.dev/cubejs-api/v1/load?query=%7B%7D",
                [
                    {"json": {"error": "Continue wait"}, "status_code": 200},
                    {"json": {"data": "mocked response"}, "status_code": 200},
                ],
            )
            op = CubeQueryOperator(
                task_id="query_op",
                cube_conn_id="cube_test",
                query={},
                wait=0,
            )
            payload = op.execute(context={})
            assert payload["data"] == "mocked response"


response_tokens = [
    "be598e318484848cbb06291baa59ca3a",
    "d4bb22530aa9905219b2f0e6a214c39f",
    "e1578a60514a7c55689016adf0863965",
]
response_status_missing_partition = {
    "e1578a60514a7c55689016adf0863965": {
        "table": "preaggs.e_commerce__manual_updates20201201_kuggpskn_alfb3s4u_1hmrdkc",
        "status": "missing_partition",
        "selector": {
            "cubes": ["ECommerce"],
            "preAggregations": ["ECommerce.ManualUpdates"],
            "contexts": [{"securityContext": {"tenant": "t2"}}],
            "timezones": ["America/Los_Angeles"],
            "dataSources": ["default"],
        },
    },
    "d4bb22530aa9905219b2f0e6a214c39f": {
        "table": "preaggs.e_commerce__manual_updates20201101_rvfrwirb_ucnfhp2g_1hmrdkc",
        "status": "missing_partition",
        "selector": {
            "cubes": ["ECommerce"],
            "preAggregations": ["ECommerce.ManualUpdates"],
            "contexts": [{"securityContext": {"tenant": "t2"}}],
            "timezones": ["America/Los_Angeles"],
            "dataSources": ["default"],
        },
    },
    "be598e318484848cbb06291baa59ca3a": {
        "table": "preaggs.e_commerce__manual_updates20201201_kbn0y0iy_fvfip33o_1hmrdkc",
        "status": "missing_partition",
        "selector": {
            "cubes": ["ECommerce"],
            "preAggregations": ["ECommerce.ManualUpdates"],
            "contexts": [{"securityContext": {"tenant": "t2"}}],
            "timezones": ["UTC"],
            "dataSources": ["default"],
        },
    },
}
response_status_failure = {
    "e1578a60514a7c55689016adf0863965": {
        "table": "preaggs.e_commerce__manual_updates20201201_kuggpskn_alfb3s4u_1hmrdkc",
        "status": "done",
        "selector": {
            "cubes": ["ECommerce"],
            "preAggregations": ["ECommerce.ManualUpdates"],
            "contexts": [{"securityContext": {"tenant": "t2"}}],
            "timezones": ["America/Los_Angeles"],
            "dataSources": ["default"],
        },
    },
    "d4bb22530aa9905219b2f0e6a214c39f": {
        "table": "preaggs.e_commerce__manual_updates20201101_rvfrwirb_ucnfhp2g_1hmrdkc",
        "status": "done",
        "selector": {
            "cubes": ["ECommerce"],
            "preAggregations": ["ECommerce.ManualUpdates"],
            "contexts": [{"securityContext": {"tenant": "t2"}}],
            "timezones": ["America/Los_Angeles"],
            "dataSources": ["default"],
        },
    },
    "be598e318484848cbb06291baa59ca3a": {
        "table": "preaggs.e_commerce__manual_updates20201201_kbn0y0iy_fvfip33o_1hmrdkc",
        "status": "failure: returned error",
        "selector": {
            "cubes": ["ECommerce"],
            "preAggregations": ["ECommerce.ManualUpdates"],
            "contexts": [{"securityContext": {"tenant": "t2"}}],
            "timezones": ["UTC"],
            "dataSources": ["default"],
        },
    },
}
response_status_processing = {
    "e1578a60514a7c55689016adf0863965": {
        "table": "preaggs.e_commerce__manual_updates20201201_kuggpskn_alfb3s4u_1hmrdkc",
        "status": "done",
        "selector": {
            "cubes": ["ECommerce"],
            "preAggregations": ["ECommerce.ManualUpdates"],
            "contexts": [{"securityContext": {"tenant": "t2"}}],
            "timezones": ["America/Los_Angeles"],
            "dataSources": ["default"],
        },
    },
    "d4bb22530aa9905219b2f0e6a214c39f": {
        "table": "preaggs.e_commerce__manual_updates20201101_rvfrwirb_ucnfhp2g_1hmrdkc",
        "status": "processing",
        "selector": {
            "cubes": ["ECommerce"],
            "preAggregations": ["ECommerce.ManualUpdates"],
            "contexts": [{"securityContext": {"tenant": "t2"}}],
            "timezones": ["America/Los_Angeles"],
            "dataSources": ["default"],
        },
    },
    "be598e318484848cbb06291baa59ca3a": {
        "table": "preaggs.e_commerce__manual_updates20201201_kbn0y0iy_fvfip33o_1hmrdkc",
        "status": "scheduled",
        "selector": {
            "cubes": ["ECommerce"],
            "preAggregations": ["ECommerce.ManualUpdates"],
            "contexts": [{"securityContext": {"tenant": "t2"}}],
            "timezones": ["UTC"],
            "dataSources": ["default"],
        },
    },
}
response_status_done = {
    "e1578a60514a7c55689016adf0863965": {
        "table": "preaggs.e_commerce__manual_updates20201201_kuggpskn_alfb3s4u_1hmrdkc",
        "status": "done",
        "selector": {
            "cubes": ["ECommerce"],
            "preAggregations": ["ECommerce.ManualUpdates"],
            "contexts": [{"securityContext": {"tenant": "t2"}}],
            "timezones": ["America/Los_Angeles"],
            "dataSources": ["default"],
        },
    },
    "d4bb22530aa9905219b2f0e6a214c39f": {
        "table": "preaggs.e_commerce__manual_updates20201101_rvfrwirb_ucnfhp2g_1hmrdkc",
        "status": "done",
        "selector": {
            "cubes": ["ECommerce"],
            "preAggregations": ["ECommerce.ManualUpdates"],
            "contexts": [{"securityContext": {"tenant": "t2"}}],
            "timezones": ["America/Los_Angeles"],
            "dataSources": ["default"],
        },
    },
    "be598e318484848cbb06291baa59ca3a": {
        "table": "preaggs.e_commerce__manual_updates20201201_kbn0y0iy_fvfip33o_1hmrdkc",
        "status": "done",
        "selector": {
            "cubes": ["ECommerce"],
            "preAggregations": ["ECommerce.ManualUpdates"],
            "contexts": [{"securityContext": {"tenant": "t2"}}],
            "timezones": ["UTC"],
            "dataSources": ["default"],
        },
    },
}


@mock.patch.dict("os.environ", AIRFLOW_CONN_CUBE_TEST=AIRFLOW_CONN_CUBE_TEST)
class TestCubeBuildOperator:
    """
    Test Cube Build Operator.
    """

    def test_uncompleted(self):
        with requests_mock.Mocker() as m:
            m.post(
                "https://example.cube.dev/cubejs-api/v1/pre-aggregations/jobs",
                json={"data": "mocked response"},
                status_code=200,
            )
            op = CubeBuildOperator(
                task_id="build_op",
                cube_conn_id="cube_test",
                headers={},
                selector={},
                complete=False,
                wait=0,
            )
            payload = op.execute(context={})
            assert payload["data"] == "mocked response"

    def test_completed_missing_partition(self):
        with requests_mock.Mocker() as m:
            m.post(
                "https://example.cube.dev/cubejs-api/v1/pre-aggregations/jobs",
                [
                    {"json": response_tokens, "status_code": 200},
                    {"json": response_status_missing_partition, "status_code": 200},
                ],
            )
            op = CubeBuildOperator(
                task_id="build_op",
                cube_conn_id="cube_test",
                headers={},
                selector={},
                complete=True,
                wait=0,
            )
            with pytest.raises(
                Exception,
                match="Cube pre-aggregations build failed: missing partitions.",
            ):
                op.execute(context={})

    def test_completed_failure(self):
        with requests_mock.Mocker() as m:
            m.post(
                "https://example.cube.dev/cubejs-api/v1/pre-aggregations/jobs",
                [
                    {"json": response_tokens, "status_code": 200},
                    {"json": response_status_failure, "status_code": 200},
                ],
            )
            op = CubeBuildOperator(
                task_id="build_op",
                cube_conn_id="cube_test",
                headers={},
                selector={},
                complete=True,
                wait=0,
            )
            with pytest.raises(
                Exception,
                match="Cube pre-aggregations build failed: failure: returned error",
            ):
                op.execute(context={})

    def test_completed(self):
        with requests_mock.Mocker() as m:
            m.post(
                "https://example.cube.dev/cubejs-api/v1/pre-aggregations/jobs",
                [
                    {"json": response_tokens, "status_code": 200},
                    {"json": response_status_processing, "status_code": 200},
                    {"json": response_status_done, "status_code": 200},
                ],
            )
            op = CubeBuildOperator(
                task_id="build_op",
                cube_conn_id="cube_test",
                headers={},
                selector={},
                complete=True,
                wait=0,
            )
            payload = op.execute(context={})
            assert payload == True
