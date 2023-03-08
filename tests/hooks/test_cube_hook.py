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
Unittest module to test Hooks.
Requires the json, unittest, pytest, and requests-mock Python libraries.
Run test: `pytest -s tests/hooks/test_cube_hook.py`
"""

import os
import json
import pytest
import logging
import requests_mock
from unittest import mock
from cube_provider.hooks.cube import CubeHook

log = logging.getLogger(__name__)

AIRFLOW_CONN_CUBE_NO_HOST=json.dumps({
    "conn_type": "generic",
    "password": "23dff8b29cf20df38a4c78dfaf689fa55916add4d27ee3dd9ba75d14732aafa3",
    "extra": {
        "security_context": {
            "expiresIn": "7d"
        }
    }
})

AIRFLOW_CONN_CUBE_WRONG_HOST=json.dumps({
    "conn_type": "generic",
    "host": "example.cube.dev",
    "password": "23dff8b29cf20df38a4c78dfaf689fa55916add4d27ee3dd9ba75d14732aafa3",
    "extra": {
        "security_context": {
            "expiresIn": "7d"
        }
    }
})

AIRFLOW_CONN_CUBE_NO_PASS=json.dumps({
    "conn_type": "generic",
    "host": "https://example.cube.dev",
    "extra": {
        "security_context": {
            "expiresIn": "7d"
        }
    }
})

AIRFLOW_CONN_CUBE_NO_EXTRA=json.dumps({
    "conn_type": "generic",
    "host": "https://example.cube.dev",
    "password": "23dff8b29cf20df38a4c78dfaf689fa55916add4d27ee3dd9ba75d14732aafa3"
})

AIRFLOW_CONN_CUBE_WRONG_EXTRA=json.dumps({
    "conn_type": "generic",
    "host": "https://example.cube.dev",
    "password": "23dff8b29cf20df38a4c78dfaf689fa55916add4d27ee3dd9ba75d14732aafa3",
    "extra": {
        "securityContext": {
            "expiresIn": "7d"
        }
    }
})

AIRFLOW_CONN_CUBE_TEST=json.dumps({
    "conn_type": "generic",
    "host": "https://example.cube.dev",
    "password": "23dff8b29cf20df38a4c78dfaf689fa55916add4d27ee3dd9ba75d14732aafa3",
    "extra": {
        "security_context": {
            "expiresIn": "7d"
        }
    }
})

@mock.patch.dict('os.environ', AIRFLOW_CONN_CUBE_NO_HOST=AIRFLOW_CONN_CUBE_NO_HOST)
@mock.patch.dict('os.environ', AIRFLOW_CONN_CUBE_WRONG_HOST=AIRFLOW_CONN_CUBE_WRONG_HOST)
@mock.patch.dict('os.environ', AIRFLOW_CONN_CUBE_NO_PASS=AIRFLOW_CONN_CUBE_NO_PASS)
@mock.patch.dict('os.environ', AIRFLOW_CONN_CUBE_NO_EXTRA=AIRFLOW_CONN_CUBE_NO_EXTRA)
@mock.patch.dict('os.environ', AIRFLOW_CONN_CUBE_WRONG_EXTRA=AIRFLOW_CONN_CUBE_WRONG_EXTRA)
@mock.patch.dict('os.environ', AIRFLOW_CONN_CUBE_TEST=AIRFLOW_CONN_CUBE_TEST)

class TestCubeHook():
    """
    Test Cube Hook.
    """

    def test_no_host(self):
        hook = CubeHook(cube_conn_id='cube_no_host')
        with pytest.raises(TypeError, match="Cube's connection host is missed or invalid."):
            hook.run()

    def test_wrong_host(self):
        hook = CubeHook(cube_conn_id='cube_wrong_host')
        with pytest.raises(TypeError, match="Cube's connection host is missed or invalid."):
            hook.run()

    def test_no_pass(self):
        hook = CubeHook(cube_conn_id='cube_no_pass')
        with pytest.raises(TypeError, match="Cube's connection secret is missed."):
            hook.run()

    def test_no_extra(self):
        hook = CubeHook(cube_conn_id='cube_no_extra')
        with pytest.raises(TypeError, match='Cube\'s securityContext is missed or invalid.'):
            hook.run()

    def test_wrong_extra(self):
        hook = CubeHook(cube_conn_id='cube_wrong_extra')
        with pytest.raises(TypeError, match='Cube\'s securityContext is missed or invalid.'):
            hook.run()

    def test_get(self):
        with requests_mock.Mocker() as m:
            m.get('https://example.cube.dev/api', json={'data': 'mocked response'})
            hook = CubeHook(cube_conn_id='cube_test')
            payload = hook.run(endpoint = '/api', method = "GET")
            assert payload['data'] == 'mocked response'

    def test_get_error(self):
        with requests_mock.Mocker() as m:
            m.get('https://example.cube.dev/api', status_code=400)
            hook = CubeHook(cube_conn_id='cube_test')
            with pytest.raises(
                Exception,
                match='GET request to the https://example.cube.dev/api failed with the reason: None'
            ):
                hook.run(endpoint = '/api', method = "GET")

    def test_post(self):
        with requests_mock.Mocker() as m:
            m.post('https://example.cube.dev/api', json={'data': 'mocked response'})
            hook = CubeHook(cube_conn_id='cube_test')
            payload = hook.run(endpoint = '/api', method = "POST")
            assert payload['data'] == 'mocked response'

    def test_post_error(self):
        with requests_mock.Mocker() as m:
            m.post('https://example.cube.dev/api', status_code=400)
            hook = CubeHook(cube_conn_id='cube_test')
            with pytest.raises(
                Exception,
                match='POST request to the https://example.cube.dev/api failed with the reason: None'
            ):
                hook.run(endpoint = '/api', method = "POST")
