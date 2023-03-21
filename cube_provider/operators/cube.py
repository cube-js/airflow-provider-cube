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

from __future__ import annotations
from typing import TYPE_CHECKING, Any, Dict
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from cube_provider.hooks.cube import CubeHook
import time
import json

if TYPE_CHECKING:
    from airflow.utils.context import Context


class CubeBaseOperator(BaseOperator):
    """
    Calls Cube's REST API endpoint to execute an action.

    :param cube_conn_id: connection to run the operator with
    :param endpoint: the relative part of the full url
    :param method: the HTTP method to use
    :param data: the data to pass
    :param headers: the HTTP headers to be added to the request
    """

    # jinja templating
    template_fields = ["endpoint", "method", "data", "headers"]
    template_fields_renderers = {"headers": "json", "data": "py"}
    template_ext = ()
    ui_color = "#f4a460"

    def __init__(
        self,
        *,
        cube_conn_id: str = CubeHook.default_conn_name,
        endpoint: str | None = None,
        method: str = "POST",
        data: Any | None = None,
        headers: dict[str, str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.cube_conn_id = cube_conn_id
        self.endpoint = endpoint
        self.method = method
        self.data = data or {}
        self.headers = headers or {}

        if kwargs.get("xcom_push") is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead"
            )

    def execute(self, context: Context) -> Any:
        hook = CubeHook(cube_conn_id=self.cube_conn_id)
        self.log.info("Cube API method call")
        return hook.run(
            endpoint=self.endpoint,
            method=self.method,
            data=self.data,
            headers=self.headers,
        )


class CubeQueryOperator(CubeBaseOperator):
    """
    Retrieve data from a Cube.

    :param cube_conn_id: connection to run the operator with
    :param headers: the HTTP headers to be added to the request
    :param query: Cube query object
    :param timeout: timeout in seconds to wait for an API call to respond (default 30 sec)
    :param wait: number of seconds to wait between API calls (default 10 sec)
    """

    def __init__(
        self,
        *,
        cube_conn_id: str = CubeHook.default_conn_name,
        headers: dict[str, str] | None = None,
        query: Any | None = None,
        timeout: int = 30,
        wait: int = 10,
        **kwargs,
    ) -> None:
        super().__init__(
            cube_conn_id=cube_conn_id,
            endpoint="/cubejs-api/v1/load",
            method="GET",
            data={"query": json.dumps(query)},
            headers=headers,
            **kwargs,
        )
        self.timeout = timeout
        self.wait = wait

    def execute(self, context: Context) -> Any:
        elapsed_time = 0
        while not self.timeout or elapsed_time <= self.timeout:
            data = super().execute(context)
            if "error" in data.keys() and "Continue wait" in data["error"]:
                self.log.info(f"Continue wait error. Sleeping {self.wait} sec.")
                time.sleep(self.wait)
                elapsed_time += self.wait
                continue
            else:
                return data

        msg = f"Cube API took longer than {self.timeout} seconds."
        self.log.warning(msg)
        raise Exception(msg)


class CubeBuildOperator(CubeBaseOperator):
    """
    Performs Cube's pre-aggregations build.

    :param cube_conn_id: connection to run the operator with
    :param headers: the HTTP headers to be added to the request
    :param selector: object representing valid `pre-aggregations/jobs` API selector
    :param complete: whether the task should wait for the job run completion or not (default False)
    :param wait: number of seconds to wait between API calls (default 10 sec)
    """

    def __init__(
        self,
        *,
        cube_conn_id: str = CubeHook.default_conn_name,
        headers: dict[str, str] | None = None,
        selector: Dict = {},
        complete: bool = False,
        wait: int = 10,
        **kwargs,
    ) -> None:
        super().__init__(
            cube_conn_id=cube_conn_id,
            endpoint="/cubejs-api/v1/pre-aggregations/jobs",
            method="POST",
            data=json.dumps(
                {
                    "action": "post",
                    "selector": selector,
                },
            ),
            headers=headers,
            **kwargs,
        )

        self.selector = selector
        self.complete = complete
        self.wait = wait

    def execute(self, context: Context) -> Any:
        tokens = super().execute(context)
        if not self.complete:
            return tokens

        hook = CubeHook(cube_conn_id=self.cube_conn_id)
        iterate = len(tokens) > 0
        while iterate:
            time.sleep(self.wait)

            self.log.info("Get build status.")
            statuses = hook.run(
                endpoint="/cubejs-api/v1/pre-aggregations/jobs",
                method="POST",
                data=json.dumps(
                    {
                        "action": "get",
                        "resType": "object",
                        "tokens": tokens,
                    }
                ),
                headers=self.headers,
            )

            self.log.info("Check build status.")
            missing_only = True
            all_tokens = statuses.keys()
            in_process = []
            for token in all_tokens:
                status = statuses[token]["status"]
                if status.find("failure") >= 0:
                    raise Exception(f"Cube pre-aggregations build failed: {status}.")
                if status != "missing_partition":
                    missing_only = False
                if status != "done":
                    in_process.append(token)

            if missing_only:
                raise Exception(
                    "Cube pre-aggregations build failed: missing partitions."
                )

            iterate = len(in_process) > 0

        return True
