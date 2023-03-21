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
from typing import Any
import json
import jwt
import requests
from airflow.hooks.base import BaseHook


class CubeHook(BaseHook):
    """
    Cube Hook that interacts with the Cube Server via HTTP.

    :param cube_conn_id: connection that has the base API url and auth info.
    """

    conn_name_attr = "cube_conn_id"
    default_conn_name = "cube_default"
    conn_type = "generic"
    hook_name = "Cube"

    def __init__(
        self,
        cube_conn_id: str = default_conn_name,
    ) -> None:
        super().__init__()
        self.cube_conn_id = cube_conn_id
        self.base_url: str = ""

    def get_conn(self, headers: dict[str, Any] | None = None) -> requests.Session:
        """
        Returns http session to use with requests. Uses generic connection. Connection
        `host` field is required and must contain schema, address and port. Cube's secret must
        be specified in the connection `password` field. To specify Cube security context
        `extra.security_context` field must be used. Rest of the connection fields will be
        ignored.

        :param headers: additional headers to be passed through as a dictionary.
        """

        session = requests.Session()

        if self.cube_conn_id:
            conn = self.get_connection(self.cube_conn_id)
            key = "DEFAULT_SECRET"

            # host
            if conn.host and "://" in conn.host:
                self.base_url = conn.host
            else:
                raise TypeError("Cube's connection host is missed or invalid.")

            # password
            if conn.password:
                key = conn.password
            else:
                raise TypeError("Cube's connection secret is missed.")

            # extra
            if conn.extra:
                extra = json.loads(conn.extra)
                if "security_context" in extra:
                    payload = extra["security_context"]
                else:
                    raise TypeError("Cube's securityContext is missed or invalid.")
            else:
                raise TypeError("Cube's securityContext is missed or invalid.")
            extra.pop("security_context", None)

            # payload
            if "exp" not in payload and "expiresIn" not in payload:
                payload["expiresIn"] = "7d"

            # auth token
            extra["Authorization"] = jwt.encode(
                payload=payload, key=key, algorithm="HS256"
            )

            # extra headers
            try:
                extra["Content-type"] = "application/json"
                session.headers.update(extra)
            except TypeError:
                self.log.warning("Connection to %s has invalid extra field.", conn.host)

        # specified headers
        if headers:
            session.headers.update(headers)
        return session

    def run(
        self,
        endpoint: str | None = None,
        method: str = "POST",
        data: dict[str, Any] | str | None = None,
        headers: dict[str, Any] | None = None,
        **request_kwargs,
    ) -> Any:
        """
        Performs the request.

        :param endpoint: the endpoint to be called i.e.
            `/cubejs-api/v1/load` or `/cubejs-api/v1/pre-aggregations/jobs`
        :param data: payload to be uploaded or request parameters
        :param headers: additional headers to be passed through as a dictionary
        """

        # api session
        session: requests.Session = self.get_conn(headers)

        # api url
        if (
            self.base_url
            and not self.base_url.endswith("/")
            and endpoint
            and not endpoint.startswith("/")
        ):
            url = self.base_url + "/" + endpoint
        else:
            url = (self.base_url or "") + (endpoint or "")

        # api call
        try:
            self.log.info(f"Sending {method} to the url {url}")
            if method.upper() == "GET":
                response = session.get(url=url, params=data)
            if method.upper() == "POST":
                response = session.post(url=url, data=data)
        except requests.exceptions.ConnectionError as e:
            self.log.warning("%s Tenacity will retry to execute the operation", e)
            raise e

        # response
        if response.status_code == 200:
            return response.json()
        else:
            msg = f"{method} request to the {url} failed with the reason: {response.reason}"
            self.log.warning(msg)
            raise Exception(msg)
