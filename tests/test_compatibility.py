#
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
Backward compatibility tests for Airflow 2.11+ and Airflow 3.x.
Ensures the provider's public API surface, inheritance, and
provider registration remain stable across Airflow versions.

Run test: `pytest -s tests/test_compatibility.py`
"""

import pytest


class TestImports:
    """Verify all public modules and classes are importable."""

    def test_import_hook_module(self):
        from cube_provider.hooks import cube  # noqa: F401

    def test_import_cube_hook(self):
        from cube_provider.hooks.cube import CubeHook  # noqa: F401

    def test_import_operator_module(self):
        from cube_provider.operators import cube  # noqa: F401

    def test_import_cube_base_operator(self):
        from cube_provider.operators.cube import CubeBaseOperator  # noqa: F401

    def test_import_cube_query_operator(self):
        from cube_provider.operators.cube import CubeQueryOperator  # noqa: F401

    def test_import_cube_build_operator(self):
        from cube_provider.operators.cube import CubeBuildOperator  # noqa: F401

    def test_import_provider_info(self):
        from cube_provider import get_provider_info  # noqa: F401


class TestInheritance:
    """Verify classes inherit from the correct Airflow base classes."""

    def test_cube_hook_is_base_hook(self):
        from cube_provider.hooks.cube import CubeHook
        from airflow.providers.common.compat.sdk import BaseHook

        assert issubclass(CubeHook, BaseHook)

    def test_cube_base_operator_is_base_operator(self):
        from cube_provider.operators.cube import CubeBaseOperator
        from airflow.providers.common.compat.sdk import BaseOperator

        assert issubclass(CubeBaseOperator, BaseOperator)

    def test_cube_query_operator_is_base_operator(self):
        from cube_provider.operators.cube import CubeQueryOperator
        from airflow.providers.common.compat.sdk import BaseOperator

        assert issubclass(CubeQueryOperator, BaseOperator)

    def test_cube_build_operator_is_base_operator(self):
        from cube_provider.operators.cube import CubeBuildOperator
        from airflow.providers.common.compat.sdk import BaseOperator

        assert issubclass(CubeBuildOperator, BaseOperator)

    def test_cube_query_extends_cube_base(self):
        from cube_provider.operators.cube import CubeBaseOperator, CubeQueryOperator

        assert issubclass(CubeQueryOperator, CubeBaseOperator)

    def test_cube_build_extends_cube_base(self):
        from cube_provider.operators.cube import CubeBaseOperator, CubeBuildOperator

        assert issubclass(CubeBuildOperator, CubeBaseOperator)


class TestHookPublicAPI:
    """Verify CubeHook exposes the expected public attributes and methods."""

    def test_hook_class_attributes(self):
        from cube_provider.hooks.cube import CubeHook

        assert CubeHook.conn_name_attr == "cube_conn_id"
        assert CubeHook.default_conn_name == "cube_default"
        assert CubeHook.conn_type == "generic"
        assert CubeHook.hook_name == "Cube"

    def test_hook_has_get_conn(self):
        from cube_provider.hooks.cube import CubeHook

        assert callable(getattr(CubeHook, "get_conn", None))

    def test_hook_has_run(self):
        from cube_provider.hooks.cube import CubeHook

        assert callable(getattr(CubeHook, "run", None))

    def test_hook_has_get_connection(self):
        """get_connection is inherited from BaseHook."""
        from cube_provider.hooks.cube import CubeHook

        assert callable(getattr(CubeHook, "get_connection", None))

    def test_hook_has_log(self):
        """log is inherited from BaseHook."""
        from cube_provider.hooks.cube import CubeHook

        hook = CubeHook()
        assert hasattr(hook, "log")


class TestOperatorPublicAPI:
    """Verify operators expose the expected public attributes."""

    def test_base_operator_template_fields(self):
        from cube_provider.operators.cube import CubeBaseOperator

        assert "endpoint" in CubeBaseOperator.template_fields
        assert "method" in CubeBaseOperator.template_fields
        assert "data" in CubeBaseOperator.template_fields
        assert "headers" in CubeBaseOperator.template_fields

    def test_base_operator_has_execute(self):
        from cube_provider.operators.cube import CubeBaseOperator

        assert callable(getattr(CubeBaseOperator, "execute", None))

    def test_query_operator_has_execute(self):
        from cube_provider.operators.cube import CubeQueryOperator

        assert callable(getattr(CubeQueryOperator, "execute", None))

    def test_build_operator_has_execute(self):
        from cube_provider.operators.cube import CubeBuildOperator

        assert callable(getattr(CubeBuildOperator, "execute", None))

    def test_xcom_push_rejected(self):
        """xcom_push kwarg must raise an error (AirflowException on Airflow 2, TypeError on Airflow 3)."""
        from airflow.providers.common.compat.sdk import AirflowException
        from cube_provider.operators.cube import CubeBaseOperator

        with pytest.raises((TypeError, AirflowException), match="xcom_push"):
            CubeBaseOperator(task_id="test", xcom_push=True)


class TestProviderRegistration:
    """Verify provider info is well-formed for Airflow's provider discovery."""

    def test_get_provider_info_returns_dict(self):
        from cube_provider import get_provider_info

        info = get_provider_info()
        assert isinstance(info, dict)

    def test_provider_info_required_keys(self):
        from cube_provider import get_provider_info

        info = get_provider_info()
        assert "package-name" in info
        assert "name" in info
        assert "description" in info
        assert "versions" in info
        assert "connection-types" in info

    def test_provider_info_package_name(self):
        from cube_provider import get_provider_info

        info = get_provider_info()
        assert info["package-name"] == "airflow-provider-cube"

    def test_provider_info_connection_types(self):
        from cube_provider import get_provider_info

        info = get_provider_info()
        conn_types = info["connection-types"]
        assert len(conn_types) >= 1
        assert any(
            ct["connection-type"] == "generic"
            and ct["hook-class-name"] == "cube_provider.hooks.cube.CubeHook"
            for ct in conn_types
        )

    def test_provider_info_versions_is_list(self):
        from cube_provider import get_provider_info

        info = get_provider_info()
        assert isinstance(info["versions"], list)
        assert len(info["versions"]) >= 1


class TestCompatImports:
    """Verify that the compat layer resolves to the correct base classes."""

    def test_compat_base_hook_resolves(self):
        """The compat import should resolve to a usable BaseHook class."""
        from airflow.providers.common.compat.sdk import BaseHook

        assert hasattr(BaseHook, "get_connection")

    def test_compat_base_operator_resolves(self):
        """The compat import should resolve to a usable BaseOperator class."""
        from airflow.providers.common.compat.sdk import BaseOperator

        assert hasattr(BaseOperator, "execute")

    def test_compat_airflow_exception_resolves(self):
        """The compat import should resolve to a usable AirflowException."""
        from airflow.providers.common.compat.sdk import AirflowException

        assert issubclass(AirflowException, Exception)
