from importlib.metadata import version

__name__ = "airflow-provider-cube"
__version__ = version(__name__)

def get_provider_info():
    return {
        "package-name": __name__,
        "name": "Cube Apache Airflow Provider",
        "description": "A Cube Apache Airflow provider package built by Cube.",
        "versions": [__version__],
        "connection-types": [
            {"connection-type": "generic", "hook-class-name": "cube_provider.hooks.cube.CubeHook"}
        ],
    }
