# Airflow Cube Provider

Cube is the semantic layer for building data applications. It helps 
data engineers and application developers access data from modern data
stores, organize it into consistent definitions, and deliver it to
every application.

## Connection requirements

`CubeHook` uses `Generic` connection. So, in order to create a 
connection to a Cube you will need to create an Airflow `Generic` 
connection and specify the following required properties:

* `host` - your Cube's host (must contain schema, address and port);
* `password` - your Cube's `CUBEJS_API_SECRET`;
* `extra.security_context` - your user `securityContext` (in a Cube's terms).

Rest of the connection fields will be ignored.

> Note: the `CubeBuildOperator` uses Cube's `/cubejs-api/v1/pre-aggregations/jobs`
endpoint that is forbiden by the default. Make sure that specified 
`securityContext` and Cube's `contextToPermissions` function are 
configured in a way that allow you to run this query. See Cube's 
documentation for a more context.

## Package description

This package provides the common `CubeHook` class, the abstract 
`CubeBaseOperator`, the `CubeQueryOperator` to run analytical queries 
over a Cube and the `CubeBuildOperator` to run pre-aggregations build 
process.

## Dependencies

Python 3.10

| Package            | Version |
|--------------------|---------|
| typing             | 2.6.0   |
| jwt                | 2.6.0   |
| json               | 2.0.9   |
| requests           | 2.28.2  |
| airflow            | 2.5.1   |
