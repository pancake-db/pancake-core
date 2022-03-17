[![Crates.io][crates-badge]][crates-url]

[crates-badge]: https://img.shields.io/crates/v/pancake-db-client.svg
[crates-url]: https://crates.io/crates/pancake-db-client

# PancakeDB Client

The PancakeDB client supports
* the full PancakeDB API via GRPC,
* helper macros to build requests more easily,
* and higher-level functionality for reads.

Most users will primarily use the client library for writing data with
`write_to_partition` requests and occasionally use it for table creation,
alteration, and deletion.

## Get Started
For basic usage and detailed explanations, see
[the docs.rs page](https://docs.rs/pancake-db-client/latest/pancake_db_client/).

For a complete example, see [the runthrough](./examples/runthrough.rs).

For details about the API calls and what all their fields mean,
see [the API docs](https://github.com/pancake-db/pancake-idl).
