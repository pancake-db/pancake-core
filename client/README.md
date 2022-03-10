[![Crates.io][crates-badge]][crates-url]

[crates-badge]: https://img.shields.io/crates/v/pancake-db-client.svg
[crates-url]: https://crates.io/crates/pancake-db-client

# Pancake DB Client

Create a client instance via
```rust
use pancake_db_client::Client;
use std::net::{IpAddr, Ipv4Addr};
let client = Client::from_ip_port(
  IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
  3841,
);
```

You can then make API calls via its async API; e.g.
`let resp = client.api_write_to_partition(&req).await?;`.

For detailed documentation of the client, see
[the docs.rs page](https://docs.rs/pancake-db-client/latest/pancake_db_client/).

For an end-to-end example, see [the client runthrough](examples/runthrough.rs).

For details about the API calls and what all their fields mean,
see [the API docs](https://github.com/pancake-db/pancake-idl).
