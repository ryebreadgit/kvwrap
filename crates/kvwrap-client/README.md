# kvwrap-client

Lightweight client for connecting to remote kvwrap clusters.

Implements the `KvStore` trait over gRPC, allowing applications to interact with a distributed kvwrap instance. Enable this through the `client` feature on the [`kvwrap`](https://crates.io/crates/kvwrap) crate rather than depending on it directly.

For full documentation see the [project README](https://github.com/ryebreadgit/kvwrap).

## License

[MIT](../../LICENSE.md)