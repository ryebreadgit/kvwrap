# Changelog

## [0.1.2] - 2026-04-22

### Added

- Add --experimental_allow_proto3_optional option

### Changed

- Update version to 0.1.2

## [0.1.1] - 2026-04-14

### Added

- Add readme for each crate
- Add core description
- Add workspace description
- Add todo to readme

### Changed

- Update version

### Chores

- Cargo update

### Fixed

- Fix spelling in description

## [0.1.0] - 2026-04-14

### Added

- Include version, edition, authors, license, and repository in crates. Add version to workspace
- Add README.md and license
- Add basic example, add pollster for example as dev-depedency
- Initial all_keys implementation
- Add sled to bin by default for now
- Add watch_key and watch_prefix, intitial RemoteStore support without proxying
- Add sled backend feature, default to fjall
- Add profile.release options
- Move json parsing to get & set_json_impl. Add to dyn KvStore as well
- Add Arc implementations from dyn KvStore to fix Sized requirement issues
- Add verbose and node_id args
- Add Dockerfile and compose.yml for server binary
- Create router_config by default, add additional formatter options
- Add generate_default_config and router_config_path options
- Add fjwrap_proto default exports
- Add serde Serialize and Deserialize to tonic_prost_build
- Add additional cluster service, add comments to services
- Add tonic imports properly
- Initial RemoteStore implementation
- Initial gRPC server functionality
- Implement initial LocalStore functionality
- Initial commit
- Build initial StaticRouter for shard metadata routing

### Changed

- Rename all_keys to scan to now return a tuple of the key and value
- Switch partition back to a string as it fits better with fjall, removes base64 dependancy
- Rename project to kvwrap
- Update .get_json() to return Option like .get()
- Update include_proto! to updated package name
- Change partition to allow any binary partition name. Use base64 for partition name to allow the most possibilities as Keyspace requires a str
- Move compose.yml to compose.yml.example
- Rename KeyRange start to begin for sorting
- Move server over to it, proxy requests where it does not exist on shard
- Switch to just using Error
- Move protos to fjwrap-proto
- Replace tokio with blocking in remote client
- Capitalize env vars
- Split proto files
- Merge KvStoreExt into default functions in KvStore directly
- Extend default KeyRange length, set default start to None to show this works. update write path to correct router_config_path
- Use async-compat on client for agnostic async-runtime on client-side
- Use fjwrap directly rather than the components

### Chores

- Cargo machete
- Cargo fix
- Ignore IDE files
- Ignore default config path

### Fixed

- Make connect_lazy to fix async tokio errors

### Removed

- Remove default dependancies to instead flow down from package defaults
- Remove compose.yml
- Remove unused imports
