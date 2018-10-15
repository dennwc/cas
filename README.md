# Content Addressable Storage

This project implements a simple and pragmatic approach to Content Addressable Storage (CAS).
It was heavily influenced by [Perkeep](https://perkeep.org/) (aka Camlistore) and Git.

For more details, see [concepts](./docs/concepts.md) and [comparison](./docs/comparison.md) with other systems.

## Status

The project is in an active development. Both API and on-disk format may change.

Check the [Quick start guide](./docs/quickstart.md) for a list of basic commands.

## Goals

- **Simplicity:** the core specification should be trivial to implement.

- **Interop:** CAS should play nicely with existing tools and technologies,
    either content-addressable or not.

- **Easy to use:** CAS should be a single command away, similar to `git init`.

## Use cases

- Immutable and versioned archives: CAS supports files with multiple
  TBs of data, folders with millions of files and can index and use remote
  data without storing it locally.

- Data processing pipelines: CAS caching capabilities allows to use it for
  incremental data pipelines.

- Git for large files: CAS stores files with an assumption that they can
  be multiple TBs and is optimized for this use case, while still supporting
  tags and branches, like Git.

## Features and the roadmap

**Implemented:**

- Fast file hashing
    - SHA-256, other can be used
    - Stores results in file attributes (cache)
- Support for large archives
    - Large contiguous files (> TB)
    - Large multipart files (> TB)
    - Large directories (> millions of files)
    - Zero-copy file fetch (BTRFS)
- Integrations
    - Can index and sync web content
    - HTTP(S) caching (as a Go library)
- Remote storage
    - Self-hosted HTTP CAS server (read-only)
    - Google Cloud Storage
- Usability
    - Mutable objects (pins)
    - Local storage in Git fashion
- Data pipelines
    - Extendable
    - Caches results
    - Incremental

**Planned:**

- Support for large multipart files (> TB)
    - Support multilevel parts
    - Support blob splitters (rolling checksum, new line, etc)
- Remote storage
    - AWS, etc
    - Self-hosted HTTP CAS server (read-write)
- Integration with Git
    - Zero-copy fetch from Git (either remote or local)
    - LFS integration
- Integration with Docker
    - Zero-copy fetch of an image from Docker
    - Unpack FS images to CAS
    - Use containers in pipelines
- Integration with BitTorrent:
    - Store torrent files
    - Download torrent data directly to CAS
    - To consider: expose CAS as a peer
- Integration with other CAS systems:
    - Perkeep
    - Upspin
    - IPFS
- Windows and OSX support
- Better support for pipelines
