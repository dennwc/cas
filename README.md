# Content Addressable Storage

This project implements a simple and pragmatic approach to Content Addressable Storage (CAS).
It was heavily influenced by [Perkeep](https://perkeep.org/) (aka Camlistore) and Git.

## Goals

- **Simplicity:** the core specification should be trivial to implement.

- **Interop:** CAS should play nicely with existing tools and technologies,
    either content-addressable or not.

- **Easy to use:** CAS should be a single command away, similar to `git init`.


## Status

The project is in an active development. Both API and on-disk format may change.