# Concepts

## Blob

A BLOB (**B**inary **L**arge **OB**ject) is a chunk of bytes of an arbitrary
length. It can be a single file, a segment of the file or a piece of metadata.

Since the goal of this project is to provide content-addressable storage,
Blobs in the system are immutable. For mutable objects, see Pins.

## Ref

A Ref is cryptographic hash of a Blob content. It consists of two parts:
the hash algorithm and the hash payload.

For example, SHA256-based Ref may look like `sha256:fee8292...` in a
text encoding.

## Schema blob

A schema blob contains a metadata about other blobs or their relations.
In current version, schema blobs are stored as JSON objects with a specific
formatting, so they can be distinguished from other JSON blobs.

As an example, a schema blob may describe a Unix file with a name, a size
and permission bits and points to a blob with actual file content.

## Pin

Pin is a mutable named variable that points to a single Ref. Since a Ref
may point to a schema blob that describes a file, a directory or any other object,
it is possible in general to store arbitrary mutable objects in CAS.

This concept is similar to a branch in Git or a [permanode](https://perkeep.org/doc/terms#permanode) in Perkeep.