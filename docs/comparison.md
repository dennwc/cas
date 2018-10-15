# How CAS is different from X

## Perkeep

Perkeep was an original inspiration for CAS. This is a great personal
content addressable storage with strong security mechanisms and support
for multiple kinds of backends.

Both systems address blobs in the same way and store metadata in a similar
fashion.

But there are few notable differences:

| | Perkeep | CAS
--- | --- | ---
Max blob size | 16 MB | no limit
Zero copy (local) | - | +
Default splitting | rolling checksum | none
Custom split | + (as a library) | +
Mutations | + (requires an index) | +
ACLs | per blob (requires index) | per repository
Metadata signing | + (required) | - *
Interop | - | +
Indexing | external | external / internal


### Blob size limit

Perkeep has a limit on a maximal size of a blob, which is 16MB. In practice,
blobs will be even smaller (see below).

CAS has no such limitation and allows user to choose how blobs are split.

Because of this, Perkeep is able to retry upload of any blob since it
always fits into memory.

CAS trades of this ability to allow user to store large files in CAS
without copying it first. Filesystems such as BTRFS allows to clone a file
so blocks are shared between an original file and an immutable clone of a
file in CAS, saving the disk space and making loading files of any size
to CAS almost instant.

### Blob splitting

Perkeep uses a rolling checksum to split blobs before the 16MB boundary.

CAS allows to either store data as a single blob, or to split it with
a custom splitting function (rolling checksum, line break, etc).

Again, this allows CAS to reuse existing files by making an immutable clone
(if supported by file system).

### Mutability

Perkeep uses a claim system to achieve mutability. This means that each
tag or branch (permanode) will always be stored as a metadata blob.
It allows to see all the mutations that happened with a given permanode.

The downside is that Perkeep is not able to work with permanodes until
all the existing permanode claims are indexed.

CAS follows the Git model, where tags/branches (pins) are stored separately
from blobs and are directly supported by underlying storage. This allows
CAS to work with branches and tags without building an index first.

Optionally, CAS supports archiving pins to the storage to preserve the
history of mutations.

### ACLs

Perkeep has an awesome ACL system based on claims that allows a granular
permissions to be set to blobs and even subtrees.

At the same time, this comes at the cost of requiring a full index to be
built before any blob can be accessed.

CAS follows a Git model, where an access is managed only per repository,
simplifying the protocol and allowing to store and retrieve blobs without
an index.

### Security

All Perkeep mutations are required to be signed by an owner. This provides
a great level of security and allows to validate who made each individual
change.

CAS does not require a signature for blobs or metadata. Instead it relies
on the trust established between the client and the server via HTTPS.
This does not prevent bad actors from rewriting the storage on the server,
but it provides similar level of security as Git, assuming commits are
not signed by an author.

There are plans to support optional metadata signing in CAS.


### Interop

Perkeep is meant to be a personal storage and the primary data management
system. The data is expected to be stored either locally, or delegated
to a remote blob server of Perkeep instance.

CAS is designed to coexists with other systems like Git, Perkeep,
BitTorrent, IPFS or even plain web servers.

This means that if you want to have an access to some data in Perkeep,
you will need to store it in one of Perkeep instances first.

With CAS, you can index a URL, but decide to not store the data in CAS.
In this case, it will allow you to access the data as if the endpoint
was running a CAS system. Thus, data in CAS is allowed to be hosted in
any kind of system, while CAS will only store a metadata describing the
content and the method how to access it.

### Indexing

Perkeep expects a separate index server to be deployed. Usually the blob
server will be remote, while an indexing server will be local (but not
required to).

CAS assumes a basic indexing capabilities from all storage implementations,
and, at the same time, allows to store an index files as blobs.
This hybrid system allows to calculate and store an index as an immutable
blob for clients to use, while storing the list of those indexes in the
storage itself. This simplifies the setup, since there is no need to run
a separate index server.