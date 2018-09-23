# Quick start guide

## Installation

CAS requires Go 1.10+ to build. For Ubuntu, you can install Go via Snap:
```
snap install --classic go
```

Pull the Go CAS package:
```
go get -u github.com/dennwc/cas
```

And install the CAS binary:
```
go install github.com/dennwc/cas/cmd/cas
```

## Hashing files

The most basic use case of CAS tool is to hash a file:

```
$ cas hash file.dat
sha256:694b27f0... file.dat
```

If you run the command the second time, the result will be calculated
much faster:

```
$ cas hash file.dat
sha256:694b27f0... file.dat (cached)
```

CAS uses extended file attributes to store a precomputed hash to avoid
calculating it each time. It will record the file modification time and
the size to make sure that cached hash is still valid. Still, in theory,
there are ways to fool CAS to use cache when the file content was changed.
To force hash calculation, `--force` flag can be passed:

```
$ cas hash -f file.dat
sha256:694b27f0... file.dat
```

This will also update the cache in case it was not valid.

## Init CAS

To start, find a directory where the CAS file will be stored. Local CAS
storage follows the Git approach and will create a `.cas` folder to store
the data, thus you can use any existing folder.

```
$ mkdir my-data
$ cd my-data
$ cas init
```

This will create a directory named `.cas` in the `my-data` directory.

## Working with files

You can now store files in CAS. The easiest way to fo it is via `cas fetch`:

```
$ echo "file content" > file.dat
$ cas fetch file.dat
sha256:694b27f0... file.dat
```

The command returns the hash of the file, or related metadata blob. This
hash is called a ["ref"](./concepts.md#ref) in CAS.

`cas fetch` will try to clone file data without storing it twice if possible,
or will fallback to a full copy otherwise. For better performance, it's highly
recommended to use a filesystem that supports file clone operations (BTRFS).

The file is now stored in CAS. To retrieve it, pass the full ref of the file
to the `cas blob get` command:

```
$ cas blob get sha256:694b27f0... > file_cas.dat
```

But refss are not particularly easy to use. Instead, CAS provides a mechanism
called ["pins"](./concepts.md#pin), that is similar to branches in Git.

To save a file to CAS and pin it, run:

```
$ cas pull file.dat
root = sha256:694b27f0...
```

By default, CAS uses a pin with a name "root" (similar to "master" branch
in Git). You can also use a custom pin name:

```
$ cas pull latest file.dat
latest = sha256:694b27f0...
```

Now you can get or set a pin by hand using `cas pin` commands, but to get
a file it's easier to just pass the pin name to `cas blob get`:

```
$ cas blob get latest > file_cas.dat
```

## Working with directories

The same set of commands can be used to store directories:

```
$ mkdir sub
$ cp *.dat ./sub/
$ cas pull sub
root = sha256:2de1ed8f...
```

If you try to run `cas blob get root`, it will print a JSON instead of
unpacking the content:

```json
{
 "@type": "cas:Directory",
 "list": [
  {
   "ref": "sha256:694b27f0...",
   "name": "file.dat",
   "size": 13
  },
  {
   "ref": "sha256:694b27f0...",
   "name": "file_cas.dat",
   "size": 13
  }
 ]
}
```

The reason is that `cas blob get` is a low level command that returns blobs
without interpreting them. To unpack the folder (or a file), use `cas checkout`:

```
$ cas checkout root dst
sha256:2de1ed8f... -> dst
$ ls dst
file_cas.dat  file.dat
```

As you can see, the folder was successfully restored to the state recorded
in CAS.

## Working with web content

One of the design goal of CAS is to work well with existing technologies,
either content-addressable or not. One of such technologies is the Web.
As a first integration point, CAS allows to pull web files directly:

```
$ cas fetch https://imgs.xkcd.com/comics/curve_fitting.png
sha256:34fc6626... https://imgs.xkcd.com/comics/curve_fitting.png
```

But if we check returned blob (with `cas blob get`), we will get a schema blob:

```json
{
 "@type": "cas:WebContent",
 "url": "https://imgs.xkcd.com/comics/curve_fitting.png",
 "ref": "sha256:c271e78f...",
 "size": 89192,
 "etag": "5ba2f0de-15c68",
 "ts": "2018-09-20T00:59:10Z"
}
```

As you can see, CAS also records the URL from which the file was received,
the ETag, timestamp, the size and the ref of an actual blob.
`cas fetch` will actually pull the content of an URL to CAS, but there is
an alternative method: only index web content, without pulling the data.

```
$ cas index https://imgs.xkcd.com/comics/curve_fitting.png
sha256:34fc6626... https://imgs.xkcd.com/comics/curve_fitting.png
```

It will return the same ref as before and will read the content of that
URL (to calculate the ref), but won't store the data locally.

This effectively creates a link for CAS that can be used to either sync
remote data from time to time, or to always retrieve the data from
remote without storing it locally, while preserving CAS properties.

<!-- TODO: implement `cas sync` and allow `cas get` to read remote data -->

### Synchronizing web data

Since CAS stores Etag and timestamp when pulling data from the Web, it
is able to verify that remote data is still the same. If web server supports
ETags or `If-Modified-Since` header, the response will be instant. If not,
CAS will still need to read the content of an URL to verify if it was not
modified.

<!-- TODO: examples of `cas sync` -->

## Pipelines

One of the strongest points of any CAS system is its ability to cache and
reuse existing data. One of the use cases that can benefit from it are
data processing pipelines.

CAS provides a basic support for pipelines. For example, let's calculate
a number of lines in a file. To do this, we need to install a line count
module:

```
go install github.com/dennwc/cas/cmd/cas-pipe-lines
```

CAS will accept any binary that is installed in `PATH` and starts with
a `cas-pipe-` prefix as a pipeline module.

To run a pipeline, we need to specify a name of the pipeline command and
a ref or a pin of the file/folder:

```
$ cas pull input file.txt
input = sha256:691129fa...
$ cas pipeline lines input
sha256:691129fa... -> sha256:916a819c...
```

<!-- TODO: allow to store pipeline results in a pin -->

The result of a pipeline can be checked in the resulting ref:

```
$ cas blob get sha256:916a819c...
{"lines":162}
```

If we run the pipeline again, CAS will notice that its inputs are the same,
and will use a cached result:

```
$ cas pipeline lines input
sha256:691129fa... -> sha256:916a819c... (cached)
```

This allows to incrementally process files by pulling the new folder version
into a pin, and running the pipeline again. CAS will only process files if
the content changed since the last run, or if the pipeline module was modified.

To check how the data is cached in CAS, you can list all schema blobs of type
`cas:TransformOp` and get one of them:

```
$ cas schema list -t cas:TransformOp
sha256:a578f4aa... 280 cas:TransformOp
$ cas blob get sha256:a578f4aa...
```
```json
{
 "@type": "cas:TransformOp",
 "src": "sha256:691129fa...",
 "op": "sha256:7d4c1493...",
 "dst": "sha256:916a819c..."
}
```

You may recognize the `src` field as am input file ref, the `dst` as an
output file ref, and an `op` is a ref of a module binary that was used.