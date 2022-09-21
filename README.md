# Gonudb 

Gonudb is an append-only key/value datastore written in Go.

[![Check Status](https://github.com/iand/gonudb/actions/workflows/check.yml/badge.svg)](https://github.com/iand/gonudb/actions/workflows/check.yml)
[![Test Status](https://github.com/iand/gonudb/actions/workflows/test.yml/badge.svg)](https://github.com/iand/gonudb/actions/workflows/test.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/iand/gonudb)](https://goreportcard.com/report/github.com/iand/gonudb)
[![go.dev reference](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white)](https://pkg.go.dev/github.com/iand/gonudb)

## Overview

Gonudb is a port of [NuDB](https://github.com/CPPAlliance/NuDB), a C++ key/value store.

A Gonudb datastore comprises a data file holding keys and values stored sequentially and an
accompanying key file which forms an on-disk hash table indexing the values stored in the data
file.

During commits a log file is created to store bookkeeping information that may be used to repair the
datastore in the event of a failure.

The data file and key file are independent and a new key file may be rebuilt from the data file if
necessary, potentially with an alternate hashing scheme.


## Installation

Execute `go get github.com/iand/gonudb` within a Go module directory to add it to your module.

## Usage

Gonudb is primarily a library. Import package `github.com/iand/gonudb` to use. A sample application
that demonstrates some simple inserts and fetches is provided in `cmd/gonudbsample`.

An admin tool can be found in `cmd/gonudbadmin` which provides some commands for inspecting and
validating the files that comprise a store.

Install by executing `go install github.com/iand/gonudb/cmd/gonudbadmin` from the root of the 
repository.

 - `gonudbadmin info` can be used to view charactistic information about any of the three files used by gonudb (data, key and log files).
 - `gonudbadmin verify` verifies the consistency of data and key files and shows some statistics on the data they hold.


## Design

Gonudb shares the design ideals that motivated NuDB (but see Status below):

 1. Writes should not block reads.
 2. Reads should be limited only by the SSD's IOPS limit.
 3. A read for a non-present key should require one IOP.
 4. A read for a present key whose data can be read in a single IOP should only require two IOPs, one to figure out where it is and one to read it in.

Keys and values are stored sequentially in an append only data file. The data file begins with a
header that contains characteristic information about the file such as the version of the encoding
scheme, a datastore identifier and an application identifier. Data records follow immediately on
from the header. Each record comprises the size of the value, followed by the size of the key,
followed by the key, followed by the value data. The data file is considered to be immutable and
there are no delete or mutate operations.

Inserts are buffered in memory and periodically committed to disk. Clients are throttled based on
the rate at which data is flushed to disk. Values are immediately discoverable via their key and
may be read from memory or disk.

Keys are hashed and written to buckets stored in the key file. As with the data file, the key file
begins with a header containing characteristic information. The key file's version, datastore
identifier and application identifier must match those in the data file header. Additionally the
key file header contains the hash salt, the block size of each bucket and the target load factor
which determines when a bucket should be split. Buckets are a fixed size and written sequentially
after the header which enables them to the be easily located by index.

Each bucket is assigned a range of hash values and entries within a bucket are ordered by hash. When
the number of entries in a bucket exceeds the load factor it undergoes a split and its entries are
rehashed across the pair of buckets using the linear hashing algorithm. When a bucket exceeds its
capacity it is spilled to the data file and replaced with an empty bucket containing a pointer to
the spill record. A spilled bucket may spill multiple times with the resulting spill records
forming a linked list in the data file.

In the best case reading a record from the datastore requires one read from the key file to load the
relevant bucket and a read from the data file to access the value. Additional reads from the data
file may be required to resolve hash collisions and to load spill records. Read performance is
independent of the size of the datastore and the size of buckets in the key file may be tuned to
the block size of the underlying physical media so loading a bucket may only take a single IOP.

## Status

Version 0.1.0 is an alpha quality functional port of the original NuDB suitable for testing with 
expendable loads. Correctness and safety has been prioritised over performance. Locks are broad in scope
and treat reads and writes with equal priority. Future work will tune the locking bahaviour to 
better meet the goal of writes not blocking reads.

High priority tasks include:

 * Add recover from partial writes
 * Add rekey admin function.
 * Tune locking strategy

Additional features under consideration:

 * Allow alternate hashing functions to be specified.

## Author

Go port written by:

* [Ian Davis](http://github.com/iand) - <http://iandavis.com/>

## License

Distributed under the Boost Software License, Version 1.0. (See accompanying file [LICENSE](LICENSE)
 or copy at http://www.boost.org/LICENSE_1_0.txt)
