[![Build status](https://travis-ci.org/quasar-analytics/quasar.svg?branch=master)](https://travis-ci.org/quasar-analytics/quasar)
[![Coverage Status](https://coveralls.io/repos/quasar-analytics/quasar/badge.svg)](https://coveralls.io/r/quasar-analytics/quasar)
[![Latest version](https://index.scala-lang.org/quasar-analytics/quasar/quasar-web/latest.svg)](https://index.scala-lang.org/quasar-analytics/quasar/quasar-web)
[![Join the chat at https://gitter.im/quasar-analytics/quasar](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/quasar-analytics/quasar?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

# Thanks to Sponsors

YourKit supports open source projects with its full-featured Java Profiler. YourKit, LLC is the creator of <a href="https://www.yourkit.com/java/profiler/index.jsp">YourKit Java Profiler</a> and <a href="https://www.yourkit.com/.net/profiler/index.jsp">YourKit .NET Profiler</a>, innovative and intelligent tools for profiling Java and .NET applications.

# Quasar

Quasar is an open source NoSQL analytics engine that can be used as a library or through a REST API to power advanced analytics across a growing range of data sources and databases, including MongoDB.

## SQL²

SQL² is the dialect of SQL that Quasar understands.

SQL² is a superset of standard SQL. Therefore, in the following documentation SQL² will be used interchangeably with SQL.

See the [SQL² tutorial](http://quasar-analytics.org/docs/sqltutorial/) for more info on SQL².

SQL² supports variables inside queries (`SELECT * WHERE pop < :cutoff`). Values for these variables, which can be any expression, should be specified as additional parameters in the url, using the variable name prefixed by `var.` (e.g. `var.cutoff=1000`). Failure to specify valid values for all variables used inside a query will result in an error. These values use the same syntax as the query itself; notably, strings should be surrounded by double quotes. Some acceptable values are `123`, `"CO"`, and `DATE("2015-07-06")`.

## Using the Pre-Built JARs

In [Github Releases](http://github.com/quasar-analytics/quasar/releases), you can find pre-built JARs for all the subprojects in this repository.

See the instructions below for running and configuring these JARs.

## Building from Source

**Note**: This requires Java 8 and Bash (Linux, Mac, or Cygwin on Windows).

### Build

The following sections explain how to build and run the various subprojects.

#### Basic Compile & Test

To compile the project and run tests, first clone the quasar repo and then execute the following command:

```bash
./sbt test
```

Note: please note that we are not using here a system wide sbt, but our own copy of it (under ./sbt). This is primarily
 done for determinism. In order to have a reproducible build, the helper script needs to be part of the repo.

Running the full test suite can be done in two ways:

##### Testing option 1 (prerequisite: docker)

A docker container running mongodb operates for the duration of the test run.

```bash
./bin/full-it-tests.sh
```

##### Testing option 2

In order to run the integration tests for a given backend, you will need to provide a URL to it. For instance, in the case of MongoDB, If you have a hosted MongoDB instance handy, then you can simply point to it, or else
you probably want to install MongoDB locally and point Quasar to that one. Installing MongoDB locally is probably a good idea as it will
allow you to run the integration tests offline as well as make the tests run as fast as possible.

In order to install MongoDB locally you can either use something like Homebrew (on OS X) or simply go to the MongoDB website and follow the
instructions that can be found there.

Once we have a MongoDB instance handy, we need to configure the integration tests
in order to inform Quasar about where to find the backends to test.

Simply change the values of the config values in '/it/testing.conf'. For example:

```
mongodb_3_2="mongodb://<mongoURL>"
mongodb_3_0="mongodb://<mongoURL>"
mongodb_2_6="mongodb://<mongoURL>"
```

where <mongoURL> is the url at which one can find a Mongo database. For example <mongoURL> would probably look
something like `localhost:27017` for a local installation. This means the integration tests will be run against
both MongoDB versions 2.6, 3.0, and 3.2. Alternatively, you can choose to install only one of these and run the integration
tests against only that one database. Simply omit a version in order to avoid testing against it. On the integration
server, the tests are run against all supported filesystems.

#### REPL JAR

To build a JAR for the REPL, which allows entering commands at a command-line prompt, execute the following command:

```bash
./sbt 'repl/assembly'
```

The path of the JAR will be `./repl/target/scala-2.11/quasar-repl-assembly-[version].jar`, where `[version]` is the Quasar version number.

To run the JAR, execute the following command:

```bash
java -jar [<path to jar>] [-c <config file>]
```

#### Web JAR

To build a JAR containing a lightweight HTTP server that allows you to programmatically interact with Quasar, execute the following command:

```bash
./sbt 'web/assembly'
```

The path of the JAR will be `./web/target/scala-2.11/quasar-web-assembly-[version].jar`, where `[version]` is the Quasar version number.

To run the JAR, execute the following command:

```bash
java -jar [<path to jar>] [-c <config file>]
```

### Configure

The various JARs can be configured by using a command-line argument to indicate the location of a JSON configuration file. If no config file is specified, it is assumed to be `quasar-config.json`, from a standard location in the user's home directory.

The JSON configuration file must have the following format:

```json
{
  "server": {
    "port": 8080
  },

  "mountings": {
    "/": {
      "mongodb": {
        "connectionUri": "mongodb://<user>:<pass>@<host>:<port>/<dbname>"
      }
    }
  }
}
```

One or more mountings may be included, and each must have a unique path (above, `/`), which determines where in the filesystem the database(s) contained by the mounting will appear.

#### Database mounts

If the mount's key is "mongodb", then the `connectionUri` is a standard [MongoDB connection string](http://docs.mongodb.org/manual/reference/connection-string/). Only the primary host is required to be present, however in most cases a database name should be specified as well. Additional hosts and options may be included as specified in the linked documentation.

For example, say a MongoDB instance is running on the default port on the same machine as Quasar, and contains databases `test` and `students`, the `students` database contains a collection `cs101`, and the configuration looks like this:
```json
  "mountings": {
    "/local/": {
      "mongodb": {
        "connectionUri": "mongodb://localhost/test"
      }
    }
  }
```
Then the filesystem will contain the paths `/local/test/` and `/local/students/cs101`, among others.

A database can be mounted at any directory path, but database mount paths must not be nested inside each other.

##### MongoDB

To connect to MongoDB using TLS/SSL, specify `?ssl=true` in the connection string, and also provide the following via system properties when launching either JAR (i.e. `java -Djavax.net.ssl.trustStore=/home/quasar/ssl/certs.ts`):
- `javax.net.ssl.trustStore`: path specifying a file containing the certificate chain for verifying the server.
- `javax.net.ssl.trustStorePassword`: password for the trust store.
- `javax.net.ssl.keyStore`: path specifying a file containing the client's private key.
- `javax.net.ssl.keyStorePassword`: password for the key store.
- `javax.net.debug`: (optional) use `all` for very verbose but sometimes helpful output.
- `invalidHostNameAllowed`: (optional) use `true` to disable host name checking, which is less secure but may be needed in test environments using self-signed certificates.

##### Couchbase

To connect to Couchbase use the following `connectionUri` format:

`couchbase://<host>[:<port>]?username=<username>&password=<password>[&queryTimeoutSeconds=<seconds>]`

Prerequisites
- Couchbase Server 4.5.1 or greater
- A "default" bucket with anonymous access
- Documents must have a "type" field to be listed
- Primary index on queried buckets
- Secondary index on "type" field for queried buckets
- Additional indices and tuning as recommended by Couchbase for proper N1QL performance

Known Limitations
- Slow queries — query optimization hasn't been applied
- Join unimplemented — future support planned
- [Open issues](https://github.com/quasar-analytics/quasar/issues?q=is%3Aissue+is%3Aopen+label%3ACouchbase)

##### HDFS using Apache Spark

To connect to HDFS using Apache Spark use the following `connectionUri` format:

`spark://<spark_host>:<spark_port>|hdfs://<hdfs_host>:<hdfs_port>|<root_path>`

e.g "spark://spark_master:7077|hdfs://primary_node:9000|/hadoop/users/"

##### MarkLogic

To connect to MarkLogic, specify an [XCC URL](https://docs.marklogic.com/guide/xcc/concepts#id_55196) with a `format` query parameter and an optional root directory as the `connectionUri`:

`xcc://<username>:<password>@<host>:<port>/<database>[/root/dir/path]?format=[json|xml]`

the mount will query either JSON or XML documents based on the value of the `format` parameter. For backwards-compatibility, if the `format` parameter is omitted then XML is assumed.

If a root directory path is specified, all operations and queries within the mount will be local to the MarkLogic directory at the specified path.

Prerequisites
- MarkLogic 8.0+
- Documents must to be organized under directories to be found by Quasar.
- Namespaces used in queries must be defined on the server.
- Loading schema definitions into the server, while not required, will improve sorting and other operations on types other than `xs:string`. Otherwise, non-string fields may require casting in queries using [SQL² conversion functions](http://docs.slamdata.com/en/v4.0/sql-squared-reference.html#section-11-data-type-conversion).

[Known Limitations](https://github.com/quasar-analytics/quasar/issues?utf8=%E2%9C%93&q=is%3Aissue%20is%3Aopen%20label%3AMarkLogic)
- Field aliases when working with XML must currently be valid [XML QNames](https://www.w3.org/TR/xml-names/#NT-QName) ([#1642](https://github.com/quasar-analytics/quasar/issues/1642)).
- "Default" numeric field names are prefixed with an underscore ("_") when working with XML in order to make them valid QNames. For example, `select count((1, 2, 3, 4))` will result in `{"_1": 4}` ([#1642](https://github.com/quasar-analytics/quasar/issues/1642)).
- It is not possible to query both JSON and XML documents from a single mount, a separate mount with the appropriate `format` value must be created for each type of document.
- Index usage is currently poor, so performance may degrade on large directories and/or complex queries and joins. This should improve as optimizations are applied both to the MarkLogic connector and the `QScript` compiler.

Quasar's data model is JSON-ish and thus there is a bit of translation required when applying it to XML. The current mapping aims to be intuitive while still taking advantage of the XDM as much as possible. Take note of the following:
- Projecting a field will result in the child element(s) having the given name. If more than one element matches, the result will be an array.
- As the children of an element form a sequence, they may be treated both as a mapping from element names to values and as an array of values. That is to say, given a document like `<foo><bar>1</bar><baz>2</baz></foo>`, `foo.bar` and `foo[0]` both refer to `<bar>1</bar>`.
- XML document results are currently serialized to JSON with an emphasis on producting idiomatic JSON:
  - An element is serialized to a singleton object with the element name as the only key and an object representing the children as its value. The child object will contain an entry for each child element with repeated elements collected into an array.
  - An element without attributes containing only text content will be serialized as a singleton object with the element name as the only key and the text content as its value.
  - Element attributes are serialized to an object at the `_attributes` key.
  - Text content of elements containing mixed text and element children or attributes will be available at the `_text` key.

#### View mounts

If the mount's key is "view" then the mount represents a "virtual" file, defined by a SQL² query. When the file's contents are read or referred to, the query is executed to generate the current result on-demand. A view can be used to create dynamic data that combines analysis and formatting of existing files without creating temporary results that need to be manually regenerated when sources are updated.

For example, given the above MongoDB mount, an additional view could be defined in this way:

```json
  "mountings": {
    ...,
    "/simpleZips": {
      "view": {
        "connectionUri": "sql2:///?q=select%20_id%20as%20zip%2C%20city%2C%20state%20from%20%60%2Flocal%2Ftest%2Fzips%60%20where%20pop%20%3C%20%3Acutoff&var.cutoff=1000"
      }
    }
  }
```

A view can be mounted at any file path. If a view's path is nested inside the path of a database mount, it will appear alongside the other files in the database. A view will "shadow" any actual file that would otherwise be mapped to the same path. Any attempt to write data to a view will result in an error.

#### Build Quasar for Apache Spark

Because of dependencies conflicts between Mongo & Spark connectors, currently process of building Quasar for Spark requires few additional steps:

1. Build sparkcore.jar

```./sbt 'set every sparkDependencyProvided := true' sparkcore/assembly```

2. Set environment variable QUASAR_HOME

QUASAR_HOME must point to a folder holding `sparkcore.jar`

## REPL Usage

The interactive REPL accepts SQL `SELECT` queries.

First, choose the database to be used. Here, a MongoDB instance is mounted at
the root, and it contains a database called `test`:

```
💪 $ cd test
```

The "tables" in SQL queries refer to collections in the database by name:
```
💪 $ select * from zips where state="CO" limit 3
Mongo
db.zips.aggregate(
  [
    { "$match": { "state": "CO" } },
    { "$limit": NumberLong(3) },
    { "$out": "tmp.gen_0" }],
  { "allowDiskUse": true });
db.tmp.gen_0.find();


Query time: 0.1s
 city    | loc[0]       | loc[1]     | pop    | state |
---------|--------------|------------|--------|-------|
 ARVADA  |  -105.098402 |  39.794533 |  12065 | CO    |
 ARVADA  |  -105.065549 |  39.828572 |  32980 | CO    |
 ARVADA  |   -105.11771 |  39.814066 |  33260 | CO    |

💪 $ select city from zips limit 3
...
 city     |
----------|
 AGAWAM   |
 CUSHMAN  |
 BARRE    |
```

You may also store the result of a SQL query:

```sql
💪 $ out1 := select * from zips where state="CO" limit 3
```

The location of a collection may be specified as an absolute path by
surrounding the path with double quotes:

```sql
select * from `/test/zips`
```

Type `help` for information on other commands.


## API Usage

The server provides a simple JSON API.

### GET /query/fs/[path]?q=[query]&offset=[offset]&limit=[limit]&var.[foo]=[value]

Executes a SQL² query, contained in the required `q` parameter, on the backend responsible for the request path.

Optional `offset` and `limit` parameters can be specified to page through the results, and are interpreted the same way as for `GET /data` requests.

The result is returned in the response body. The `Accept` header may be used in order to specify the desired [format](#data-formats) in which the client wishes to receive results.

For compressed output use `Accept-Encoding: gzip`.


### POST /query/fs/[path]?var.[foo]=[value]

Executes a SQL² query, contained in the request body, on the backend responsible for the request path.

The `Destination` header must specify the *output path*, where the results of the query will become available if this API successfully completes. If the output path already exists, it will be overwritten with the query results.

All paths referenced in the query, as well as the output path, are interpreted as relative to the request path, unless they begin with `/`.

This API method returns the name where the results are stored, as an absolute path, as well as logging information.

```json
{
  "out": "/[path]/tmp231",
  "phases": [
    ...
  ]
}
```

If the query fails to compile, a 400 response is produced with a JSON body similar to the following:

```json
{
  "status": "Bad Request",
  "detail": {
    "errors" [
      <all errors produced during compilation, each an object with `status` and `detail` fields>
    ],
    "phases": [
      <see the following sections>
    ]
  }
}
```

If an error occurs while executing the query on a backend, a 500 response is produced, with this content:

```json
{
  "status": <general error description>,
  "detail": {
    "message": <specific error description>,
    "phases": [
      <see the following sections>
    ],
    "logicalPlan": <tree of objects describing the logical plan the query compiled to>,
    "cause": <optional, backend-specific error>
  }
}
```

the `cause` field is optional and the `detail` object may also contain additional, backend-specific fields.

The `phases` array contains a sequence of objects containing the result from
each phase of the query compilation process. A phase may result in a tree of
objects with `type`, `label` and (optional) `children`:

```json
{
  ...,
  "phases": [
    ...,
    {
      "name": "Logical Plan",
      "tree": {
        "type": "LogicalPlan/Let",
        "label": "'tmp0",
        "children": [
          {
            "type": "LogicalPlan/Read",
            "label": "./zips"
          },
          ...
        ]
      }
    },
    ...
  ]
}
```

Or a blob of text:

```json
{
  ...,
  "phases": [
    ...,
    {
      "name": "Mongo",
      "detail": "db.zips.aggregate([\n  { \"$sort\" : { \"pop\" : 1}}\n])\n"
    }
  ]
}
```

Or an error (typically no further phases appear, and the error repeats the
error at the root of the response):

```json
{
  ...,
  "phases": [
    ...,
    {
      "name": "Physical Plan",
      "error": "Cannot compile ..."
    }
  ]
}
```

### GET /compile/fs/[path]?q=[query]&var.[foo]=[value]

Compiles (but does not execute) a SQL² query, contained in the single, required query parameter.
Returns a Json object with the following shape:

```json
{
  "inputs": [<filePath>, ...],
  "physicalPlan": "Description of physical plan"
}

where `inputs` is a field containing a list of files that are referenced by the query.
where `physicalPlan` is a string description of the physical plan that would be executed by this query. `null` if no physical plan is required in order to execute this query. A query may not need a physical plan in order to be executed if the query is "constant", that is that no data needs to be read from a backend.

### GET /metadata/fs/[path]

Retrieves metadata about the files, directories, and mounts which are children of the specified directory path. If the path names a file, the result is empty.

```json
{
  "children": [
    {"name": "foo", "type": "directory"},
    {"name": "bar", "type": "file"},
    {"name": "test", "type": "directory", "mount": "mongodb"},
    {"name": "baz", "type": "file", "mount": "view"}
  ]
}
```

### GET /data/fs/[path]?offset=[offset]&limit=[limit]

Retrieves data from the specified path in the [format](#data-formats) specified in the `Accept` header. The optional `offset` and `limit` parameters can be used in order to page through results.

```json
{"id":0,"guid":"03929dcb-80f6-44f3-a64c-09fc1d810c61","isActive":true,"balance":"$3,244.51","picture":"http://placehold.it/32x32","age":38,"eyeColor":"green","latitude":87.709281,"longitude":-20.549375}
{"id":1,"guid":"09639710-7f99-4fe1-a890-b1b592cbe223","isActive":false,"balance":"$1,544.65","picture":"http://placehold.it/32x32","age":27,"eyeColor":"blue","latitude":52.394181,"longitude":-0.631589}
{"id":2,"guid":"e71b7f01-ce0e-4824-ad1e-4e118872aec4","isActive":true,"balance":"$1,882.92","picture":"http://placehold.it/32x32","age":24,"eyeColor":"green","latitude":30.061766,"longitude":-106.813523}
{"id":3,"guid":"79602676-6f63-41d0-9c0a-a4f5851a43db","isActive":false,"balance":"$1,281.00","picture":"http://placehold.it/32x32","age":25,"eyeColor":"blue","latitude":14.713939,"longitude":62.253264}
{"id":4,"guid":"0024a8ad-373f-459a-8316-d50d7a8f7b10","isActive":true,"balance":"$1,908.50","picture":"http://placehold.it/32x32","age":26,"eyeColor":"brown","latitude":-21.874648,"longitude":67.270659}
{"id":5,"guid":"f7e33b92-a885-450e-8ad5-92103b1f5ff3","isActive":true,"balance":"$2,231.90","picture":"http://placehold.it/32x32","age":31,"eyeColor":"blue","latitude":58.461107,"longitude":176.40584}
{"id":6,"guid":"a2863ec1-9652-46d3-aa12-aa92308de055","isActive":false,"balance":"$1,621.67","picture":"http://placehold.it/32x32","age":34,"eyeColor":"blue","latitude":-83.908456,"longitude":67.190633}
```

If the supplied path represents a directory (ends with a slash), this request produces a `zip` archive containing the contents of the named directory, database, etc. Each file in the archive is formatted as specified in the request query and/or `Accept` header.

### PUT /data/fs/[path]

Replace data at the specified path. Uploaded data may be in any of the [supported formats](#data-formats) and the request must include the appropriate `Content-Type` header indicating the format used.

A successful upload will replace any previous contents atomically, leaving them unchanged if an error occurs.

If an error occurs when reading data from the request body, the response will contain a summary in the common `error` field and a separate array of error messages about specific values under `details`.

Fails if the path identifies a view.

### POST /data/fs/[path]

Append data to the specified path. Uploaded data may be in any of the [supported formats](#data-formats) and the request must include the appropriate `Content-Type` header indicating the format used. This operation is _not_ atomic and some data may have been written even if an error occurs. The body of an error response will describe what was done.

If an error occurs when reading data from the request body, the response contains a summary in the common `error` field, and a separate array of error messages about specific values under `details`.

Fails if the path identifies a view.

### DELETE /data/fs/[path]

Removes all data and views at the specified path. Single files are deleted atomically.

### MOVE /data/fs/[path]

Moves data from one path to another within the same backend. The new path must
be provided in the `Destination` request header. Single files are moved atomically.

### GET /mount/fs/[path]

Retrieves the configuration for the mount point at the provided path. In the case of MongoDB, the response will look like

```
{ "mongodb": { "connectionUri": "mongodb://localhost/test" } }
```

The outer key is the backend in use, and the value is a backend-specific configuration structure.

### POST /mount/fs/[path]

Adds a new mount point using the JSON contained in the body. The path is the containing directory, and an `X-File-Name` header should contain the name of the mount. This will return a 409 Conflict if the mount point already exists or if a database mount already exists above or below a new database mount.

### PUT /mount/fs/[path]

Creates a new mount point or replaces an existing mount point using the JSON contained in the body. This will return a 409 Conflict if a database mount already exists above or below a new database mount.

### DELETE /mount/fs/[path]

Deletes an existing mount point, if any exists at the given path. If no such mount exists, the request succeeds but the response has no content. Mounts that are nested within the mount being deleted (i.e. views) are also deleted.

### MOVE /mount/fs/[path]

Moves a mount from one path to another. The new path must be provided in the `Destination` request header. This will return a 409 Conflict if a database mount is being moved above or below the path of an existing database mount. Mounts that are nested within the mount being moved (i.e. views) are moved along with it.

### PUT /server/port

Takes a port number in the body, and attempts to restart the server on that port, shutting down the current instance which is running on the port used to make this http request.

### DELETE /server/port

Removes any configured port, reverting to the default (20223) and restarting, as with `PUT`.


## Error Responses

Error responses from the REST api have the following form

```
{
  "error": {
    "status": <succinct message>,
    "detail": {
      "field1": <JSON>,
      "field2": <JSON>,
      ...
      "fieldN": <JSON>
    }
  }
}
```

The `status` field will always be present and will contain a succinct description of the error in english, the same content will be used as the status message of the HTTP response itself. The `detail` field is optional and, if present, will contain a JSON object with additional information about the error.

Examples of `detail` fields would be a backend-specific error message, detailed type information for type errors in queries, the actual invalid arguments presented to a function, etc. These fields are error-specific, however, if the error is going to include a more detailed error message, it will found under the `message` field in the `detail` object.


## Paths

Paths identify files and directories in Quasar's virtual file system. File and directory paths are distinct, so `/foo` and `/foo/` represent a file and a directory, respectively.

Depending on the backend, some restrictions may apply:
- it may be possible for a file and directory with the same name to exist side by side.
- it may _not_ be possible for an empty directory to exist. That is, deleting the only descendant file from a directory may cause the directory to disappear as well.
- there may be limits on the overall length of paths, and/or the length of particular path segments. Any request that exceeds these limits will result in an error.

_Any_ character can appear in a path, but when paths are embedded in character strings and byte-streams they are encoded in the following ways:

When a path appears in a request URI, or in a header such as `Destination` or `X-FileName`, it must be URL-encoded. Note: `/` characters that appear _within_ path segments are encoded.

When a path appears in a JSON string value, `/` characters that appear _within_ path segments are encoded as `$sep$`.

In both cases, the special names `.` and `..` are encoded as `$dot$` and $dotdot$`, but only if they appear as an _entire_ segment.

When only a single path segment is shown, as in the response body of a `/metadata` request, no special encoding is done (beyond the normal JSON encoding of `"` and non-ASCII characters).

For example, a file called `Plan 1/2 笑` in a directory `mydata` would appear in the following ways:
- in a URL: `http://<host>:<port>/data/fs/mydata/Plan%201%2F2%20%E7%AC%91`
- in a header: `Destination: /mydata/Plan%201%2F2%20%E7%AC%91`
- in the response body of `/metadata/fs/mydata/`: `{ "type": "file", "name": "Plan 1/2 \u7b11" }`
- in an error:
```json
{
  "error": {
    "status": "Path not found.",
    "detail": {
      "path": "/local/quasar-test/mydata/Plan 1$sep$2 \u7b11"
    }
  }
}
```

## Request Headers

Request headers may be supplied via a query parameter in case the client is unable to send arbitrary headers (e.g. browsers, in certain circumstances). The parameter name is `request-headers` and the value should be a JSON-formatted string containing an object whose fields are named for the corresponding header and whose values are strings or arrays of strings. If any header appears both in the `request-headers` query parameter and also as an ordinary header, the query parameter takes precedence.

For example:
```
GET http://localhost:8080/data/fs/local/test/foo?request-headers=%7B%22Accept%22%3A+%22text%2Fcsv%22%7D
```
Note: that's the URL-encoded form of `{"Accept": "text/csv"}`.

## Data Formats

Quasar produces and accepts data in two JSON-based formats or CSV (`text/csv`). Each JSON-based format can
represent all the types of data that Quasar supports. The two formats are appropriate for
different purposes.

Json can either be line delimited (`application/ldjson`/`application/x-ldjson`) or a single json value (`application/json`).

In the case of an HTTP request, it is possible to add the `disposition` extension to any media-type specified in an `Accept` header in order to receive a response with that value in the `Content-Disposition` header field.

Choosing between the two json formats is done using the "mode" content-type extension and by supplying either the "precise" or "readable" values. If no `mode` is supplied, `quasar` will default to the `readable` mode. If neither json nor csv is supplied, quasar will default to returning the results in `json` format. In the case of an upload request, the client MUST supply a media-type and requests without any media-type will result in an HTTP 415 error response.

### Precise JSON

This format is unambiguous, allowing every value of every type to be specified. It's useful for
entering data, and for extracting data to be read by software (as opposed to people.) Contains
extra information that can make it harder to read.


### Readable JSON

This format is easy to read and use with other tools, and contains minimal extra information.
It does not always convey the precise type of the source data, and does not allow all values
to be specified. For example, it's not possible to tell the difference between the string
`"12:34:56"` and the time value equal to 34 minutes and 56 seconds after noon.


### Examples

Type      | Readable        | Precise  | Notes
----------|-----------------|----------|------
null      | `null`          | *same*   |
boolean   | `true`, `false` | *same*   |
string    | `"abc"`         | *same*   |
int       | `1`             | *same*   |
decimal   | `2.1`           | *same*   |
object    | `{ "a": 1 }`    | *same*   |
object    | `{ "$foo": 2 }` | `{ "$obj": { "$foo": 2 } }` | Requires a type-specifier if any key starts with `$`.
array     | `[1, 2, 3]`     | *same*   |
set       | `[1, 2, 3]`     | `{ "$set": [1, 2, 3] }` |
timestamp | `"2015-01-31T10:30:00Z"` | `{ "$timestamp": "2015-01-31T10:30:00Z" }` |
date      | `"2015-01-31"`  | `{ "$date": "2015-01-31" }` |
time      | `"10:30:05"`    | `{ "$time": "10:30:05" }` | HH:MM[:SS[:.SSS]]
interval  | `"PT12H34M"`    | `{ "$interval": "P7DT12H34M" }` | Note: year/month not currently supported.
binary    | `"TE1OTw=="`    | `{ "$binary": "TE1OTw==" }` | BASE64-encoded.
object id | `"abc"`         | `{ "$oid": "abc" }` |


### CSV

When Quasar produces CSV, all fields and array elements are "flattened" so that each column in the output contains the data for a single location in the source document. For example, the document `{ "foo": { "bar": 1, "baz": 2 } }` becomes

```
foo.bar,foo.baz
1,2
```

Data is formatted the same way as the "Readable" JSON format, except that all values including `null`, `true`, `false`, and numbers are indistinguishable from their string representations.

It is possible to use the `columnDelimiter`, `rowDelimiter` `quoteChar` and `escapeChar` media-type extensions keys in order to customize the layout of the csv.

When data is uploaded in CSV format, the headers are interpreted as field names in the same way. As with the Readable JSON format, any string that can be interpreted as another kind of value will be, so for example there's no way to specify the string `"null"`.


## Troubleshooting

First, make sure that the `quasar-analytics/quasar` Github repo is building correctly (the status is displayed at the top of the README).

Then, you can try the following command:

```bash
./sbt test
```

This will ensure that your local version is also passing the tests.

Check to see if the problem you are having is mentioned in the [JIRA issues](https://slamdata.atlassian.net/) and, if it isn't, feel free to create a new issue.

You can also discuss issues on Gitter: [quasar-analytics/quasar](https://gitter.im/quasar-analytics/quasar).

## Legal

Copyright &copy; 2014 - 2016 SlamData Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
