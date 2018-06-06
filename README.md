[![Travis](https://travis-ci.org/slamdata/quasar.svg?branch=master)](https://travis-ci.org/slamdata/quasar)
[![AppVeyor](https://ci.appveyor.com/api/projects/status/pr5he90wye6ii8ml/branch/master?svg=true)](https://ci.appveyor.com/project/jdegoes/quasar/branch/master)
[![Discord](https://img.shields.io/discord/373302030460125185.svg?logo=discord)](https://discord.gg/QNjwCg6)

# Quasar

Quasar is an open source NoSQL analytics engine that can be used as a library or through a REST API to power advanced analytics across a growing range of data sources and databases, including MongoDB.

## SQL²

SQL² is the dialect of SQL that Quasar understands.

In the following documentation SQL² will be used interchangeably with SQL.

SQL² supports variables inside queries (`SELECT * WHERE pop < :cutoff`). Values for these variables, which can be any expression, should be specified as additional parameters in the url, using the variable name prefixed by `var.` (e.g. `var.cutoff=1000`). Failure to specify valid values for all variables used inside a query will result in an error. These values use the same syntax as the query itself; notably, strings should be surrounded by double quotes. Some acceptable values are `123`, `"CO"`, and `DATE("2015-07-06")`.

## Building from Source

**Note**: This requires Java 8 and Bash (Linux, Mac).  Bash is not required on Windows, but the non-SBT infrastructure (e.g. the docker scripts) currently only works on Unix platforms.

### Build

The following sections explain how to build and run the various subprojects.

#### Basic Compile & Test

To compile the project and run tests, first clone the quasar repo and then execute the following command (if on Windows, reverse the slashes):

```bash
./sbt test
```

Note: please note that we are not using here a system wide sbt, but our own copy of it (under ./sbt). This is primarily done for determinism. In order to have a reproducible build, the helper script needs to be part of the repo.

Running the full test suite can be done using docker containers for various backends:

##### Full Testing (prerequisite: docker and docker-compose)

In order to run integration tests for various backends the `docker/scripts` are provided to easily create dockerized backend data stores.

Of particular interest are the following two scripts:

  1. `docker/scripts/setupContainers`
  2. `docker/scripts/assembleTestingConf`


Quasar supports the following datastores:

```
quasar_mongodb_read_only
quasar_mongodb_3_2
quasar_mongodb_3_4
quasar_metastore
quasar_marklogic_xml
quasar_marklogic_json
quasar_couchbase
```

Knowing which backend datastores are supported you can create and configure docker containers using `setupContainers`. For example
if you wanted to run integration tests with mongo, marklogic, and couchbase you would use:

```
./setupContainers -u quasar_metastore,quasar_mongodb_3_4,quasar_marklogic_xml,quasar_couchbase
```

Note: `quasar_metastore` is always needed to run integration tests.

This command will pull docker images, create containers running the specified backends, and configure them appropriately for Quasar testing.

Once backends are ready we need to configure the integrations tests in order to inform Quasar about where to find the backends to test.
This information is conveyed to Quasar using the file `it/testing.conf`. Using the `assembleTestingConf` script you can generate a `testing.conf`
file based on the currently running containerizd backends using the following command:

```
./assembleTestingConf -a
```

After running this command your `testing.conf` file should look similar to this:

```
> cat it/testing.conf
postgresql_metastore="{\"host\":\"192.168.99.101\",\"port\":5432,\"database\":\"metastore\",\"userName\":\"postgres\",\"password\":\"\"}"
couchbase="couchbase://192.168.99.101/beer-sample?password=&docTypeKey=type&socketConnectTimeoutSeconds=15"
marklogic_xml="xcc://marklogic:marklogic@192.168.99.101:8000/Documents?format=xml"
mongodb_3_4="mongodb://192.168.99.101:27022"
```

IP's will vary depending on your docker environment. In addition the scripts assume you have docker and docker-compose installed.
You can find information about installing docker [here](https://www.docker.com/products/docker-toolbox).


#### REPL JAR

To build a JAR for the REPL, which allows entering commands at a command-line prompt, execute the following command:

```bash
./sbt 'repl/assembly'
```

The path of the JAR will be `./.targets/repl/scala-2.11/quasar-repl-assembly-[version].jar`, where `[version]` is the Quasar version number.

To run the JAR, execute the following command:

```bash
java -jar [<path to jar>] [-c <config file>]
```

As a command-line REPL user, to work with a fully functioning REPL you will need the metadata store and a mount point. See [here](#full-testing-prerequisite-docker-and-docker-compose) for instructions on creating the metadata store backend using docker.

Once you have a running metastore you can start the web api service with [these](#web-jar) instructions and issue curl commands
of the following format to create new mount points.

```bash
curl -v -X PUT http://localhost:8080/mount/fs/<mountPath>/ -d '{ "<mountKey>": { "connectionUri":"<protocol><uri>" } }'
```
The `<mountPath>` specifies the path of your mount point and the remaining parameters are listed below:

| mountKey            | protocol         | uri                                   |
|---------------------|------------------|---------------------------------------|
| `couchbase`         | `couchbase://`   | [Couchbase](#couchbase)               |
| `marklogic`         | `xcc://`         | [MarkLogic](#marklogic)               |
| `mimir`             |                  | "\<path-to-mimir-storage-directory\>" |
| `local_file_system` |                  | "\<path-to-mimir-storage-directory\>" |
| `mongodb`           | `mongodb://`     | [MongoDB](#database-mounts)           |


See [here](#get-mountfspath) for more details on the mount web api service.

For example, to create a couchbase mount point, issue a `curl` command like:

```bash
curl -v -X PUT http://localhost:8080/mount/fs/cb/ -d '{ "couchbase": { "connectionUri":"couchbase://192.168.99.100/beer-sample?password=&docTypeKey=type" } }'
```

#### Web JAR

To build a JAR containing a lightweight HTTP server that allows you to programmatically interact with Quasar, execute the following command:

```bash
./sbt 'web/assembly'
```

The path of the JAR will be `./.targets/web/scala-2.11/quasar-web-assembly-[version].jar`, where `[version]` is the Quasar version number.

To run the JAR, execute the following command:

```bash
java -jar [<path to jar>] [-c <config file>]
```

Web jar users, will also need the metadata store. See [here](#full-testing-prerequisite-docker-and-docker-compose) for getting up and running with one using docker.

### Backends

By default, neither the REPL nor the web assemblies contain any backends *other* than mimir.  Thus, if you invoke them as shown above, the only mount type that will be understood will be `mimir`.  In order to use other mounts – such as mongodb – you will need to build the relevant backend and place the JAR in a directory where quasar can find it.  This can be done in one of two ways

#### Plugins Directory

Create a directory where you will place individual backend JARs:

```bash
$ mkdir plugins/
```

Now run the `assembly` task for the relevant backend:

```bash
$ ./sbt mongodb/assembly
```

The path to the JAR will be something like `./.targets/mongodb/scala-2.11/quasar-mongodb-internal-assembly-23.1.5.jar`, though the exact name of the JAR (and the directory path in question) will of course depend on the backend built (for example, `marklogic/assembly` will produce a very different JAR from `mongodb/assembly`).

For each backend that you wish to support, run that backend's `assembly` and copy the JAR file into your new `plugins/` directory.  Once this is done, you can launch the web assembly using the following sort of command:

```bash
java -jar [<path to quasar jar>] [-c <config file>] -P plugins/
```

All of the JARs within the `plugins/` directory will be loaded as a backend provider, and their relevant mount type will be made available by quasar. Be sure that no two different versions of the same connector are found within this directory.

This technique (a directory containing multiple plugin JARs) only works with the web assembly.  If you wish to use the REPL, you will need to use the second method (which works with both).

#### Individual Backend Configuration

This technique is designed for local development use, where the backend implementation is changing frequently.  Under certain circumstances though, it may be useful for the pre-built JAR case.

As with the plugins directory approach, you will need to run the `assembly` task for each backend that you want to use.  But instead of copying the JAR files into a directory, you will be referencing each JAR file individually using the `--backend` switch on the web or REPL JAR invocation:

```bash
java -jar [<path to jar>] [-c <config file>] --backend:quasar.physical.mongodb.MongoDb\$=.targets/mongodb/scala-2.11/quasar-mongodb-internal-assembly-23.1.5.jar
```

Replace the JAR file in the above with the path to the backend whose `assembly` you ran.  The `--backend` switch may be repeated as many times as necessary: once for each backend you wish to add.  The value to the left of the `=` is the `BackendModule` object *class name* which defines the backend in question.  Note that we need to escape the `$` character which will be present in each class name, solely because of bash syntax. If you are invoking the `--backend` option within `sbt` (for example running `web/run` or `repl/run`) you do not need to escape the `$`.

What follows is a list of class names for each supported backend:

| mountKey          | class name                                               |
|-------------------|----------------------------------------------------------|
| `couchbase`       | `quasar.physical.couchbase.Couchbase$`                   |
| `marklogic`       | `quasar.physical.marklogic.MarkLogic$`                   |
| `mongodb`         | `quasar.physical.mongodb.MongoDb$`                       |

Mimir is not included in the above, since it is already built into the core of quasar.

The value to the *right* of the `=` is a *comma*-separated list of paths which will be used as the classpath for the backend in question.  You can include as many JARs or directories (containing classes) as you need, just as with any classpath configuration.

### Configure

The various REPL JARs can be configured by using a command-line argument to indicate the location of a JSON configuration file. If no config file is specified, it is assumed to be `quasar-config.json`, from a standard location in the user's home directory.

The JSON configuration file must have the following format:

```json
{
  "server": {
    "port": 8080
  },
  "metastore": {
    "database": {
      <metastore_config>
    }
  }
}
```

#### Metadata Store

Configuration for the metadata store consists of providing connection information for a supported database. Currently the [H2](http://www.h2database.com/) and [PostgreSQL](https://www.postgresql.org/) (9.5+) databases are supported.

To easily get up and running with a PostgreSQL metastore backend using docker see [Full Testing](#Full) section.

If no metastore configuration is specified, the default configuration will use an H2 database located in the default quasar configuration directory for your operating system.

An example H2 configuration would look something like
```json
"h2": {
  "location": "`database_url`"
}
```

Where `database_url` can be any h2 url as described [here](http://www.h2database.com/html/features.html#database_url).

A PostgreSQL configuration looks something like
```json
"postgresql": {
  "host": "localhost",
  "port": 8087,
  "database": "<database name>",
  "userName": "<database user>",
  "password": "<password for database user>",
  "parameters": <an optional JSON object of parameter key:value pairs>
}
```

The contents of the optional `parameters` object correspond to the various driver configuration parameters available for PostgreSQL. One example for a value of the `parameters` object may be a `loglevel`:

```json
"parameters": {
  "loglevel": 1
}
```

#### Initializing and updating Schema

Before the server can be started, the metadata store schema must be initialized. To do so utilize the "initUpdateMetaStore" command with a web or repl quasar jar.

If mounts are already defined in the config file, initialization will migrate those to the metadata store.

### Database mounts

If the mount's key is "mongodb", then the `connectionUri` is a standard [MongoDB connection string](http://docs.mongodb.org/manual/reference/connection-string/). Only the primary host is required to be present, however in most cases a database name should be specified as well. Additional hosts and options may be included as specified in the linked documentation.

For example, say a MongoDB instance is running on the default port on the same machine as Quasar, and contains databases `test` and `students`, the `students` database contains a collection `cs101`, and the `connectionUri` is `mongodb://localhost/test`. Then the filesystem will contain the paths `/local/test/` and `/local/students/cs101`, among others.

A database can be mounted at any directory path, but database mount paths must not be nested inside each other.

#### MongoDB

To connect to MongoDB using TLS/SSL, specify `?ssl=true` in the connection string, and also provide the following via system properties when launching either JAR (i.e. `java -Djavax.net.ssl.trustStore=/home/quasar/ssl/certs.ts`):
- `javax.net.ssl.trustStore`: path specifying a file containing the certificate chain for verifying the server.
- `javax.net.ssl.trustStorePassword`: password for the trust store.
- `javax.net.ssl.keyStore`: path specifying a file containing the client's private key.
- `javax.net.ssl.keyStorePassword`: password for the key store.
- `javax.net.debug`: (optional) use `all` for very verbose but sometimes helpful output.
- `invalidHostNameAllowed`: (optional) use `true` to disable host name checking, which is less secure but may be needed in test environments using self-signed certificates.

#### Couchbase

To connect to Couchbase use the following `connectionUri` format:

`couchbase://<host>[:<port>]/<bucket-name>?password=<password>&docTypeKey=<type>[&queryTimeoutSeconds=<seconds>]`

Prerequisites
- Couchbase Server 4.5.1 or greater
- A "default" bucket with anonymous access
- Documents must have a `docTypeKey` field to be listed
- Primary index on queried buckets
- Secondary index on `docTypeKey` field for queried buckets
- Additional indices and tuning as recommended by Couchbase for proper N1QL performance

Known Limitations
- Slow queries — query optimization hasn't been applied
- Join unimplemented — future support planned

#### MarkLogic

To connect to MarkLogic, specify an [XCC URL](https://docs.marklogic.com/guide/xcc/concepts#id_55196) with a `format` query parameter and an optional root directory as the `connectionUri`:

`xcc://<username>:<password>@<host>:<port>/<database>[/root/dir/path]?format=[json|xml]`

the mount will query either JSON or XML documents based on the value of the `format` parameter. For backwards-compatibility, if the `format` parameter is omitted then XML is assumed.

If a root directory path is specified, all operations and queries within the mount will be local to the MarkLogic directory at the specified path.

Prerequisites
- MarkLogic 8.0+
- The URI lexicon must be enabled.
- Namespaces used in queries must be defined on the server.
- Loading schema definitions into the server, while not required, will improve sorting and other operations on types other than `xs:string`. Otherwise, non-string fields may require casting in queries using [SQL² conversion functions](http://docs.slamdata.com/en/v4.0/sql-squared-reference.html#section-11-data-type-conversion).

- It is not possible to query both JSON and XML documents from a single mount, a separate mount with the appropriate `format` value must be created for each type of document.
- Index usage is currently poor, so performance may degrade on large directories and/or complex queries and joins. This should improve as optimizations are applied both to the MarkLogic connector and the `QScript` compiler.

Quasar's data model is JSON-ish and thus there is a bit of translation required when applying it to XML. The current mapping aims to be intuitive while still taking advantage of the XDM as much as possible. Take note of the following:
- Projecting a field will result in the child element(s) having the given name. If more than one element matches, the result will be an array.
- As the children of an element form a sequence, they may be treated both as a mapping from element names to values and as an array of values. That is to say, given a document like `<foo><bar>1</bar><baz>2</baz></foo>`, `foo.bar` and `foo[0]` both refer to `<bar>1</bar>`.
- XML document results are currently serialized to JSON with an emphasis on producting idiomatic JSON:
  - An element is serialized to a singleton object with the element name as the only key and an object representing the children as its value. The child object will contain an entry for each child element with repeated elements collected into an array.
  - An element without attributes containing only text content will be serialized as a singleton object with the element name as the only key and the text content as its value.
  - Element attributes are serialized to an object at the `_xml.attributes` key.
  - Text content of elements containing mixed text and element children or attributes will be available at the `_xml.text` key.
- Fields that are not valid [XML QNames](https://www.w3.org/TR/xml-names/#NT-QName) are encoded as `<ejson:key>` elements with a `ejson:key-id` attribute including the field's original name. For instance, the query `SELECT TO_STRING(city), TO_STRING(state) FROM zips` yields elements with numeric field names. Numeric names are not valid QNames and will be encoded as follows:

  ```xml
  <ejson:key ejson:key-id="0" ejson:type="string">GILMAN CITY</ejson:key>
  <ejson:key ejson:key-id="1" ejson:type="string">MO</ejson:key>
  ```


### View mounts

If the mount's key is "view" then the mount represents a "virtual" file, defined by a SQL² query. When the file's contents are read or referred to, the query is executed to generate the current result on-demand. A view can be used to create dynamic data that combines analysis and formatting of existing files without creating temporary results that need to be manually regenerated when sources are updated.

For example, given the above MongoDB mount, an additional view could be defined with a `connectionUri` of `sql2:///?q=select%20_id%20as%20zip%2C%20city%2C%20state%20from%20%60%2Flocal%2Ftest%2Fzips%60%20where%20pop%20%3C%20%3Acutoff&var.cutoff=1000`

A view can be mounted at any file path. If a view's path is nested inside the path of a database mount, it will appear alongside the other files in the database. A view will "shadow" any actual file that would otherwise be mapped to the same path. Any attempt to write data to a view will result in an error.

#### Caching

View mounts can optionally be cached. When cached a view is refreshed periodically in the background with respect to its associated `max-age`.

A cached view is created by adding the `Cache-Control: max-age=<seconds>`  header to a `/mount/fs/` request.

Like ordinary views, cached views appear as a file in the filesystem.

### Module mounts

If the mount's key is "module" then the mount represents a "virtual" directory which contains a collection of SQL Statements. The Quasar Filesystem surfaces each SQL function definition as a file despite the fact that it is not possible to read from that file. Instead one needs to use the `invoke` endpoint in order to pass arguments to a particular function and get the result.

A module function can be thought of as a parameterized view, i.e. a view with "holes" that can be filled dynamically.

The value of a module mount is simply the SQL string which will be parsed into a list of SQL Statements.

To create a new module one would send a json blob similar to this one to the mount endpoint:

```json
{ "module": "CREATE FUNCTION ARRAY_LENGTH(:foo) BEGIN COUNT(:foo[_]) END; CREATE FUNCTION USER_DATA(:user_id) BEGIN SELECT * FROM `/root/path/data/` WHERE user_id = :user_id END" }
```

Similar to views, modules can be mounted at any directory path. If a module's path is nested inside the path of a database mount, it will appear alongside the other directory and files in the database. A module will "shadow" any actual directory that would otherwise be mapped to the same path. Any attempt to write data to a module will result in an error.

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
```

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

If the supplied path does not exist, a 404 `NotFound` response is returned.

### PUT /data/fs/[path]

Replace data at the specified path. Uploaded data may be in any of the [supported formats](#data-formats) and the request must include the appropriate `Content-Type` header indicating the format used.

A successful upload will replace any previous contents atomically, leaving them unchanged if an error occurs.

If an error occurs when reading data from the request body, the response will contain a summary in the common `error` field and a separate array of error messages about specific values under `details`.

Fails if the path identifies an existing view.

#### Uploading multpile files

If the supplied path represents a directory (ends with a slash), the request body must contain a `zip` archive containing the contents of the named directory, database, etc., and a special file, `/.quasar-metadata.json`, which specifies the format for each file, as it would be provided in a `Content-Type` header if the file was individually uploaded:

```json
{
  "/foo": {
    "Content-Type": "application/ldjson"
  },
  "/foo/bar": {
    "Content-Type": "application/json; mode=precise"
  }
}
```

Note: if the zip archive was created by downloading a directory from Quasar, then it will already have this hidden file.

Each file in the archive is written as if it was uploaded separately. The write is _not_ atomic; if an error occurs after some files are written, the file system is not restored to its previous state.

### POST /data/fs/[path]

Append data to the specified path. Uploaded data may be in any of the [supported formats](#data-formats) and the request must include the appropriate `Content-Type` header indicating the format used. This operation is _not_ atomic and some data may have been written even if an error occurs. The body of an error response will describe what was done.

If an error occurs when reading data from the request body, the response contains a summary in the common `error` field, and a separate array of error messages about specific values under `details`.

Fails if the path identifies an existing view.

### DELETE /data/fs/[path]

Removes all data and views at the specified path. Single files are deleted atomically.

### MOVE /data/fs/[path]

Moves data from one path to another. Currently, both the source and destination must be within the same backend. The new path is provided in the `Destination` request header. Single files are moved atomically.

A 400 BadRequest is returned if the destination header is missing or if the source and destination are the same.
A 409 Conflict is returned if a file or directory already exists at the specified destination
A 201 Created is returned if the operation completed successfully

### COPY /data/fs/[path]

Copy a file or directory in the filesystem. Currently, both the source and destination must be within the same backend. The destination path is provided in the `Destination` request header.

A 400 BadRequest is returned if the destination header is missing or if the source and destination are the same.
A 409 Conflict is returned if a file or directory already exists at the specified destination
A 201 Created is returned if the operation completed successfully

### GET /invoke/fs/[path]

Where `path` is a file path. Invokes the function represented by the file path with the parameters supplied in the query string.

### GET /schema/fs/[path]?q=[query]&var.[foo]=[value]&arrayMaxLength=[size]&mapMaxSize=[size]&stringMaxLength=[size]&unionMaxSize=[size]

Where `path` is a directory path, `query` is a SQL² query and `size` is a positive integer. Returns a schema document, summarizing the results of the query. Free variables in the query may be bound using parameters like `var.foo=value` where `foo` is the variable to be bound and `value` is what it should be bound to.

For example, given query results like:

```json
{"_id":"01001","city":"AGAWAM","loc":[-72.622739,42.070206],"pop":15338,"state":"MA"}
{"_id":"01002","city":"CUSHMAN","loc":[-72.51565,42.377017],"pop":36963,"state":"MA"}
{"_id":"01005","city":"BARRE","loc":[-72.108354,42.409698],"pop":4546,"state":"MA"}
{"_id":"01007","city":"BELCHERTOWN","loc":[-72.410953,42.275103],"pop":10579,"state":"MA"}
{"_id":"01008","city":"BLANDFORD","loc":[-72.936114,42.182949],"pop":1240,"state":"MA"}
{"_id":"01010","city":"BRIMFIELD","loc":[-72.188455,42.116543],"pop":3706,"state":"MA"}
{"_id":"01011","city":"CHESTER","loc":[-72.988761,42.279421],"pop":1688,"state":"MA"}
{"_id":"01012","city":"CHESTERFIELD","loc":[-72.833309,42.38167],"pop":177,"state":"MA"}
{"_id":"01013","city":"CHICOPEE","loc":[-72.607962,42.162046],"pop":23396,"state":"MA"}
{"_id":"01020","city":"CHICOPEE","loc":[-72.576142,42.176443],"pop":31495,"state":"MA"}
```

a schema document might look like

```json
{
  "measure" : {
    "kind" : "collection",
    "count" : 1000.0,
    "minLength" : 5.0,
    "maxLength" : 5.0
  },
  "structure" : {
    "type" : "map",
    "of" : {
      "city" : {
        "measure" : {
          "kind" : "collection",
          "count" : 1000.0,
          "minLength" : 3.0,
          "maxLength" : 16.0
        },
        "structure" : {
          "tag" : "_structural.string",
          "type" : "array",
          "of" : {
            "measure" : {
              "count" : 8693.0,
              "distribution" : {
                "state" : {
                  "centralMoment4" : 893992600.3364398,
                  "size" : 8693.0,
                  "centralMoment3" : -18773123.74002289,
                  "centralMoment2" : 876954.1582882765,
                  "centralMoment1" : 74.29506499482345
                },
                "variance" : 100.89210288636407,
                "kurtosis" : 10.111128909991152,
                "mean" : 74.29506499482345,
                "skewness" : -2.1317240928957726
              },
              "min" : " ",
              "max" : "Z",
              "kind" : "char"
            },
            "structure" : {
              "type" : "character"
            }
          }
        }
      },
      "state" : {
        "measure" : {
          "kind" : "collection",
          "count" : 1000.0,
          "minLength" : 2.0,
          "maxLength" : 2.0
        },
        "structure" : {
          "tag" : "_structural.string",
          "type" : "array",
          "of" : {
            "measure" : {
              "count" : 2000.0,
              "distribution" : {
                "state" : {
                  "centralMoment4" : 11285757.38865178,
                  "size" : 2000.0,
                  "centralMoment3" : 11979.395483999382,
                  "centralMoment2" : 103139.03800000004,
                  "centralMoment1" : 76.19100000000014
                },
                "variance" : 51.59531665832919,
                "kurtosis" : 2.1271635715970967,
                "mean" : 76.19100000000014,
                "skewness" : 0.01618606190840656
              },
              "min" : "A",
              "max" : "Z",
              "kind" : "char"
            },
            "structure" : {
              "type" : "character"
            }
          }
        }
      },
      "pop" : {
        "measure" : {
          "count" : 1000.0,
          "distribution" : {
            "state" : {
              "centralMoment4" : 2.323080620322664E+20,
              "size" : 1000.0,
              "centralMoment3" : 4322812032420233.5,
              "centralMoment2" : 150795281801.8999,
              "centralMoment1" : 8721.71000000001
            },
            "variance" : 150946228.02992985,
            "kurtosis" : 10.267451061747597,
            "mean" : 8721.71000000001,
            "skewness" : 2.337959182172043
          },
          "min" : 0,
          "max" : 94317,
          "kind" : "decimal"
        },
        "structure" : {
          "type" : "decimal"
        }
      },
      "_id" : {
        "measure" : {
          "kind" : "collection",
          "count" : 1000.0,
          "minLength" : 5.0,
          "maxLength" : 5.0
        },
        "structure" : {
          "tag" : "_structural.string",
          "type" : "array",
          "of" : {
            "measure" : {
              "count" : 5000.0,
              "distribution" : {
                "state" : {
                  "centralMoment4" : 556673.1571508175,
                  "size" : 5000.0,
                  "centralMoment3" : 7962.505025040006,
                  "centralMoment2" : 38822.78220000003,
                  "centralMoment1" : 52.24340000000003
                },
                "variance" : 7.766109661932393,
                "kurtosis" : 1.8485506875431554,
                "mean" : 52.24340000000003,
                "skewness" : 0.07362665279751003
              },
              "min" : "0",
              "max" : "9",
              "kind" : "char"
            },
            "structure" : {
              "type" : "character"
            }
          }
        }
      },
      "loc" : {
        "measure" : {
          "kind" : "collection",
          "count" : 1000.0,
          "minLength" : 2.0,
          "maxLength" : 2.0
        },
        "structure" : {
          "type" : "array",
          "of" : [
            {
              "measure" : {
                "count" : 1000.0,
                "distribution" : {
                  "state" : {
                    "centralMoment4" : 281712013.3937695,
                    "size" : 1000.0,
                    "centralMoment3" : -4328243.124174622,
                    "centralMoment2" : 212160.04270665144,
                    "centralMoment1" : -90.52571468599996
                  },
                  "variance" : 212.3724151217732,
                  "kurtosis" : 6.290020275246902,
                  "mean" : -90.52571468599996,
                  "skewness" : -1.4027118290018674
                },
                "min" : -170.293408,
                "max" : -67.396382,
                "kind" : "decimal"
              },
              "structure" : {
                "type" : "decimal"
              }
            },
            {
              "measure" : {
                "count" : 1000.0,
                "distribution" : {
                  "state" : {
                    "centralMoment4" : 3748702.702983835,
                    "size" : 1000.0,
                    "centralMoment3" : 15799.696678343358,
                    "centralMoment2" : 26853.295673558245,
                    "centralMoment1" : 39.09175202499995
                  },
                  "variance" : 26.880175849407653,
                  "kurtosis" : 5.224679782347093,
                  "mean" : 39.09175202499995,
                  "skewness" : 0.1137115453464455
                },
                "min" : 20.907097,
                "max" : 65.824542,
                "kind" : "decimal"
              },
              "structure" : {
                "type" : "decimal"
              }
            }
          ]
        }
      }
    }
  }
}
```

Schema documents represent an estimate of the structure of the given dataset and are generated from a random sample of the data. Each node of the resulting structure is annotated with the frequency the node was observed and the bounds of the observed values, when available (NB: bounds should be seen as a reference and not taken as the true, global maximum or minimum values). Additionally, for numeric values, statistical distribution information is included.

When two documents differ in structure, their differences are accumulated in a union. Basic frequency information is available for the union and more specific annotations are preserved as much as possible for the various members.

The `arrayMaxLength`, `mapMaxSize`, `stringMaxLength` and `unionMaxSize` parameters allow for control over the amount of information contained in the returned schema by limiting the size of various structures in the result. Structures that exceed the various size thresholds are compressed using various heuristics depending on the structure involved.

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

### GET /server/info

Returns information about this server. Name and app version.

Example response:

```json
{"name":"Quasar","version":"19.1.2"}
```

### PUT /server/port

Takes a port number in the body, and attempts to restart the server on that port, shutting down the current instance which is running on the port used to make this http request. If this request succeeds, the client will not receive a response as the server is killed by the request. However, any subsequent request to the new port should succeed.

### DELETE /server/port

Removes any configured port, reverting to the default (the one the server was started on) and restarting. As with `PUT`, if this request succeeds, the client will not receive a response as the server is killed by the request. However, any subsequent request to the new port should succeed.

### GET /metastore

Retrieve the connection information of the current metastore in use. In the case where the current metastore is a postgres database, the password will be obscured for security reasons.

A few example responses:

```json
{
    "h2": {
        "location": "mem"
    }
}

```

```json
{
    "postgresql": {
      "host": "localhost",
      "port": 8087,
      "database": "slamdata_db",
      "userName": "slamdata",
      "password": "****"
    }
}
```

### PUT /metastore

Attempts to change the metastore using the supplied connection information

An optional `initialize` query parameter can be supplied so that Quasar automatically initializes the new
metastore after a successful connection if it has not already been initialized. If this parameter is omitted
and the new metastore has not been initialized, the request will fail with a message to the effect that the
new metastore has not been initialized.

An optional `copy` query parameter can be supplied to instruct Quasar to copy the current
metastore to a new empty metastore.

An example request body:

```json
{
    "postgresql": {
        "host": "localhost",
        "port": 9876,
        "database": "meta",
        "userName": "bob",
        "password": "123456"
    }
}
```

Returns `200 OK` if the change was performed successfully otherwise returns a `400` with a message body explaining what went wrong.

### GET /timings

Dumps timing information collected from a few of the last queries; all units are milliseconds

An example of the data returned:
```json
{
    "start": 0,
    "size": 4500,
    "children": {
      "parse SQL": {
        "start": 0,
        "size": 2,
        "children": {}
      },
      "resolve imports": {
        "start": 2,
        "size": 1,
        "children": {}
      },
      "plan": {
        "start": 3,
        "size": 4,
        "children": {}
      },
      "evaluate": {
        "start": 7,
        "size": 500,
        "children": {}
      }
    }
}
```

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
localdatetime  | `"2015-01-31T10:30:00"`    | `{ "$localdatetime": "2015-01-31T10:30" }` |
localdate      | `"2015-01-31"`    | `{ "$localdate": "2015-01-31" }` |
localtime      | `"10:30:00.000"`    | `{ "$localtime": "10:30" }` |
offsetdatetime | `"2015-01-31T10:30:00Z"`    | `{ "$offsetdatetime": "2015-01-31T10:30Z" }` |
offsetdate | `"2015-01-31Z"`    | `{ "$offsetdate": "2015-01-31Z" }` |
offsettime | `"10:30:00.000Z"`    | `{ "$offsettime": "10:30Z" }` |
interval  | `"PT12H34M"`    | `{ "$interval": "P7DT12H34M" }` | 
binary    | `"TE1OTw=="`    | `{ "$binary": "TE1OTw==" }` | BASE64-encoded.
object id | `"abc"`         | `{ "$oid": "abc" }` |


### CSV

When Quasar produces CSV, all fields and array elements are "flattened" so that each column in the output contains the data for a single location in the source document. For example, the document `{ "foo": { "bar": 1, "baz": 2 } }` becomes

```
foo.bar,foo.baz
1,2
```

Data is formatted the same way as the "Readable" JSON format, except that all values including `null`, `true`, `false`, and numbers are indistinguishable from their string representations.

It is possible to use the `columnDelimiter`, `rowDelimiter` `quoteChar` and `escapeChar` media-type extensions keys in order to customize the layout of the csv. If some or all of these extensions are not specified, they will default to the following values:

- columnDelimiter: `,`
- rowDelimiter: `\r\n`
- quoteChar: `"`
- escapeChar: `"`

Note: Due to [the following issue](https://github.com/tototoshi/scala-csv/issues/97) in one of our dependencies. The `rowDelimiter` extension will be ignored for any CSV being uploaded. The `rowDelimiter` extension will, however, be observed for downloaded data.
      Also due to [this issue](https://github.com/tototoshi/scala-csv/issues/98) best to avoid non "standard" csv formats. See the `MessageFormatGen.scala` file for examples of which csv formats we test against.

When data is uploaded in CSV format, the headers are interpreted as field names in the same way. As with the Readable JSON format, any string that can be interpreted as another kind of value will be, so for example there's no way to specify the string `"null"`.


## Troubleshooting

First, make sure that the `slamdata/quasar` Github repo is building correctly (the status is displayed at the top of the README).

Then, you can try the following command:

```bash
./sbt test
```

This will ensure that your local version is also passing the tests.

You can also discuss issues [on Discord](https://discord.gg/QNjwCg6).

## Thanks to Sponsors

YourKit supports open source projects with its full-featured Java Profiler. YourKit, LLC is the creator of <a href="https://www.yourkit.com/java/profiler/index.jsp">YourKit Java Profiler</a> and <a href="https://www.yourkit.com/.net/profiler/index.jsp">YourKit .NET Profiler</a>, innovative and intelligent tools for profiling Java and .NET applications.

## Legal

Copyright &copy; 2014 - 2018 SlamData Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
