# Cottontail DB

![Cottontail CI](https://github.com/ppanopticon/cottontaildb/workflows/Cottontail%20CI/badge.svg?branch=master)

Cottontail DB is a column store aimed at multimedia retrieval. It allows for classical boolean as well as vector-space retrieval (k-nearest-neighbours lookup) used in similarity search.

## Setup
Cottontail DB requires Java 9 or newer (Open JDK or Oracle JDK should both work). 

Please clone this repository including submodules using either

``git clone --recurse-submodules https://github.com/vitrivr/cottontaildb.git``

or initialize submodules after cloning using the following command from within the project directory

``git submodule update --init --recursive``

Before executing or building Cottontail DB, you must generate Proto sources by executing the Gradle task `generateProto`, i.e. by running `./gradlew generateProto` from within the project directory.

### Building and starting Cottontail DB
You can simply build an executable JAR with the `./gradlew shadowJar` gradle task.
Alternatively -- preferably -- an executable distribution of Cottontail DB can then be built from sources using the 
Gradle tasks `distTar` or `distZip`. Distributions will be stored relative to the project root in `build/distributions` as either TAR or ZIP file.

Cottontail DB release artifacts (either built or downloaded from the releases page) can be started by executing `bin/cottontaildb` or `bin/cottontaildb.bat` (Windows). It requires a path to a valid configuration file as a program argument, i.e.

``bin/cottontaildb /path/to/your/config.json``

This should bring up the following cottontail CLI prompt:

```
2020-09-16 15:20:20 INFO  CottontailGrpcServer:62 - Cottontail DB server is up and running at port 1865 ! Hop along...
cottontaildb> 
```

To get a list of available commands, type `help`. Currently, there is type-ahead for commands,
schema and entity.

### Using Cottontail DB Docker Container

There is a pre-built Docker container for Cottontail DB for every release version. You can run it using the following command

``docker run --name cottontaildb -p 1865:1865 -v /path/to/volume:/cottontaildb-data docker.pkg.github.com/vitrivr/cottontaildb/cottontaildb:<version>``

It is important to expose the Cottontail DB port using `-p 1865:1865` (adjust uf using a different port) and to map the data directory from the host into the container using `-v`. The data directory is expected to contain a valid `config.json` file!

Please mind, that you need to login into GitHub in order to be able to download the Docker image. See official [manual](https://help.github.com/en/packages/using-github-packages-with-your-projects-ecosystem/configuring-docker-for-use-with-github-packages) for further information

### Configuration
All the configuration of Cottontail DB is done by means of a single configuration file. See `config.json` in project directory for structure of such a file. Most importantly, the file should contain at least the following
parameters:

* __root__: Path to the root directory used by Cottontail DB. The catalogue and all the data will be stored in this location. Hence, there must be enough space and Cottontail DB must be allowed to read and write it.
* __memoryConfig.forceUnmapMappedFiles__: Determines whether MappedByteBuffers should be force-unmapped. Should be set to true unless it causes problems.
* __memoryConfig.dataPageShift__: Size of a single data page. A value of e.g. 22 means, that a single page has 2^22 bytes.
* __memoryConfig.cataloguePageShift__: Size of a single catalogue page. A value of e.g. 20 means, that a single page has 2^20 bytes.

Remaining parameters will be documented in a future version of this file. Check `org.vitrivr.cottontail.config` package for code documentation of the configuration parameters.

### Connecting to Cottontail DB
Communication with Cottontail DB is facilitated by [gRPC](https://grpc.io/). By default, the gRPC endpoint runs on **port 1865**. The server provides three different services: one for data definition (DDL), one for
data management (DML) and one for queries (DQL).

To connect to Cottontail DB, use the gRPC library of your preference based on the programming environment you use. For example, in Kotlin, a connection could be created as follows:

```kotlin
    val channel = ManagedChannelBuilder.forAddress("127.0.0.1", 1865).usePlaintext().build()
    val dqlService =  CottonDQLGrpc.newBlockingStub(channel)
    val ddlService =  CottonDDLGrpc.newBlockingStub(channel)
    val dmlService =  CottonDMLGrpc.newBlockingStub(channel)
```

The [example repository](https://github.com/vitrivr/cottontaildb-examples) points to some simple examples as to how Cottontail DB can be used.

## Credits
Cottontail DB is based on the ideas presented in the following papers:

- Ivan Giangreco and Heiko Schuldt (2016): ADAMpro: Database Support for Big Multimedia Retrieval. Datenbank-Spektrum.
http://link.springer.com/article/10.1007/s13222-015-0209-y
- Ivan Giangreco (2018): Database support for large-scale multimedia retrieval. PhD Thesis. https://edoc.unibas.ch/64751/

Furthermore, the current release of Cottontail DB relies heavily on [MapDB](http://www.mapdb.org/) for internal data organization and storage.
