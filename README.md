# Cottontail DB

![Cottontail CI](https://github.com/ppanopticon/cottontaildb/workflows/Cottontail%20CI/badge.svg?branch=master)
[![Maven Central](https://img.shields.io/maven-central/v/org.vitrivr/cottontaildb.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22org.vitrivr%22%20AND%20a:%22cottontaildb%22)

Cottontail DB is a column store aimed at multimedia retrieval. It allows for classical boolean as well as vector-space retrieval, i.e., nearest-neighbours lookup, as used in similarity search. If you're interested in using or contributing to
Cottontail DB, please have a look at the [Wiki](https://github.com/vitrivr/cottontaildb/wiki/).

## Setup
Cottontail DB requires Java 11 or newer (Open JDK or Oracle JDK should both work). Please clone this repository using:

``git clone https://github.com/vitrivr/cottontaildb.git``

The entire project is a Gradle project and comes with a Gradle Wrapper so things should work pretty much out of the box.

### Building and starting Cottontail DB

You can build an executable JAR with the `./gradlew shadowJar` gradle task. Alternatively -- preferably -- an executable distribution of Cottontail DB can be built from the sources using the Gradle tasks `distTar` or `distZip`. Distributions will be
stored relatively to the project root in `build/distributions` as either TAR or ZIP file.

All the details as to how Cottontail DB can be setup and started can be found in the [Wiki](https://github.com/vitrivr/cottontaildb/wiki/Setup).

### Using Cottontail DB Docker Image

Cottontail DB is available as Docker Image from [DockerHub](https://hub.docker.com/r/vitrivr/cottontaildb). Please have a look at the repository instructions and/or the [Wiki](https://github.com/vitrivr/cottontaildb/wiki/Setup) for more information.

### Connecting to Cottontail DB

Communication with Cottontail DB is facilitated by [gRPC](https://grpc.io/). By default, the gRPC endpoint runs on **port 1865**. To connect to Cottontail DB, you must first generate the model classes and stubs using the gRPC library of your
preference based on the programming environment you use. You can find the latest gRPC definitions [here](https://github.com/vitrivr/cottontaildb-proto).

For Kotlin and Java, there is also a Maven dependency, which includes pre-built stubs and models as well as a client API:

```xml
<dependency>
  <groupId>org.vitrivr</groupId>
  <artifactId>cottontaildb-proto</artifactId>
  <version>0.12.0</version>
</dependency>
```

More information as to how to connect to and use Cottontail DB can be found in the [Wiki](https://github.com/vitrivr/cottontaildb/wiki/Connecting-to-Cottontail-DB) and the [example repository](https://github.com/vitrivr/cottontaildb-examples).

## Citation
We kindly ask you to refer to the following paper in publications mentioning or employing Cottontail DB:

Ralph Gasser, Luca Rossetto, Silvan Heller, Heiko Schuldt. _Cottontail DB: An Open Source Database System for Multimedia Retrieval and Analysis._ In Proceedings of 28th ACM International Conference on Multimedia (ACM MM 2020), Seattle, USA, 2020

**Link:** https://doi.org/10.1145/3394171.3414538

**Bibtex:**

```
@inproceedings{10.1145/3394171.3414538,
    author = {Gasser, Ralph and Rossetto, Luca and Heller, Silvan and Schuldt, Heiko},
    title = {Cottontail DB: An Open Source Database System for Multimedia Retrieval and Analysis},
    year = {2020},
    isbn = {9781450379885},
    publisher = {Association for Computing Machinery},
    address = {New York, NY, USA},
    doi = {10.1145/3394171.3414538},
    booktitle = {Proceedings of the 28th ACM International Conference on Multimedia},
    pages = {4465–4468},
    numpages = {4},
    keywords = {open source, multimedia retrieval, database, multimedia indexing, data management system},
    location = {Seattle, WA, USA},
    series = {MM '20}
}
```

## Credits
Cottontail DB is based on the ideas presented in the following papers:

- Ivan Giangreco and Heiko Schuldt (2016): ADAMpro: Database Support for Big Multimedia Retrieval. Datenbank-Spektrum.
http://link.springer.com/article/10.1007/s13222-015-0209-y
- Ivan Giangreco (2018): Database support for large-scale multimedia retrieval. PhD Thesis. https://edoc.unibas.ch/64751/