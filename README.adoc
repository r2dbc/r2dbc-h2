= Reactive Relational Database Connectivity H2 Implementation

image:https://github.com/r2dbc/r2dbc-h2/workflows/Java%20CI%20with%20Maven/badge.svg?branch=main["Java CI with Maven",link="https://github.com/r2dbc/r2dbc-h2/actions?query=workflow%3A%22Java+CI+with+Maven%22+branch%3Amain"]

image::https://maven-badges.herokuapp.com/maven-central/io.r2dbc/r2dbc-h2/badge.svg[Maven Central, link="https://maven-badges.herokuapp.com/maven-central/io.r2dbc/r2dbc-h2"]

This project contains the https://www.h2database.com/html/main.html[H2] implementation of the https://github.com/r2dbc/r2dbc-spi[R2DBC SPI].
This implementation is not intended to be used directly, but rather to be used as the backing implementation for a humane client library.

This driver provides the following features:

* Filesystem or in-memory instances
* Explict transactions
* Execution of prepared statements with bindings
* Execution of batch statements without bindings
* Read and write support for all data types except LOB types (e.g. `BLOB`, `CLOB`)

WARNING: Since this driver runs on top of the internals of H2, there is risk of change.
`r2dbc-h2` does not guarantee compatibility except against the version of H2 found in the build file.
Because various parts of H2 are blocking, like file and network access, the only non-blocking assurances are in the layers above H2.
Nevertheless, `r2dbc-h2` is a great way to warm up to the usage of R2DBC with a small footprint.

== Code of Conduct

This project is governed by the https://github.com/r2dbc/.github/blob/main/CODE_OF_CONDUCT.adoc[R2DBC Code of Conduct]. By participating, you are expected to uphold this code of conduct. Please report unacceptable behavior to mailto:info@r2dbc.io[info@r2dbc.io].

== Getting Started

Here is a quick teaser of how to use R2DBC H2 in Java:

**URL Connection Factory Discovery for In-Memory Databases**

[source,java]
----
ConnectionFactory connectionFactory = ConnectionFactories.get("r2dbc:h2:mem:///testdb");

Publisher<? extends Connection> connectionPublisher = connectionFactory.create();
----

**URL Connection Factory Discovery for File Databases**

[source,java]
----
ConnectionFactory connectionFactory = ConnectionFactories.get("r2dbc:h2:file///my/relative/path");

Publisher<? extends Connection> connectionPublisher = connectionFactory.create();
----

**Programmatic Connection Factory Discovery**

[source,java]
----
ConnectionFactoryOptions options = builder()
    .option(DRIVER, "h2")
    .option(PROTOCOL, "...")  // file, mem
    .option(HOST, "…")
    .option(USER, "…")
    .option(PASSWORD, "…")
    .option(DATABASE, "…")
    .build();

ConnectionFactory connectionFactory = ConnectionFactories.get(options);

Publisher<? extends Connection> connectionPublisher = connectionFactory.create();

// Alternative: Creating a Mono using Project Reactor
Mono<Connection> connectionMono = Mono.from(connectionFactory.create());
----

**Supported ConnectionFactory Discovery Options**

[%header,cols=2*]
|===
| Option            | Description
| `driver`          | Must be `h2`.
| `protocol`        | Must be `file`, `mem`, or `tcp`. Requires `database` if set _(Optional)_
| `host`            | Only for `tcp` protocol: Server hostname to connect to. _(Optional)_
| `port`            | Only for `tcp` protocol: Server port to connect to. _(Optional)_
| `username`        | Login username.
| `password`        | Login password.
| `database`        | Database to use. For `file` protocol: Relative (`r2dbc:h2:file//../relative/file/name`) or absolute (`r2dbc:h2:file///absolute/file/name`) file name. For `mem` protocol: In-memory database name (`r2dbc:h2:mem:///testdb`).
| `<well-known-h2-option>`         | Pass-thru of well-known H2 options such as `DB_CLOSE_DELAY=10&MODE=DB2`. See https://github.com/r2dbc/r2dbc-h2/blob/main/src/main/java/io/r2dbc/h2/H2ConnectionOption.java[`io.r2dbc.h2.H2ConnectionOption`] for all options. _(Optional)_
| `options`         | A semicolon-delimited list of H2 configuration options(`options=DB_CLOSE_DELAY=10;DB_CLOSE_ON_EXIT=true;…)`. _(Optional)_
|===

**Programmatic Configuration**

[source,java]
----
H2ConnectionFactory connectionFactory = new H2ConnectionFactory(H2ConnectionConfiguration.builder()
    .inMemory("...")
    .option(H2ConnectionOption.DB_CLOSE_DELAY, "-1")
    .build());

Mono<Connection> connection = connectionFactory.create();
----

**Programmatic In-Memory Database Configuration**

[source,java]
----
CloseableConnectionFactory connectionFactory = H2ConnectionFactory.inMemory("testdb");

Mono<Connection> connection = connectionFactory.create();
----

== Maven

Artifacts can be found on https://search.maven.org/search?q=r2dbc-h2[Maven Central].

[source,xml]
----
<dependency>
  <groupId>io.r2dbc</groupId>
  <artifactId>r2dbc-h2</artifactId>
  <version>${version}</version>
</dependency>
----

If you'd rather like the latest snapshots of the upcoming major version, use our Maven snapshot repository and declare the appropriate dependency version.

[source,xml]
----
<dependency>
  <groupId>io.r2dbc</groupId>
  <artifactId>r2dbc-h2</artifactId>
  <version>${version}.BUILD-SNAPSHOT</version>
</dependency>

<repository>
  <id>sonatype-nexus-snapshots</id>
  <name>Sonatype OSS Snapshot Repository</name>
  <url>https://oss.sonatype.org/content/repositories/snapshots</url>
</repository>
----

== Setting query params

H2 uses index parameters that are prefixed with `$`.
The following SQL statement makes use of parameters:

[source,sql]
----
INSERT INTO person (id, first_name, last_name) VALUES ($1, $2, $3)
----

Parameters are referenced using the same identifiers when binding these:

[source,java]
----
connection
    .createStatement("INSERT INTO person (id, first_name, last_name) VALUES ($1, $2, $3)")
    .bind("$1", 1)
    .bind("$2", "Walter")
    .bind("$3", "White")
    .execute()
----

== Geometry support

`r2dbc-h2` will automatically register support for https://locationtech.github.io/jts/[JTS Toplogy Suite] and handle it's `Geometry` types if `org.locationtech.jts:jts-core` is on the classpath.

To enable, add this to your build:

[source,xml]
----
<dependency>
    <groupId>org.locationtech.jts</groupId>
    <artifactId>jts-core</artifactId>
    <version>${jts.version}</version>
</dependency>
----

IMPORTANT: Be sure to plug in your version of JTS!

Also read https://h2database.com/html/datatypes.html#geometry_type[H2's reference documentation] on `GEOMETRY` types.

== We also support params binding as

* index `bind(1, "Walter")`.
Notice that passing an integer means index (zero-based) references.
* $ symbol `bind("$2", "Walter")`.
H2 supports postgres params notation.
* Object (Integer) `bind(yourIntegerAsObject, "Walter")`.
If you index (int) was converted into object by a framework

=== Running JMH Benchmarks

Running the JMH benchmarks builds and runs the benchmarks without running tests.

[source,bash]
----
 $ ./mvnw clean install -Pjmh
----

== Getting Help

Having trouble with R2DBC? We'd love to help!

* Check the https://r2dbc.io/spec/0.8.1.RELEASE/spec/html/[spec documentation], and https://r2dbc.io/spec/0.8.1.RELEASE/api/[Javadoc].
* If you are upgrading, check out the https://r2dbc.io/spec/0.8.1.RELEASE/CHANGELOG.txt[changelog] for "new and noteworthy" features.
* Ask a question - we monitor https://stackoverflow.com[stackoverflow.com] for questions
  tagged with https://stackoverflow.com/tags/r2dbc[`r2dbc`].
  You can also chat with the community on https://gitter.im/r2dbc/r2dbc[Gitter]
* Report bugs with R2DBC H2 at https://github.com/r2dbc/r2dbc-h2/issues[github.com/r2dbc/r2dbc-h2/issues].

== Reporting Issues

R2DBC uses GitHub as issue tracking system to record bugs and feature requests.
If you want to raise an issue, please follow the recommendations below:

* Before you log a bug, please search the https://github.com/r2dbc/r2dbc-h2/issues[issue tracker] to see if someone has already reported the problem.
* If the issue doesn't already exist, https://github.com/r2dbc/r2dbc-h2/issues/new[create a new issue].
* Please provide as much information as possible with the issue report, we like to know the version of R2DBC H2 that you are using and JVM version.
* If you need to paste code, or include a stack trace use Markdown +++```+++ escapes before and after your text.
* If possible try to create a test-case or project that replicates the issue.
Attach a link to your code or a compressed file containing your code.

== Building from Source

You don't need to build from source to use R2DBC H2 (binaries in Maven Central), but if you want to try out the latest and greatest, R2DBC H2 can be easily built with the
https://github.com/takari/maven-wrapper[maven wrapper].
You also need JDK 1.8.

[source,bash]
----
 $ ./mvnw clean install
----

If you want to build with the regular `mvn` command, you will need https://maven.apache.org/run-maven/index.html[Maven v3.5.0 or above].

_Also see https://github.com/r2dbc/.github/blob/main/CONTRIBUTING.adoc[CONTRIBUTING.adoc] if you wish to submit pull requests.
Commits require `Signed-off-by` (`git commit -s`) to ensure https://developercertificate.org/[Developer Certificate of Origin]._

== Staging to Maven Central

To stage a release to Maven Central, you need to create a release tag (release version):

. `ci/create-release.sh <release-version> <next-snapshot-version>` (e.g. `ci/create-release.sh 0.8.5.RELEASE 0.8.6.BUILD-SNAPSHOT`)
. `git checkout release`
. `git reset --hard v<release-version>` (e.g. `git reset --hard v0.8.5.RELEASE`, observe the `v` prefix)
. `git push --force`

This push will trigger a Maven staging build (see `build-and-deploy-to-maven-central.sh`).

== License

This project is released under version 2.0 of the https://www.apache.org/licenses/LICENSE-2.0[Apache License].

