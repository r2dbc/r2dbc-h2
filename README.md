# Reactive Relational Database Connectivity H2 Implementation

This project contains the [H2][h] implementation of the [R2DBC SPI][r]. This implementation is not inteded to be used directly, but rather to be used as the backing implementation for a humane client library to delegate to.

This driver provides the following features:

* Filesystem or in-memory instances
* Explict transactions
* Execution of prepared statements with bindings
* Execution of batch statements without bindings
* Read and write support for all data types except LOB types (e.g. `BLOB`, `CLOB`)

[h]: https://www.h2database.com/html/main.html
[r]: https://github.com/r2dbc/r2dbc-spi

## Maven
Both milestone and snapshot artifacts (library, source, and javadoc) can be found in Maven repositories.

```xml
<dependency>
  <groupId>io.r2dbc</groupId>
  <artifactId>r2dbc-h2</artifactId>
  <version>1.0.0.BUILD-SNAPSHOT</version>
</dependency>
```

Artifacts can be found at the following repositories.

### Repositories
```xml
<repository>
    <id>spring-snapshots</id>
    <name>Spring Snapshots</name>
    <url>https://repo.spring.io/snapshot</url>
    <snapshots>
        <enabled>true</enabled>
    </snapshots>
</repository>
```

```xml
<repository>
    <id>spring-milestones</id>
    <name>Spring Milestones</name>
    <url>https://repo.spring.io/milestone</url>
    <snapshots>
        <enabled>false</enabled>
    </snapshots>
</repository>
```

## Usage
Configuration of the `ConnectionFactory` can be accomplished in two ways:

### Connection Factory Discovery
```java
ConnectionFactory connectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
   .option(DRIVER, "h2")
   .option(PROTOCOL, "...")  // file, mem
   .option(DATABASE, "...")
   .build());

Mono<Connection> connection = connectionFactory.create();
```

Supported Connection Factory Discovery options:

| Option | Description
| ------ | -----------
| `driver` | Must be `h2`
| `protocol` | Must be `file` or `mem`.  Requires `database` if set.  _(Optional)_ if `url` set.
| `url` | A fully qualified H2 URL.  _(Optional)_ if `procotol` and `database` are set.
| `username` | Login username
| `password` | Login password
| `options` | A semicolon-delimited list of configuration options.  _(Optional)_

### Programmatic
```java
ConnectionFactory connectionFactory = new H2ConnectionFactory(H2ConnectionConfiguration.builder()
    .inMemory("...")
    .build());

Mono<Connection> connection = connectionFactory.create();
```

H2 uses index parameters that are prefixed with `$`.  The following SQL statement makes use of parameters:

```sql
INSERT INTO person (id, first_name, last_name) VALUES ($1, $2, $3)
```
Parameters are referenced using the same identifiers when binding these:

```java
connection
    .createStatement("INSERT INTO person (id, first_name, last_name) VALUES ($1, $2, $3)")
    .bind("$1", 1)
    .bind("$2", "Walter")
    .bind("$3", "White")
    .execute()
```

Binding also allowed positional index (zero-based) references.  The parameter index is derived from the parameter discovery order when parsing the query.

## License
This project is released under version 2.0 of the [Apache License][l].

[l]: https://www.apache.org/licenses/LICENSE-2.0
