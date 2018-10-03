/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.r2dbc.h2;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import io.r2dbc.h2.util.H2DatabaseExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * @author Greg Turnquist
 */
final class Examples {

	private static final String DATABASE_NAME = "mem:r2dbc-examples";
	private static final String JDBC_CONNECTION_URL = "jdbc:h2:" + DATABASE_NAME;

	@RegisterExtension
	static final H2DatabaseExtension SERVER = new H2DatabaseExtension(JDBC_CONNECTION_URL);

	private final H2ConnectionFactory connectionFactory = new H2ConnectionFactory(Mono.defer(() -> Mono.just(DATABASE_NAME)));

	@BeforeEach
	void createTable() {
		SERVER.getJdbcOperations().execute("CREATE TABLE test ( value INTEGER )");
	}

	@AfterEach
	void dropTable() {
		SERVER.getJdbcOperations().execute("DROP TABLE test");
	}

	@Test
	void batch() {

		SERVER.getJdbcOperations().execute("INSERT INTO test VALUES (100)");

		this.connectionFactory.create()
			.flatMapMany(connection -> connection

				.createBatch()
				.add("INSERT INTO test VALUES(200)")
				.add("SELECT value FROM test")
				.execute()

				.concatWith(close(connection)))
			.as(StepVerifier::create)
			.expectNextCount(2)
			.verifyComplete();
	}

	@Test
	void bindNull() {
		this.connectionFactory.create()
			.flatMapMany(connection -> connection

				.createStatement("INSERT INTO test VALUES($1)")
				.bindNull("$1", Integer.class)
				.add()
				.execute()

				.concatWith(close(connection)))
			.as(StepVerifier::create)
			.expectNextCount(1)
			.verifyComplete();
	}

	@Test
	void compoundStatement() {

		SERVER.getJdbcOperations().execute("INSERT INTO test VALUES (100)");

		this.connectionFactory.create()
			.flatMapMany(connection -> connection

				.createStatement("SELECT value FROM test; SELECT value FROM test")
				.execute()
				.flatMap(Examples::extractColumns)

				.concatWith(close(connection)))
			.as(StepVerifier::create)
			.expectNext(Collections.singletonList(100))
			.expectNext(Collections.singletonList(100))
			.verifyComplete();
	}

	@Test
	void singleStatement() {

		SERVER.getJdbcOperations().execute("INSERT INTO test VALUES (100)");

		this.connectionFactory.create()
			.flatMapMany(connection -> connection

				.createStatement("SELECT value FROM test")
				.execute()
				.flatMap(Examples::extractColumns)

				.concatWith(close(connection)))
			.as(StepVerifier::create)
			.expectNext(Collections.singletonList(100))
			.verifyComplete();
	}

	@Test
	void generatedKeys() {

		SERVER.getJdbcOperations().execute("CREATE TABLE test2 (id SERIAL PRIMARY KEY, value INTEGER)");

		this.connectionFactory.create()
			.flatMapMany(connection ->

				connection.createStatement("INSERT INTO test2(value) VALUES ($1)")
					.bind("$1", 100)
					.add()
					.bind("$1", 200)
					.add()
					.executeReturningGeneratedKeys()
					.flatMap(Examples::extractIds)

					.concatWith(close(connection)))
			.as(StepVerifier::create)
			.expectNext(Collections.singletonList(1))
			.expectNext(Collections.singletonList(2))
			.verifyComplete();
	}

	@Test
	void prepareStatement() {

		this.connectionFactory.create()
			.flatMapMany(connection -> {
				H2Statement statement = connection.createStatement("INSERT INTO test VALUES($1)");

				IntStream.range(0, 10)
					.forEach(i -> statement
						.bind("$1", i)
						.add());

				return statement
					.execute()
					.concatWith(close(connection));
			})
			.as(StepVerifier::create)
			.expectNextCount(10)
			.verifyComplete();
	}

	@Test
	void transactionCommit() {

		SERVER.getJdbcOperations().execute("INSERT INTO test VALUES (100)");

		this.connectionFactory.create()
			.flatMapMany(connection -> connection

				.beginTransaction()
				.<Object>thenMany(connection.createStatement("SELECT value FROM test")
					.execute()
					.flatMap(Examples::extractColumns))

				.concatWith(connection.createStatement("INSERT INTO test VALUES ($1)")
					.bind("$1", 200)
					.execute()
					.flatMap(Examples::extractRowsUpdated))

				.concatWith(connection.createStatement("SELECT value FROM test")
					.execute()
					.flatMap(Examples::extractColumns))

				.concatWith(connection.commitTransaction())

				.concatWith(connection.createStatement("SELECT value FROM test")
					.execute()
					.flatMap(Examples::extractColumns))

				.concatWith(close(connection)))
			.as(StepVerifier::create)
			.expectNext(Collections.singletonList(100))
			.expectNextCount(1)
			.expectNext(Arrays.asList(100, 200))
			.expectNext(Arrays.asList(100, 200))
			.verifyComplete();
	}

//	@Test
	void transactionRollback() {

		SERVER.getJdbcOperations().execute("INSERT INTO test VALUES (100)");

		this.connectionFactory.create()
			.flatMapMany(connection -> connection

				.beginTransaction()
				.<Object>thenMany(connection.createStatement("SELECT value FROM test")
					.execute()
					.flatMap(Examples::extractColumns))

				.concatWith(connection.createStatement("INSERT INTO test VALUES ($1)")
					.bind("$1", 200)
					.execute()
					.flatMap(Examples::extractRowsUpdated))

				.concatWith(connection.createStatement("SELECT value FROM test")
					.execute()
					.flatMap(Examples::extractColumns))

				.concatWith(connection.rollbackTransaction())

				.concatWith(connection.createStatement("SELECT value FROM test")
					.execute()
					.flatMap(Examples::extractColumns))

				.concatWith(close(connection)))
			.as(StepVerifier::create)
			.expectNext(Collections.singletonList(100))
			.expectNextCount(1)
			.expectNext(Arrays.asList(100, 200))
			.expectNext(Collections.singletonList(100))
			.verifyComplete();
	}

	@Test
	void savePoint() {

		SERVER.getJdbcOperations().execute("INSERT INTO test VALUES (100)");

		this.connectionFactory.create()
			.flatMapMany(connection -> connection

				.beginTransaction()
				.<Object>thenMany(connection.createStatement("SELECT value FROM test")
					.execute()
					.flatMap(Examples::extractColumns))

				.concatWith(connection.createStatement("INSERT INTO test VALUES ($1)")
					.bind("$1", 200)
					.execute()
					.flatMap(Examples::extractRowsUpdated))
				.concatWith(connection.createStatement("SELECT value FROM test")
					.execute()
					.flatMap(Examples::extractColumns))

				.concatWith(connection.createSavepoint("test_savepoint"))
				.concatWith(connection.createStatement("INSERT INTO test VALUES ($1)")
					.bind("$1", 300)
					.execute()
					.flatMap(Examples::extractRowsUpdated))
				.concatWith(connection.createStatement("SELECT value FROM test")
					.execute()
					.flatMap(Examples::extractColumns))

				.concatWith(connection.rollbackTransactionToSavepoint("test_savepoint"))
				.concatWith(connection.createStatement("SELECT value FROM test")
					.execute()
					.flatMap(Examples::extractColumns))

				.concatWith(close(connection)))
			.as(StepVerifier::create)
			.expectNext(Collections.singletonList(100))
			.expectNextCount(1)
			.expectNext(Arrays.asList(100, 200))
			.expectNextCount(1)
			.expectNext(Arrays.asList(100, 200, 300))
			.expectNext(Arrays.asList(100, 200))
			.verifyComplete();
	}
	
	private static <T> Mono<T> close(H2Connection connection) {
		return connection
			.close()
			.then(Mono.empty());
	}

	private static Mono<List<Integer>> extractColumns(H2Result result) {
		return result
			.map((row, rowMetadata) -> row.get("value", Integer.class))
			.collectList();
	}

	private static Mono<List<Integer>> extractIds(H2Result result) {
		return result
			.map((row, rowMetadata) -> row.get("id", Integer.class))
			.collectList();
	}

	private static Mono<Integer> extractRowsUpdated(H2Result result) {
		return result.getRowsUpdated();
	}
}
