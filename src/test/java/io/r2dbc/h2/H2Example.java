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

import static io.r2dbc.spi.Mutability.*;
import static io.r2dbc.spi.test.Example.*;

import io.r2dbc.h2.util.H2DatabaseExtension;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Mutability;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.test.Example;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import org.springframework.jdbc.core.JdbcOperations;

/**
 * @author Greg Turnquist
 */
final class H2Example implements Example<String> {

	private static final String DATABASE_NAME = "mem:r2dbc-examples";
	private static final String JDBC_CONNECTION_URL = "jdbc:h2:" + DATABASE_NAME;

	@RegisterExtension
	static final H2DatabaseExtension SERVER = new H2DatabaseExtension(JDBC_CONNECTION_URL);

	private final H2ConnectionFactory connectionFactory = new H2ConnectionFactory(Mono.just(DATABASE_NAME));


	@Override
	public ConnectionFactory getConnectionFactory() {
		return this.connectionFactory;
	}

	@Override
	public String getIdentifier(int index) {
		return getPlaceholder(index);
	}

	@Override
	public JdbcOperations getJdbcOperations() {
		return SERVER.getJdbcOperations();
	}

	@Override
	public String getPlaceholder(int index) {
		return String.format("$%d", index + 1);
	}


	@Test
	@Override
	public void connectionMutability() {

		Mono.from(getConnectionFactory().create())
			.flatMapMany(connection -> Mono.from(connection

				.setTransactionMutability(READ_ONLY))
				.thenMany(Flux.from(connection.createStatement(String.format("INSERT INTO test VALUES (%s)", getPlaceholder(0)))
					.bind(getIdentifier(0), 100)
					.execute())
					.flatMap(Example::extractRowsUpdated))

				.concatWith(close(connection)))
			.as(StepVerifier::create)
			.verifyError(R2dbcException.class);

		// Switch to READ_WRITE to support test cleanup.
		
		this.connectionFactory.create()
			.flatMapMany(connection -> Mono.from(connection.setTransactionMutability(Mutability.READ_WRITE)))
			.as(StepVerifier::create)
			.verifyComplete();
	}

	@Test
	@Override
	public void transactionMutability() {

		Mono.from(getConnectionFactory().create())
			.flatMapMany(connection -> Mono.from(connection

				.beginTransaction())

				.then(Mono.from(connection.setTransactionMutability(READ_ONLY)))
				.thenMany(Flux.from(connection.createStatement(String.format("INSERT INTO test VALUES (%s)", getPlaceholder(0)))
					.bind(getIdentifier(0), 200)
					.execute())
					.flatMap(Example::extractRowsUpdated))

				.concatWith(close(connection)))
			.as(StepVerifier::create)
			.verifyError(R2dbcException.class);

		// Switch to READ_WRITE to support test cleanup.

		this.connectionFactory.create()
			.flatMapMany(connection -> Mono.from(connection.setTransactionMutability(Mutability.READ_WRITE)))
			.as(StepVerifier::create)
			.verifyComplete();

	}
}
