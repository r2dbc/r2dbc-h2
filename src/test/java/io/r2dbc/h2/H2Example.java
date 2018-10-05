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

import io.r2dbc.h2.util.H2DatabaseExtension;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.test.Example;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.extension.RegisterExtension;
import reactor.core.publisher.Mono;
import org.springframework.jdbc.core.JdbcOperations;

/**
 * @author Greg Turnquist
 */
final class H2Example implements Example<String> {

	private static final String DATABASE_NAME = "mem:r2dbc-examples";
	private static final String JDBC_CONNECTION_URL = "jdbc:h2:" + DATABASE_NAME;

	@RegisterExtension
	static final H2DatabaseExtension SERVER = new H2DatabaseExtension(JDBC_CONNECTION_URL);

	private final H2ConnectionFactory connectionFactory = new H2ConnectionFactory(Mono.defer(() -> Mono.just(DATABASE_NAME)));


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

	@Disabled
	@Override
	public void connectionMutability() {
		// Not implemented yet.
	}

	@Disabled
	@Override
	public void transactionMutability() {
		// Not implemented yet.
	}
}
