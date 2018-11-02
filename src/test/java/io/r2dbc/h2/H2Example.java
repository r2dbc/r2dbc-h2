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

import io.r2dbc.h2.util.H2ServerExtension;
import io.r2dbc.spi.test.Example;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.jdbc.core.JdbcOperations;

final class H2Example {

    @RegisterExtension
    static final H2ServerExtension SERVER = new H2ServerExtension();

    private final H2ConnectionConfiguration configuration = H2ConnectionConfiguration.builder()
        .password(SERVER.getPassword())
        .url(SERVER.getUrl())
        .username(SERVER.getUsername())
        .build();

    private final H2ConnectionFactory connectionFactory = new H2ConnectionFactory(this.configuration);

    // TODO: Remove once implemented
    @Disabled("Not yet implemented")
    @Nested
    final class JdbcStyle implements Example<Integer> {

        @Override
        public H2ConnectionFactory getConnectionFactory() {
            return H2Example.this.connectionFactory;
        }

        @Override
        public Integer getIdentifier(int index) {
            return index;
        }

        @Override
        public JdbcOperations getJdbcOperations() {
            JdbcOperations jdbcOperations = SERVER.getJdbcOperations();

            if (jdbcOperations == null) {
                throw new IllegalStateException("JdbcOperations not yet initialized");
            }

            return jdbcOperations;
        }

        @Override
        public String getPlaceholder(int index) {
            return "?";
        }
    }

    @Nested
    final class PostgresqlStyle implements Example<String> {

        @Override
        public H2ConnectionFactory getConnectionFactory() {
            return H2Example.this.connectionFactory;
        }

        @Override
        public String getIdentifier(int index) {
            return getPlaceholder(index);
        }

        @Override
        public JdbcOperations getJdbcOperations() {
            JdbcOperations jdbcOperations = SERVER.getJdbcOperations();

            if (jdbcOperations == null) {
                throw new IllegalStateException("JdbcOperations not yet initialized");
            }

            return jdbcOperations;
        }

        @Override
        public String getPlaceholder(int index) {
            return String.format("$%d", index + 1);
        }

    }
}
