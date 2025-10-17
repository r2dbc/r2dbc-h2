/*
 * Copyright 2018-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.r2dbc.h2.util;

import io.r2dbc.h2.H2Connection;
import io.r2dbc.h2.H2ConnectionFactory;
import io.r2dbc.h2.H2ConnectionFactoryProvider;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.RegisterExtension;

import static io.r2dbc.h2.H2ConnectionFactoryProvider.URL;
import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;
import static io.r2dbc.spi.ConnectionFactoryOptions.builder;

/**
 * Support class for integration tests.
 */
public abstract class IntegrationTestSupport {

    @RegisterExtension
    protected static final H2ServerExtension SERVER = new H2ServerExtension();

    protected static H2ConnectionFactory connectionFactory;

    protected static H2Connection connection;

    protected static ConnectionFactoryOptions options;

    @BeforeAll
    static void beforeAll() {

        options = builder()
            .option(DRIVER, H2ConnectionFactoryProvider.H2_DRIVER)
            .option(PASSWORD, SERVER.getPassword())
            .option(URL, SERVER.getUrl())
            .option(USER, SERVER.getUsername())
            .build();

        connectionFactory = (H2ConnectionFactory) ConnectionFactories.get(options);
        connection = connectionFactory.create().block();
    }

    @AfterAll
    static void afterAll() {

        if (connection != null) {
            connection.close().subscribe();
        }
    }
}
