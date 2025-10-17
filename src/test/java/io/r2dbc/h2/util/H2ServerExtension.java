/*
 * Copyright 2017-2018 the original author or authors.
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

import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.UUID;

import static io.r2dbc.h2.H2ConnectionFactoryProvider.H2_DRIVER;
import static io.r2dbc.h2.H2ConnectionFactoryProvider.URL;
import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;

public final class H2ServerExtension implements ParameterResolver {

    private static final ExtensionContext.Namespace NAMESPACE = ExtensionContext.Namespace.create(H2ServerExtension.class);

    private static final String password = UUID.randomUUID().toString();

    private static final String url = String.format("mem:%s", UUID.randomUUID());

    private static final String username = UUID.randomUUID().toString();

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        return DataSource.class.isAssignableFrom(parameterContext.getParameter().getType()) || JdbcOperations.class.isAssignableFrom(parameterContext.getParameter().getType()) || H2ServerExtension.class.isAssignableFrom(parameterContext.getParameter().getType()) || ConnectionFactory.class.isAssignableFrom(parameterContext.getParameter().getType());
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {

        if (H2ServerExtension.class.isAssignableFrom(parameterContext.getParameter().getType())) {
            return this;
        }

        if (ConnectionFactory.class.isAssignableFrom(parameterContext.getParameter().getType())) {
            return ConnectionFactories.get(ConnectionFactoryOptions.builder()
                .option(DRIVER, H2_DRIVER)
                .option(PASSWORD, getPassword())
                .option(URL, getUrl())
                .option(USER, getUsername())
                .build());
        }

        JdbcDataSource connectionPool = getConnectionPool(extensionContext);

        if (DataSource.class.isAssignableFrom(parameterContext.getParameter().getType())) {
            return connectionPool;
        }

        return new JdbcTemplate(connectionPool);
    }

    public String getPassword() {
        return password;
    }

    public String getUrl() {
        return url;
    }

    public String getUsername() {
        return username;
    }

    private JdbcDataSource getConnectionPool(ExtensionContext context) {
        ExtensionContext.Store store = context.getStore(NAMESPACE);
        return store.getOrComputeIfAbsent(JdbcDataSource.class, it -> {

            JdbcDataSource dataSource = new JdbcDataSource();
            dataSource.setURL(String.format("jdbc:h2:%s;DB_CLOSE_DELAY=-1;TRACE_LEVEL_FILE=4", url));
            dataSource.setUser(this.getUsername());
            dataSource.setPassword(this.getPassword());

            return dataSource;
        }, JdbcDataSource.class);
    }
}
