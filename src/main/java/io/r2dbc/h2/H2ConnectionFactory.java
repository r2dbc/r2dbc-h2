/*
 * Copyright 2018 the original author or authors.
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

package io.r2dbc.h2;

import io.r2dbc.h2.H2DatabaseExceptionFactory.H2R2dbcNonTransientResourceException;
import io.r2dbc.h2.client.Client;
import io.r2dbc.h2.client.SessionClient;
import io.r2dbc.h2.codecs.DefaultCodecs;
import io.r2dbc.h2.util.Assert;
import io.r2dbc.spi.Closeable;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import org.h2.message.DbException;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;


/**
 * An implementation of {@link ConnectionFactory} for creating connections to an H2 database.
 */
public final class H2ConnectionFactory implements ConnectionFactory {

    private final Mono<? extends Client> clientFactory;

    /**
     * Creates a new connection factory.
     *
     * @param configuration the configuration to use to create connections
     * @throws NullPointerException if {@code configuration} is {@code null}
     */
    public H2ConnectionFactory(H2ConnectionConfiguration configuration) {
        this(Mono.fromSupplier(() -> {
            return getSessionClient(configuration, false);
        }));
    }

    /**
     * Create a new {@link CloseableConnectionFactory In-Memory database connection factory} for database at {@code name}.
     * <p>The closeable {@link ConnectionFactory} keeps the database open unless the connection factory is {@link Closeable#close() closed}.
     * <p>The database gets created if it does not exist yet.
     * <p>Connecting to an existing database closes the database for all participating components if the resulting {@link ConnectionFactory} is {@link CloseableConnectionFactory#close() closed}.
     *
     * @param name database name
     * @return connection factory for database at {@code name}
     */
    public static CloseableConnectionFactory inMemory(String name) {
        return inMemory(name, "sa", "");
    }

    /**
     * Create a new {@link CloseableConnectionFactory In-Memory database connection factory} for database at {@code name} given {@code username} and {@code password}.
     * <p>The closeable {@link ConnectionFactory} keeps the database open unless the connection factory is {@link Closeable#close() closed}.
     * <p>The database gets created if it does not exist yet.
     * <p>Connecting to an existing database closes the database for all participating components if the resulting {@link ConnectionFactory} is {@link CloseableConnectionFactory#close() closed}.
     *
     * @param name     database name
     * @param username the username
     * @param password the password to use
     * @return connection factory for database at {@code name}.
     */
    public static CloseableConnectionFactory inMemory(String name, String username, CharSequence password) {
        return inMemory(name, username, password, Collections.emptyMap());
    }

    /**
     * Create a new {@link CloseableConnectionFactory In-Memory database connection factory} for database at {@code name} given {@code username} and {@code password}.
     * <p>The closeable {@link ConnectionFactory} keeps the database open unless the connection factory is {@link Closeable#close() closed}.
     * <p>Connecting to an existing database closes the database for all participating components if the resulting {@link ConnectionFactory} is {@link CloseableConnectionFactory#close() closed}.
     *
     * @param name       database name
     * @param username   the username
     * @param password   the password to use
     * @param properties properties to set
     * @return connection factory for database at {@code name}.
     * @see H2ConnectionOption
     */
    public static CloseableConnectionFactory inMemory(String name, String username, CharSequence password, Map<H2ConnectionOption, String> properties) {

        Assert.requireNonNull(name, "name must not be null");
        Assert.requireNonNull(username, "username must not be null");
        Assert.requireNonNull(password, "password must not be null");
        Assert.requireNonNull(properties, "properties must not be null");

        H2ConnectionConfiguration.Builder builder = H2ConnectionConfiguration.builder().inMemory(name).username(username).password(password);

        for (Map.Entry<H2ConnectionOption, String> entry : properties.entrySet()) {
            builder.property(entry.getKey(), entry.getValue());
        }

        return new DefaultCloseableConnectionFactory(builder.build());
    }

    private static SessionClient getSessionClient(H2ConnectionConfiguration configuration, boolean shutdownDatabaseOnClose) {
        Assert.requireNonNull(configuration, "configuration must not be null");

        try {
            return new SessionClient(configuration.getConnectionInfo(), shutdownDatabaseOnClose);
        } catch (DbException e) {
            throw H2DatabaseExceptionFactory.convert(e);
        }
    }

    H2ConnectionFactory(Mono<? extends Client> clientFactory) {
        this.clientFactory = Assert.requireNonNull(clientFactory, "clientFactory must not be null");
    }

    @Override
    public Mono<H2Connection> create() {
        return this.clientFactory
            .map(client -> new H2Connection(client, new DefaultCodecs(client)));
    }

    @Override
    public H2ConnectionFactoryMetadata getMetadata() {
        return H2ConnectionFactoryMetadata.INSTANCE;
    }

    @Override
    public String toString() {
        return "H2ConnectionFactory{" +
            "clientFactory=" + this.clientFactory +
            '}';
    }

    private static class DefaultCloseableConnectionFactory implements CloseableConnectionFactory {

        private final H2ConnectionConfiguration configuration;

        private final Supplier<SessionClient> clientFactory;

        private volatile SessionClient persistentConnection;

        public DefaultCloseableConnectionFactory(H2ConnectionConfiguration configuration) {
            this.configuration = configuration;
            this.clientFactory = () -> getSessionClient(configuration, false);
            this.persistentConnection = getSessionClient(configuration, true);
        }

        @Override
        public Mono<Void> close() {
            return Mono.defer(() -> {

                SessionClient connection = this.persistentConnection;
                this.persistentConnection = null;

                if (connection != null) {
                    return connection.close();
                }

                return Mono.empty();
            });
        }

        @Override
        public Mono<H2Connection> create() {
            return Mono.fromSupplier(() -> {

                if (this.persistentConnection == null) {
                    throw new H2R2dbcNonTransientResourceException(String.format("ConnectionFactory for %s is closed", this.configuration.getUrl()));
                }

                Client client = this.clientFactory.get();
                return new H2Connection(client, new DefaultCodecs(client));
            });
        }

        @Override
        public ConnectionFactoryMetadata getMetadata() {
            return H2ConnectionFactoryMetadata.INSTANCE;
        }

        @Override
        public String toString() {
            return "CloseableConnectionFactory{" +
                "configuration=" + this.configuration +
                '}';
        }
    }

}
