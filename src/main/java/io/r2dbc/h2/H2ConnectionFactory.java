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

import io.r2dbc.h2.client.Client;
import io.r2dbc.h2.client.SessionClient;
import io.r2dbc.h2.codecs.DefaultCodecs;
import io.r2dbc.h2.util.Assert;
import io.r2dbc.spi.ConnectionFactory;
import org.h2.engine.ConnectionInfo;
import reactor.core.publisher.Mono;

import java.util.Properties;

import static org.h2.engine.Constants.START_URL;

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
        this(Mono.defer(() -> {
            Assert.requireNonNull(configuration, "configuration must not be null");

            return Mono.just(new SessionClient(getConnectionInfo(configuration)));
        }));
    }

    H2ConnectionFactory(Mono<? extends Client> clientFactory) {
        this.clientFactory = Assert.requireNonNull(clientFactory, "clientFactory must not be null");
    }

    @Override
    public Mono<H2Connection> create() {
        return this.clientFactory
            .map(client -> new H2Connection(client, new DefaultCodecs()));
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

    private static ConnectionInfo getConnectionInfo(H2ConnectionConfiguration configuration) {
        StringBuilder sb = new StringBuilder(START_URL).append(configuration.getUrl());
        configuration.getUsername().ifPresent(username -> sb.append(";USER=").append(username));
        configuration.getPassword().ifPresent(password -> sb.append(";PASSWORD=").append(password));

        return new ConnectionInfo(sb.toString(), new Properties());
    }

}
