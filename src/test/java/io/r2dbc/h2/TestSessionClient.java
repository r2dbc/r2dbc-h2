/*
 * Copyright 2025 the original author or authors.
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

import io.r2dbc.h2.client.SessionClient;
import io.r2dbc.h2.codecs.DefaultCodecs;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.h2.engine.ConnectionInfo;

import java.util.Properties;
import java.util.UUID;
import java.util.function.Function;

/**
 * Test utility to work with a {@link SessionClient} and an associated {@link H2Connection}.
 *
 * @author Mark Paluch
 */
public class TestSessionClient {

    private final SessionClient client;

    private final H2Connection connection;

    private TestSessionClient(SessionClient client) {
        this.client = client;
        this.connection = new H2Connection(client, new DefaultCodecs(client));
    }

    /**
     * Create a new {@link TestSessionClient} with a fresh in-memory database.
     */
    public static TestSessionClient create() {

        ConnectionInfo configuration = new ConnectionInfo("jdbc:h2:mem:" + UUID.randomUUID() + ";USER=sa;PASSWORD=sa;", new Properties(), null, null);
        SessionClient sessionClient = new SessionClient(configuration, false);
        return new TestSessionClient(sessionClient);
    }

    /**
     * Create a new {@link TestSessionClient} given {@link ConnectionFactoryOptions}.
     */
    public static TestSessionClient create(ConnectionFactoryOptions options) {

        H2ConnectionConfiguration configuration = H2ConnectionFactoryProvider.getConfiguration(options);
        SessionClient sessionClient = new SessionClient(configuration.getConnectionInfo(), false);
        return new TestSessionClient(sessionClient);
    }

    /**
     * Execute the given callback with the underlying {@link SessionClient}.
     *
     * @param callback callback action.
     * @return return value of the callback.
     */
    public <T> T doWithClient(Function<SessionClient, T> callback) {
        return callback.apply(this.client);
    }

    /**
     * Execute the given callback with the underlying {@link H2Connection}.
     *
     * @param callback callback action.
     * @return return value of the callback.
     */
    public <T> T doWithConnection(Function<H2Connection, T> callback) {
        return callback.apply(this.connection);
    }

}
