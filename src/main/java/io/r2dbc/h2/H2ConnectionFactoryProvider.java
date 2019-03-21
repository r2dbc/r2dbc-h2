/*
 * Copyright 2017-2019 the original author or authors.
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

import io.r2dbc.h2.util.Assert;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.ConnectionFactoryProvider;
import io.r2dbc.spi.Option;

import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PROTOCOL;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;

/**
 * An implementation of {@link ConnectionFactoryProvider} for creating {@link H2ConnectionFactory}s.
 */
public final class H2ConnectionFactoryProvider implements ConnectionFactoryProvider {

    /**
     * Driver option value.
     */
    public static final String H2_DRIVER = "h2";

    /**
     * Options.  Semicolon delimited.
     */
    public static final Option<String> OPTIONS = Option.valueOf("options");

    /**
     * File-system protocol.
     */
    public static final String PROTOCOL_FILE = "file";

    /**
     * In-memory protocol.
     */
    public static final String PROTOCOL_MEM = "mem";

    /**
     * Url.
     */
    public static final Option<String> URL = Option.valueOf("url");

    @Override
    public H2ConnectionFactory create(ConnectionFactoryOptions connectionFactoryOptions) {
        Assert.requireNonNull(connectionFactoryOptions, "connectionFactoryOptions must not be null");

        H2ConnectionConfiguration.Builder builder = H2ConnectionConfiguration.builder();

        String protocol = connectionFactoryOptions.getValue(PROTOCOL);
        if (PROTOCOL_FILE.equals(protocol)) {
            builder.file(connectionFactoryOptions.getRequiredValue(DATABASE));
        } else if (PROTOCOL_MEM.equals(protocol)) {
            builder.inMemory(connectionFactoryOptions.getRequiredValue(DATABASE));
        } else if (protocol != null) {
            throw new IllegalArgumentException(String.format("protocol option %s is unsupported (%s, %s)", protocol, PROTOCOL_FILE, PROTOCOL_MEM));
        }

        String url = connectionFactoryOptions.getValue(URL);
        if (url != null) {
            builder.url(url);
        }

        String options = connectionFactoryOptions.getValue(OPTIONS);
        if (options != null) {
            for (String option : options.split(";")) {
                builder.option(option);
            }
        }

        CharSequence password = connectionFactoryOptions.getValue(PASSWORD);
        if (password != null) {
            builder.password(password.toString());
        }

        builder.username(connectionFactoryOptions.getValue(USER));

        return new H2ConnectionFactory(builder.build());
    }

    @Override
    public boolean supports(ConnectionFactoryOptions connectionFactoryOptions) {
        Assert.requireNonNull(connectionFactoryOptions, "connectionFactoryOptions must not be null");

        String driver = connectionFactoryOptions.getValue(DRIVER);
        if (driver == null || !driver.equals(H2_DRIVER)) {
            return false;
        }

        if (connectionFactoryOptions.hasOption(URL)) {
            return true;
        }

        if (connectionFactoryOptions.hasOption(PROTOCOL) && connectionFactoryOptions.hasOption(DATABASE)) {
            return true;
        }

        return false;
    }

    @Override
    public String getDriver() {
        return H2_DRIVER;
    }
}
