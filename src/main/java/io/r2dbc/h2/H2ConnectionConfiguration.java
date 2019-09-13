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

package io.r2dbc.h2;

import io.r2dbc.h2.util.Assert;
import reactor.util.annotation.Nullable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Connection configuration information for connecting to an H2 database.
 */
public final class H2ConnectionConfiguration {

    private final CharSequence password;

    private final String url;

    private final String username;

    private final Map<String, String> properties;

    private H2ConnectionConfiguration(@Nullable CharSequence password, String url, @Nullable String username, Map<String, String> properties) {
        this.password = password;
        this.url = Assert.requireNonNull(url, "url must not be null");
        this.username = username;
        this.properties = Assert.requireNonNull(properties, "properties must not be null");
    }

    /**
     * Returns a new {@link Builder}.
     *
     * @return a new {@link Builder}
     */
    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String toString() {
        return "H2ConnectionConfiguration{" +
            "password='REDACTED'" +
            ", properties='" + this.properties + '\'' +
            ", url='" + this.url + '\'' +
            ", username='" + this.username + '\'' +
            '}';
    }

    Optional<CharSequence> getPassword() {
        return Optional.ofNullable(this.password);
    }

    String getUrl() {
        return this.url;
    }

    Map<String, String> getProperties() {
        return this.properties;
    }

    Optional<String> getUsername() {
        return Optional.ofNullable(this.username);
    }

    /**
     * A builder for {@link H2ConnectionConfiguration} instances.
     * <p>
     * <i>This class is not threadsafe</i>
     */
    public static final class Builder {

        private List<String> options = new ArrayList<>();

        private Map<String, String> properties = new LinkedHashMap<>();

        private CharSequence password;

        private String url;

        private String username;

        /**
         * Returns a configured {@link H2ConnectionConfiguration}.
         *
         * @return a configured {@link H2ConnectionConfiguration}
         */
        public H2ConnectionConfiguration build() {
            if (this.options.isEmpty()) {
                return new H2ConnectionConfiguration(this.password, this.url, this.username, this.properties);
            }

            String urlWithOptions = this.options.stream()
                .reduce(this.url, (url, option) -> url += ";" + option);

            return new H2ConnectionConfiguration(this.password, urlWithOptions, this.username, this.properties);
        }

        /**
         * Configure a file-based database, e.g. {@code ~/my-database} or {@code /path/to/my/database.db}.
         *
         * @param path of the database file (automatically prefixed with {@code file:})
         * @return this {@link Builder}
         */
        public Builder file(String path) {
            return url(String.format("file:%s", path));
        }

        /**
         * Configure an in-memory database, e.g. {@code my-test-database}.
         *
         * @param name of a private, in-memory database (automatically prefixed with {@code mem:})
         * @return this {@link Builder}
         */
        public Builder inMemory(String name) {
            return url(String.format("mem:%s", name));
        }

        /**
         * Configure a TCP connection to a remote database.
         *
         * @param host - hostname of the remote database (can be {@code localhost})
         * @param path - path of the database, e.g. {@code ~/test} resolves to {user.home}/test.db
         * @return this {@link Builder}
         */
        public Builder tcp(String host, String path) {
            return url(String.format("tcp://%s/%s", host, path));
        }

        /**
         * Configure a TCP connection to a remote database
         *
         * @param host - hostname of the remote database (can be {@code localhost})
         * @param port - port the database is serving from
         * @param path - path of the database, e.g. {@code ~/test} resolves to {user.home}/test.db
         * @return this {@link Builder}
         */
        public Builder tcp(String host, int port, String path) {
            return url(String.format("tcp://%s:%s/%s", host, port, path));
        }

        /**
         * Configure an option that is appended at the end, e.g. {@code DB_CLOSE_DELAY=10}, prefixed with ";".
         *
         * @param option to append at the end using a {@code ;} prefix.
         * @return this {@link Builder}
         */
        public Builder option(String option) {
            this.options.add(option);
            return this;
        }

        /**
         * Configure a property for H2.
         *
         * @param option the option key.
         * @param value  the option value.
         * @return this {@link Builder}
         */
        public Builder property(String option, String value) {
            this.properties.put(Assert.requireNonNull(option, "option must not be null"), Assert.requireNonNull(value, "value must not be null"));
            return this;
        }

        /**
         * Configure a property for H2 using pre-build {@link H2ConnectionOption}.
         *
         * @param option the option key enum
         * @param value  the option value
         * @return this {@link Builder}
         */
        public Builder property(H2ConnectionOption option, String value) {
            Assert.requireNonNull(option, "option must not be null");
            return property(option.getKey(), value);
        }

        /**
         * Configure the password.
         *
         * @param password the password
         * @return this {@link Builder}
         */
        public Builder password(@Nullable CharSequence password) {
            this.password = password;
            return this;
        }

        @Override
        public String toString() {
            return "Builder{" +
                "password='REDACTED'" +
                ", properties='" + this.properties + '\'' +
                ", url='" + this.url + '\'' +
                ", username='" + this.username + '\'' +
                '}';
        }

        /**
         * Configure the database url. Includes everything after the {@code jdbc:h2:} prefix. For in-memory and file-based databases, must include the proper prefix (e.g. {@code file:} or {@code
         * mem:}).
         * <p>
         * See <a href="https://www.h2database.com/html/features.html#database_url">https://www.h2database.com/html/features.html#database_url</a> for more details.
         *
         * @param url the url
         * @return this {@link Builder}
         * @throws NullPointerException if {@code url} is {@code null}
         */
        public Builder url(String url) {
            this.url = Assert.requireNonNull(url, "url must not be null");
            return this;
        }

        /**
         * Configure the username.
         *
         * @param username the username
         * @return this {@link Builder}
         */
        public Builder username(@Nullable String username) {
            this.username = username;
            return this;
        }

    }
}
