/*
 * Copyright 2017-2018 the original author or authors.
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

import reactor.util.annotation.Nullable;

import java.util.Objects;
import java.util.Optional;

/**
 * Connection configuration information for connecting to an H2 database.
 */
public final class H2ConnectionConfiguration {

    private final String database;

    private final String password;

    private final String url;

    private final String username;

    private H2ConnectionConfiguration(@Nullable String database, @Nullable String password, String url, @Nullable String username) {
        this.database = database;
        this.password = password;
        this.url = Objects.requireNonNull(url, "url must not be null");
        this.username = username;
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
            "database='" + this.database + '\'' +
            ", password='" + this.password + '\'' +
            ", url='" + this.url + '\'' +
            ", username='" + this.username + '\'' +
            '}';
    }

    Optional<String> getDatabase() {
        return Optional.ofNullable(this.database);
    }

    Optional<String> getPassword() {
        return Optional.ofNullable(this.password);
    }

    String getUrl() {
        return this.url;
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

        private String database;

        private String password;

        private String url;

        private String username;

        /**
         * Returns a configured {@link H2ConnectionConfiguration}.
         *
         * @return a configured {@link H2ConnectionConfiguration}
         */
        public H2ConnectionConfiguration build() {
            return new H2ConnectionConfiguration(this.database, this.password, this.url, this.username);
        }

        /**
         * Configure the database.
         *
         * @param database the database
         * @return this {@link Builder}
         */
        public Builder database(@Nullable String database) {
            this.database = database;
            return this;
        }

        /**
         * Configure the password.
         *
         * @param password the password
         * @return this {@link Builder}
         */
        public Builder password(@Nullable String password) {
            this.password = password;
            return this;
        }

        @Override
        public String toString() {
            return "Builder{" +
                "database='" + this.database + '\'' +
                ", password='" + this.password + '\'' +
                ", url='" + this.url + '\'' +
                ", username='" + this.username + '\'' +
                '}';
        }

        /**
         * Configure the url.
         *
         * @param url the url
         * @return this {@link Builder}
         * @throws NullPointerException if {@code url} is {@code null}
         */
        public Builder url(String url) {
            this.url = Objects.requireNonNull(url, "url must not be null");
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
