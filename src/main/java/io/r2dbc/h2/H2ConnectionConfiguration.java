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

import io.r2dbc.h2.util.Assert;
import reactor.util.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Connection configuration information for connecting to an H2 database.
 */
public final class H2ConnectionConfiguration {

    private final String password;

    private final String url;

    private final String username;

    private H2ConnectionConfiguration(@Nullable String password, String url, @Nullable String username) {
        this.password = password;
        this.url = Assert.requireNonNull(url, "url must not be null");
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
            "password='" + this.password + '\'' +
            ", url='" + this.url + '\'' +
            ", username='" + this.username + '\'' +
            '}';
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

        private List<String> options = new ArrayList<>();
        
        private String password;

        private String url;

        private String username;

        /**
         * Returns a configured {@link H2ConnectionConfiguration}.
         *
         * @return a configured {@link H2ConnectionConfiguration}
         */
        public H2ConnectionConfiguration build() {
            if (this.options.isEmpty()) {
                return new H2ConnectionConfiguration(this.password, this.url, this.username);
            }

            String urlWithOptions = this.options.stream()
                .reduce(this.url, (url, option) -> url += ";" + option);

            return new H2ConnectionConfiguration(this.password, urlWithOptions, this.username);
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
         * Configure an option that is appended at the end, e.g. {@code DB_CLOSE_DELAY=10}, prefixed with ";".
         *
         * @param option to append at the end using a {@code ;} prefix.
         * @return this (@link Builder)
         */
        public Builder option(String option) {
            this.options.add(option);
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
                "password='" + this.password + '\'' +
                ", url='" + this.url + '\'' +
                ", username='" + this.username + '\'' +
                '}';
        }

        /**
         * Configure the database url. Includes everything after the {@code jdbc:h2:} prefix. For in-memory and file-based databases, must include the proper prefix (e.g. {@code file:} or {@code
         * mem:}).
         * <p>
         * See <a href="http://www.h2database.com/html/features.html#database_url">http://www.h2database.com/html/features.html#database_url</a> for more details.
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
