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

import org.h2.jdbcx.JdbcConnectionPool;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.JdbcTemplate;
import reactor.util.annotation.Nullable;

import java.util.UUID;

public final class H2ServerExtension implements BeforeAllCallback, AfterAllCallback {

    private final String password = UUID.randomUUID().toString();

    private final String url = String.format("mem:%s", UUID.randomUUID().toString());

    private final String username = UUID.randomUUID().toString();

    private JdbcConnectionPool dataSource;

    private JdbcOperations jdbcOperations;

    @Override
    public void afterAll(ExtensionContext context) {
    }

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL(String.format("jdbc:h2:%s;USER=%s;PASSWORD=%s;DB_CLOSE_DELAY=-1;TRACE_LEVEL_FILE=4", this.url, this.username, this.password));

        this.dataSource = JdbcConnectionPool.create(dataSource);
        this.jdbcOperations = new JdbcTemplate(this.dataSource);
    }

    @Nullable
    public JdbcOperations getJdbcOperations() {
        return this.jdbcOperations;
    }

    public String getPassword() {
        return this.password;
    }

    public String getUrl() {
        return this.url;
    }

    public String getUsername() {
        return this.username;
    }

}
