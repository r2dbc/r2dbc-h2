/*
 * Copyright 2017-2019 the original author or authors.
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

package io.r2dbc.h2.client;

import io.r2dbc.h2.util.Assert;
import org.h2.result.ResultInterface;
import org.h2.result.ResultWithGeneratedKeys;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import static java.util.regex.Pattern.CASE_INSENSITIVE;
import static java.util.regex.Pattern.DOTALL;


/**
 * An abstraction that wraps interaction with the H2 Database APIs.
 */
public interface Client {

    Pattern INSERT = Pattern.compile("[\\s]*INSERT.*", CASE_INSENSITIVE | DOTALL);

    Pattern SELECT = Pattern.compile("[\\s]*SELECT.*", CASE_INSENSITIVE | DOTALL);

    /**
     * Release any resources held by the {@link Client}.
     *
     * @return a {@link Mono} that termination is complete
     */
    Mono<Void> close();

    /**
     * Disable auto-commit.  Typically used at the beginning of a transaction.
     *
     * @return a {@link Mono} that disable is complete
     */
    Mono<Void> disableAutoCommit();

    /**
     * Enables auto-commit.  Typically used at the end of a transaction, either success or failure.
     *
     * @return a {@link Mono} that enable is complete
     */
    Mono<Void> enableAutoCommit();

    /**
     * Execute a command, discarding any results.
     *
     * @param sql the SQL of the command
     * @return a {@link Mono} that the command is complete
     * @throws NullPointerException if {@code sql} is {@code null}
     */
    default Mono<Void> execute(String sql) {
        Assert.requireNonNull(sql, "sql must not be null");

        return update(sql, Collections.emptyList())
            .then();
    }

    /**
     * Whether the {@link Client} is currently in a transaction.
     *
     * @return {@code true} if the {@link Client} is currently in a transaction, {@code false} otherwise.
     */
    boolean inTransaction();

    /**
     * Execute a query.
     *
     * @param sql      the SQL of the query
     * @param bindings the parameter bindings to use
     * @return the result of the query
     * @throws NullPointerException if {@code sql} or {@code bindings} is {@code null}
     */
    Flux<ResultInterface> query(String sql, List<Binding> bindings);

    /**
     * Execute an update.
     *
     * @param sql      the SQL of the update
     * @param bindings the parameter bindings to use
     * @return the result of the update
     * @throws NullPointerException if {@code sql} or {@code bindings} is {@code null}
     */
    Flux<ResultWithGeneratedKeys> update(String sql, List<Binding> bindings);

}
