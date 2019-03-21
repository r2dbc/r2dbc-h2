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

package io.r2dbc.h2.client;

import io.r2dbc.h2.util.Assert;
import org.h2.command.CommandInterface;
import org.h2.result.ResultInterface;
import org.h2.result.ResultWithGeneratedKeys;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.List;


/**
 * An abstraction that wraps interaction with the H2 Database APIs.
 */
public interface Client {

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

        return prepareCommand(sql, Collections.emptyList())
            .flatMap(command -> update(command, false))
            .then();
    }

    /**
     * Whether the {@link Client} is currently in a transaction.
     *
     * @return {@code true} if the {@link Client} is currently in a transaction, {@code false} otherwise.
     */
    boolean inTransaction();

    /**
     * Transform a SQL statement and a set of {@link Binding}s into a {@link CommandInterface}.
     *
     * @param sql to either query or update
     * @param bindings the parameter bindings to use
     * @return {@link CommandInterface} to be flat mapped over
     */
    Flux<CommandInterface> prepareCommand(String sql, List<Binding> bindings);

    /**
     * Execute a query.
     *
     * @param command the {@link CommandInterface} to query
     * @return the result of the query
     * @throws NullPointerException if {@code sql} or {@code bindings} is {@code null}
     */
    Mono<ResultInterface> query(CommandInterface command);

    /**
     * Execute an update.
     *
     * @param command      the {@link CommandInterface} to update
     * @param generatedColumns the parameter to specify what columns to generate
     * @return the result of the update
     * @throws NullPointerException if {@code sql} or {@code bindings} is {@code null}
     */
    Mono<ResultWithGeneratedKeys> update(CommandInterface command, Object generatedColumns);

}
