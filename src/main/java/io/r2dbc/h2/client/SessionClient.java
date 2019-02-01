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

package io.r2dbc.h2.client;

import io.r2dbc.h2.util.Assert;
import org.h2.command.CommandInterface;
import org.h2.engine.ConnectionInfo;
import org.h2.engine.SessionInterface;
import org.h2.engine.SessionRemote;
import org.h2.expression.ParameterInterface;
import org.h2.result.ResultInterface;
import org.h2.result.ResultWithGeneratedKeys;
import org.h2.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;

/**
 * An implementation of {@link Client} that wraps an H2 {@link SessionInterface}.
 */
public final class SessionClient implements Client {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final SessionInterface session;

    /**
     * Creates a new instance.
     *
     * @param connectionInfo the connection info to use
     * @throws NullPointerException if {@code connectionInfo} is {@code null}
     */
    public SessionClient(ConnectionInfo connectionInfo) {
        Assert.requireNonNull(connectionInfo, "connectionInfo must not be null");

        this.session = new SessionRemote(connectionInfo).connectEmbeddedOrServer(false);
    }

    @Override
    public Mono<Void> close() {
        return Mono.defer(() -> {
            this.session.close();
            return Mono.empty();
        });
    }

    @Override
    public Mono<Void> disableAutoCommit() {
        return Mono.defer(() -> {
            this.session.setAutoCommit(false);
            return Mono.empty();
        });
    }

    @Override
    public Mono<Void> enableAutoCommit() {
        return Mono.defer(() -> {
            this.session.setAutoCommit(true);
            return Mono.empty();
        });
    }

    @Override
    public boolean inTransaction() {
        return !this.session.getAutoCommit();
    }

    @Override
    public Flux<ResultInterface> query(String sql, List<Binding> bindings) {
        Assert.requireNonNull(sql, "sql must not be null");
        Assert.requireNonNull(bindings, "bindings must not be null");

        return Flux.fromIterable(bindings)
            .defaultIfEmpty(Binding.EMPTY)
            .map(binding -> createCommand(sql, binding))
            .doOnNext(command -> this.logger.debug("Request:  {}", command))
            .flatMap(command -> Mono.just(command.executeQuery(Integer.MAX_VALUE, false)))
            .doOnNext(result -> this.logger.debug("Response: {}", result));
    }

    @Override
    public Flux<ResultWithGeneratedKeys> update(String sql, List<Binding> bindings, Object generatedColumns) {
        Assert.requireNonNull(sql, "sql must not be null");
        Assert.requireNonNull(bindings, "bindings must not be null");

        return Flux.fromIterable(bindings)
            .defaultIfEmpty(Binding.EMPTY)
            .map(binding -> createCommand(sql, binding))
            .doOnNext(command -> this.logger.debug("Request: {}", command))
            .flatMap(command -> Mono.just(command.executeUpdate(generatedColumns)));
    }

    private CommandInterface createCommand(String sql, Binding binding) {
        CommandInterface command = this.session.prepareCommand(sql, Integer.MAX_VALUE);

        List<? extends ParameterInterface> parameters = command.getParameters();
        for (Map.Entry<Integer, Value> entry : binding.getParameters().entrySet()) {
            parameters.get(entry.getKey()).setValue(entry.getValue(), false);
        }

        return command;
    }
}
