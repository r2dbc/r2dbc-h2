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

package io.r2dbc.h2.client;

import io.r2dbc.h2.H2DatabaseExceptionFactory;
import io.r2dbc.h2.util.Assert;
import org.h2.command.CommandInterface;
import org.h2.engine.ConnectionInfo;
import org.h2.engine.SessionInterface;
import org.h2.engine.SessionRemote;
import org.h2.expression.ParameterInterface;
import org.h2.message.DbException;
import org.h2.result.ResultInterface;
import org.h2.result.ResultWithGeneratedKeys;
import org.h2.value.Value;
import reactor.core.publisher.Mono;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * An implementation of {@link Client} that wraps an H2 {@link SessionInterface}.
 */
public final class SessionClient implements Client {

    private final Logger logger = Loggers.getLogger(this.getClass());

    private final Collection<Binding> emptyBinding = Collections.singleton(Binding.EMPTY);

    private final SessionInterface session;

    private final boolean shutdownDatabaseOnClose;

    /**
     * Creates a new instance.
     *
     * @param connectionInfo the connection info to use
     * @throws NullPointerException if {@code connectionInfo} is {@code null}
     */
    public SessionClient(ConnectionInfo connectionInfo, boolean shutdownDatabaseOnClose) {
        Assert.requireNonNull(connectionInfo, "connectionInfo must not be null");

        this.session = new SessionRemote(connectionInfo).connectEmbeddedOrServer(false);
        this.shutdownDatabaseOnClose = shutdownDatabaseOnClose;
    }

    @Override
    public Mono<Void> close() {
        return Mono.defer(() -> {

            if (this.shutdownDatabaseOnClose) {
                try {
                    CommandInterface shutdown = this.session.prepareCommand("SHUTDOWN", 0);
                    shutdown.executeUpdate(null);
                } catch (DbException e) {
                    return Mono.error(H2DatabaseExceptionFactory.convert(e));
                }
            }
            this.session.close();
            return Mono.empty();
        });
    }

    @Override
    public void disableAutoCommit() {
        this.session.setAutoCommit(false);
    }

    @Override
    public void enableAutoCommit() {
        this.session.setAutoCommit(true);
    }

    @Override
    public boolean inTransaction() {
        return !this.session.getAutoCommit();
    }

    @Override
    public Iterator<CommandInterface> prepareCommand(String sql, List<Binding> bindings) {
        Assert.requireNonNull(sql, "sql must not be null");
        Assert.requireNonNull(bindings, "bindings must not be null");

        Iterator<Binding> bindingIterator = bindings.isEmpty() ? emptyBinding.iterator() : bindings.iterator();

        return new Iterator<CommandInterface>() {

            @Override
            public boolean hasNext() {
                return bindingIterator.hasNext();
            }

            @Override
            public CommandInterface next() {
                Binding binding = bindingIterator.next();

                try {
                    CommandInterface command = createCommand(sql, binding);
                    logger.debug("Request:  {}", command);
                    return command;
                } catch (DbException e) {
                    throw H2DatabaseExceptionFactory.convert(e);
                }
            }
        };
    }

    @Override
    public ResultInterface query(CommandInterface command) {

        try {
            ResultInterface result = command.executeQuery(Integer.MAX_VALUE, false);
            this.logger.debug("Response: {}", result);
            return result;
        } catch (DbException e) {
            throw H2DatabaseExceptionFactory.convert(e);
        }
    }

    @Override
    public ResultWithGeneratedKeys update(CommandInterface command, Object generatedColumns) {
        return command.executeUpdate(generatedColumns);
    }

    /**
     * Return back the current {@link SessionInterface} to the database.
     */
    @Override
    public SessionInterface getSession() {
        return this.session;
    }

    private CommandInterface createCommand(String sql, Binding binding) {
        try {
            CommandInterface command = this.session.prepareCommand(sql, Integer.MAX_VALUE);

            List<? extends ParameterInterface> parameters = command.getParameters();
            for (Map.Entry<Integer, Value> entry : binding.getParameters().entrySet()) {
                parameters.get(entry.getKey()).setValue(entry.getValue(), false);
            }

            return command;
        } catch (DbException e) {
            throw H2DatabaseExceptionFactory.convert(e);
        }
    }
}
