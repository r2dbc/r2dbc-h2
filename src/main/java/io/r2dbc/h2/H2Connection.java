/*
 * Copyright 2018 the original author or authors.
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

import io.r2dbc.h2.client.Client;
import io.r2dbc.h2.codecs.Codecs;
import io.r2dbc.h2.util.Assert;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.Option;
import io.r2dbc.spi.TransactionDefinition;
import io.r2dbc.spi.ValidationDepth;
import org.h2.command.CommandInterface;
import org.h2.engine.Constants;
import org.h2.message.DbException;
import org.h2.result.ResultInterface;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.function.Function;

import static io.r2dbc.spi.IsolationLevel.READ_COMMITTED;
import static io.r2dbc.spi.IsolationLevel.READ_UNCOMMITTED;
import static io.r2dbc.spi.IsolationLevel.REPEATABLE_READ;
import static io.r2dbc.spi.IsolationLevel.SERIALIZABLE;
import static org.h2.engine.Constants.LOCK_MODE_OFF;
import static org.h2.engine.Constants.LOCK_MODE_READ_COMMITTED;
import static org.h2.engine.Constants.LOCK_MODE_TABLE;

/**
 * An implementation of {@link Connection} for connecting to an H2 database.
 */
public final class H2Connection implements Connection {

    private final Logger logger = Loggers.getLogger(this.getClass());

    private final Client client;

    private final Codecs codecs;

    private final H2ConnectionMetadata metadata;

    private IsolationLevel isolationLevel;

    H2Connection(Client client, Codecs codecs) {

        this.client = Assert.requireNonNull(client, "client must not be null");
        this.codecs = Assert.requireNonNull(codecs, "codecs must not be null");
        this.isolationLevel = IsolationLevel.READ_COMMITTED;

        String version = Constants.VERSION;
        Iterator<CommandInterface> commands = client.prepareCommand("CALL H2VERSION()", Collections.emptyList());

        if (commands.hasNext()) {

            CommandInterface command = commands.next();
            ResultInterface query = client.query(command);
            query.next();
            version = query.currentRow()[0].getString();
            query.close();
        }

        this.metadata = new H2ConnectionMetadata(version);
    }

    @Override
    public Mono<Void> beginTransaction() {
        return beginTransaction(EmptyTransactionDefinition.INSTANCE);
    }

    @Override
    public Mono<Void> beginTransaction(TransactionDefinition definition) {
        return useTransactionStatus(inTransaction -> {
            if (!inTransaction) {

                IsolationLevel isolationLevel = definition.getAttribute(TransactionDefinition.ISOLATION_LEVEL);
                Boolean readOnly = definition.getAttribute(TransactionDefinition.READ_ONLY);

                Mono<Void> startTransaction = Mono.fromRunnable(this.client::disableAutoCommit);

                if (isolationLevel != null) {
                    startTransaction = startTransaction.then(setTransactionIsolationLevel(isolationLevel));
                }

                if (readOnly != null) {
                    this.logger.debug(TransactionDefinition.READ_ONLY + " isn't supported in H2 at the transaction level. " +
                        "You must set it on the connection URL. See http://www.h2database.com/html/features.html#read_only");
                }

                return startTransaction;

            } else {
                this.logger.debug("Skipping begin transaction because already in one");
                return Mono.empty();
            }
        }).onErrorMap(DbException.class, H2DatabaseExceptionFactory::convert);

    }

    @Override
    public Mono<Void> close() {
        return this.client.close();
    }

    @Override
    public Mono<Void> commitTransaction() {
        return useTransactionStatus(inTransaction -> {
            if (inTransaction) {
                this.client.execute("COMMIT");
                this.client.enableAutoCommit();
            } else {
                this.logger.debug("Skipping commit transaction because no transaction in progress.");
            }

            return Mono.empty();
        })
            .onErrorMap(DbException.class, H2DatabaseExceptionFactory::convert);
    }

    @Override
    public H2Batch createBatch() {
        return new H2Batch(this.client, this.codecs);
    }

    @Override
    public Mono<Void> createSavepoint(String name) {
        Assert.requireNonNull(name, "name must not be null");

        return beginTransaction()
            .then(Mono.<Void>fromRunnable(() -> this.client.execute(String.format("SAVEPOINT %s", name))))
            .onErrorMap(DbException.class, H2DatabaseExceptionFactory::convert);
    }

    @Override
    public H2Statement createStatement(String sql) {
        return new H2Statement(this.client, this.codecs, sql);
    }

    @Override
    public IsolationLevel getTransactionIsolationLevel() {
        return this.isolationLevel;
    }

    @Override
    public H2ConnectionMetadata getMetadata() {
        return this.metadata;
    }

    @Override
    public boolean isAutoCommit() {
        return this.client.getSession().getAutoCommit();
    }

    @Override
    public Mono<Void> releaseSavepoint(String name) {
        Assert.requireNonNull(name, "name must not be null");

        return useTransactionStatus(inTransaction -> {
            if (inTransaction) {
                this.client.execute(String.format("RELEASE SAVEPOINT %s", name));
            } else {
                this.logger.debug("Skipping release savepoint because no transaction in progress.");
            }

            return Mono.empty();
        })
            .onErrorMap(DbException.class, H2DatabaseExceptionFactory::convert);
    }

    @Override
    public Mono<Void> rollbackTransaction() {
        return useTransactionStatus(inTransaction -> {
            if (inTransaction) {
                this.client.execute("ROLLBACK");
                this.client.enableAutoCommit();
            } else {
                this.logger.debug("Skipping rollback because no transaction in progress.");
            }
            return Mono.empty();
        })
            .onErrorMap(DbException.class, H2DatabaseExceptionFactory::convert);
    }

    @Override
    public Mono<Void> rollbackTransactionToSavepoint(String name) {
        Assert.requireNonNull(name, "name must not be null");

        return useTransactionStatus(inTransaction -> {
            if (inTransaction) {
                this.client.execute(String.format("ROLLBACK TO SAVEPOINT %s", name));
            } else {
                this.logger.debug("Skipping rollback to savepoint because no transaction in progress.");
            }

            return Mono.empty();
        })
            .onErrorMap(DbException.class, H2DatabaseExceptionFactory::convert);
    }

    @Override
    public Mono<Void> setAutoCommit(boolean autoCommit) {
        return Mono.fromRunnable(() -> this.client.getSession().setAutoCommit(autoCommit));
    }

    @Override
    public Mono<Void> setLockWaitTimeout(Duration duration) {
        return Mono.empty();
    }

    @Override
    public Mono<Void> setStatementTimeout(Duration duration) {
        return Mono.empty();
    }

    @Override
    public Mono<Void> setTransactionIsolationLevel(IsolationLevel isolationLevel) {
        Assert.requireNonNull(isolationLevel, "isolationLevel must not be null");

        return Mono.<Void>fromRunnable(() -> this.client.execute(getTransactionIsolationLevelQuery(isolationLevel)))
            .doOnSuccess(aVoid -> {
                this.isolationLevel = isolationLevel;
            })
            .onErrorMap(DbException.class, H2DatabaseExceptionFactory::convert);
    }

    /**
     * Validates the connection according to the given {@link ValidationDepth}.
     *
     * @param depth the validation depth
     * @return a {@link Publisher} that indicates whether the validation was successful
     * @throws IllegalArgumentException if {@code depth} is {@code null}
     */
    @Override
    public Mono<Boolean> validate(ValidationDepth depth) {
        Assert.requireNonNull(depth, "depth must not be null");

        return Mono.fromCallable(() -> {
                if (this.client.getSession().isClosed()) {
                    return false;
                }

                this.client.query(this.client.prepareCommand("SELECT CURRENT_TIMESTAMP", Collections.emptyList()).next());

                return true;
            })
            .switchIfEmpty(Mono.just(false));
    }

    private static String getTransactionIsolationLevelQuery(IsolationLevel isolationLevel) {
        if (READ_COMMITTED == isolationLevel) {
            return String.format("SET LOCK_MODE %d", LOCK_MODE_READ_COMMITTED);
        } else if (READ_UNCOMMITTED == isolationLevel) {
            return String.format("SET LOCK_MODE %d", LOCK_MODE_OFF);
        } else if (REPEATABLE_READ == isolationLevel || SERIALIZABLE == isolationLevel) {
            return String.format("SET LOCK_MODE %d", LOCK_MODE_TABLE);
        } else {
            throw new IllegalArgumentException(String.format("Invalid isolation level %s", isolationLevel));
        }
    }

    private Mono<Void> useTransactionStatus(Function<Boolean, Publisher<?>> f) {
        return Flux.defer(() -> f.apply(this.client.inTransaction()))
            .onErrorMap(DbException.class, H2DatabaseExceptionFactory::convert)
            .then();
    }

    private enum EmptyTransactionDefinition implements TransactionDefinition {

        INSTANCE;

        @Override
        public <T> T getAttribute(Option<T> option) {
            return null;
        }
    }
}
