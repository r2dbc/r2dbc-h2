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
import io.r2dbc.spi.Batch;
import org.h2.message.DbException;
import org.h2.result.ResultInterface;
import org.h2.result.ResultWithGeneratedKeys;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * An implementation of {@link Batch} for executing a collection of statements in a batch against an H2 database.
 */
public final class H2Batch implements Batch {

    private final Client client;

    private final Codecs codecs;

    private final List<String> statements = new ArrayList<>();

    H2Batch(Client client, Codecs codecs) {
        this.client = Assert.requireNonNull(client, "client must not be null");
        this.codecs = Assert.requireNonNull(codecs, "codecs must not be null");
    }

    @Override
    public H2Batch add(String sql) {
        Assert.requireNonNull(sql, "sql must not be null");

        this.statements.add(sql);
        return this;
    }

    @Override
    public Flux<H2Result> execute() {
        return Flux.fromIterable(this.statements)
            .flatMapIterable(statement -> () -> this.client.prepareCommand(statement, Collections.emptyList()))
            .map(command -> {
                try {
                    if (command.isQuery()) {

                        ResultInterface result = this.client.query(command);
                        CommandUtil.clearForReuse(command);
                        return H2Result.toResult(this.codecs, result, null);
                    } else {

                        ResultWithGeneratedKeys result = this.client.update(command, false);
                        CommandUtil.clearForReuse(command);
                        int updatedCountInt = Long.valueOf(result.getUpdateCount()).intValue();
                        return H2Result.toResult(this.codecs, updatedCountInt);
                    }
                } catch (DbException e) {
                    throw H2DatabaseExceptionFactory.convert(e);
                }
            });
    }

}
