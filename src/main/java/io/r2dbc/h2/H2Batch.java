/*
 * Copyright 2018 the original author or authors.
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

import io.r2dbc.h2.client.Client;
import io.r2dbc.spi.Batch;
import org.h2.message.DbException;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static io.r2dbc.h2.client.Client.SELECT;

/**
 * An implementation of {@link Batch} for executing a collection of statements in a batch against an H2 database.
 */
public final class H2Batch implements Batch {

    private final Client client;

    private final List<String> statements = new ArrayList<>();

    H2Batch(Client client) {
        this.client = Objects.requireNonNull(client, "client must not be null");
    }

    @Override
    public H2Batch add(String sql) {
        Objects.requireNonNull(sql, "sql must not be null");

        this.statements.add(sql);
        return this;
    }

    @Override
    public Flux<H2Result> execute() {
        return Flux.fromIterable(this.statements)
            .flatMap(statement -> {
                if (SELECT.matcher(statement).matches()) {
                    return this.client.query(statement, Collections.emptyList())
                        .map(result -> H2Result.toResult(result, null));
                } else {
                    return this.client.update(statement, Collections.emptyList())
                        .map(result -> H2Result.toResult(result.getGeneratedKeys(), result.getUpdateCount()));
                }
            })
            .onErrorMap(DbException.class, H2DatabaseException::new);
    }

}
