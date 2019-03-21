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

import io.r2dbc.h2.codecs.Codecs;
import io.r2dbc.h2.util.Assert;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.h2.message.DbException;
import org.h2.result.ResultInterface;
import org.h2.value.Value;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;
import reactor.util.annotation.Nullable;

import java.util.function.BiFunction;

import static reactor.function.TupleUtils.function;

/**
 * An implementation of {@link Result} representing the results of a query against an H2 database.
 */
public final class H2Result implements Result {

    private final Mono<H2RowMetadata> rowMetadata;

    private final Flux<H2Row> rows;

    private final Mono<Integer> rowsUpdated;

    H2Result(Mono<H2RowMetadata> rowMetadata, Flux<H2Row> rows, Mono<Integer> rowsUpdated) {
        this.rowMetadata = Assert.requireNonNull(rowMetadata, "rowMetadata must not be null");
        this.rows = Assert.requireNonNull(rows, "rows must not be null");
        this.rowsUpdated = Assert.requireNonNull(rowsUpdated, "rowsUpdated must not be null");
    }

    @Override
    public Mono<Integer> getRowsUpdated() {
        return this.rowsUpdated;
    }

    @Override
    public <T> Flux<T> map(BiFunction<Row, RowMetadata, ? extends T> f) {
        Assert.requireNonNull(f, "f must not be null");

        return this.rows
            .zipWith(this.rowMetadata.repeat())
            .map(function(f::apply));
    }

    @Override
    public String toString() {
        return "H2Result{" +
            ", rowMetadata=" + this.rowMetadata +
            ", rows=" + this.rows +
            ", rowsUpdated=" + this.rowsUpdated +
            '}';
    }

    static H2Result toResult(Codecs codecs, @Nullable Integer rowsUpdated) {
        Assert.requireNonNull(codecs, "codecs must not be null");

        return new H2Result(Mono.empty(), Flux.empty(), Mono.justOrEmpty(rowsUpdated));
    }

    static H2Result toResult(Codecs codecs, ResultInterface result, @Nullable Integer rowsUpdated) {
        Assert.requireNonNull(codecs, "codecs must not be null");
        Assert.requireNonNull(result, "result must not be null");

        Mono<H2RowMetadata> rowMetadata = Mono.just(H2RowMetadata.toRowMetadata(codecs, result));

        Flux<H2Row> rows = Flux
            .generate((SynchronousSink<Value[]> sink) -> {
                if (result.next()) {
                    sink.next(result.currentRow());
                } else {
                    result.close();
                    sink.complete();
                }
            })
            .map(values -> H2Row.toRow(values, result, codecs))
            .onErrorMap(DbException.class, H2DatabaseException::new);

        return new H2Result(rowMetadata, rows, Mono.justOrEmpty(rowsUpdated));
    }
}
