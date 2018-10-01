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

import static reactor.function.TupleUtils.*;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.util.Objects;
import java.util.function.BiFunction;

import io.r2dbc.h2.helper.ValueIterable;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.h2.result.ResultInterface;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author Greg Turnquist
 */
@ToString
@EqualsAndHashCode
public final class H2Result implements Result {

	private final Mono<H2RowMetadata> rowMetadata;

	private final Flux<H2Row> rows;

	private final @Getter Mono<Integer> rowsUpdated;
	
	H2Result(Mono<H2RowMetadata> rowMetadata, Flux<H2Row> rows, Mono<Integer> rowsUpdated) {

		this.rowMetadata = rowMetadata;
		this.rows = rows;
		this.rowsUpdated = rowsUpdated;
	}

	@Override
	public <T> Flux<T> map(BiFunction<Row, RowMetadata, ? extends T> f) {

		Objects.requireNonNull(f, "f must not be null");

		return this.rows
			.zipWith(this.rowMetadata.repeat())
			.map(function((row, rowMetadata) -> {
				try {
					return f.apply(row, rowMetadata);
				} finally {
					// nothing
				}
			}));
	}

	static H2Result toResult(ResultInterface result) {

		Mono<H2RowMetadata> rowMetadata = Mono.just(H2RowMetadata.toRowMetadata(result));

		Flux<H2Row> rows = Flux.fromIterable(new ValueIterable(result))
			.zipWith(rowMetadata.repeat())
			.map(function((row, metadata) -> new H2Row(metadata, row)));

		Mono<Integer> rowsUpdated = Mono.just(result.getRowCount());

		return new H2Result(rowMetadata, rows, rowsUpdated);
	}
}
