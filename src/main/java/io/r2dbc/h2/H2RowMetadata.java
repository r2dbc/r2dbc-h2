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

import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.r2dbc.spi.RowMetadata;
import org.h2.result.ResultInterface;

/**
 * @author Greg Turnquist
 */
@ToString
@EqualsAndHashCode
public final class H2RowMetadata implements RowMetadata {

	private final ResultInterface queryResult;
	
	private final List<H2ColumnMetadata> columnMetadata;

	H2RowMetadata(ResultInterface queryResult, List<H2ColumnMetadata> columnMetadata) {

		Objects.requireNonNull(queryResult, "queryResult must not be null");
		Objects.requireNonNull(columnMetadata, "columnMetadata must not be null");

		this.queryResult = queryResult;
		this.columnMetadata = columnMetadata;
	}

	@Override
	public H2ColumnMetadata getColumnMetadata(Object identifier) {

		Objects.requireNonNull(identifier, "identifier must not be null");

		if (identifier instanceof Integer) {
			return getColumnMetadata((Integer) identifier);
		} else if (identifier instanceof String) {
			return getColumnMetadata((String) identifier);
		}

		throw new IllegalArgumentException(String.format("Identifier '%s'"));
	}

	@Override
	public Iterable<H2ColumnMetadata> getColumnMetadatas() {
		return this.columnMetadata;
	}

	static H2RowMetadata toRowMetadata(ResultInterface result) {

		List<H2ColumnMetadata> columnMetadata = IntStream.range(0, result.getVisibleColumnCount())
			.mapToObj(columnIndex -> new H2ColumnMetadata(result, columnIndex))
			.collect(Collectors.toList());

		return new H2RowMetadata(result, columnMetadata);
	}

	private H2ColumnMetadata getColumnMetadata(Integer index) {
		return this.columnMetadata.get(index);
	}

	private H2ColumnMetadata getColumnMetadata(String id) {

		return this.columnMetadata.stream()
			.filter(columnMetadata -> columnMetadata.getName().equalsIgnoreCase(id))
			.findFirst()
			.orElseThrow(() -> new IllegalArgumentException(String.format("Column name '%s' does not exist.", id)));
	}
}
