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

import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.util.Optional;

import io.r2dbc.spi.ColumnMetadata;
import org.h2.result.ResultInterface;

/**
 * @author Greg Turnquist
 */
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
@ToString
@EqualsAndHashCode
public class H2ColumnMetadata implements ColumnMetadata {

	private final ResultInterface result;

	private final @Getter int columnIndex;

	@Override
	public String getName() {
		return this.result.getColumnName(this.columnIndex);
	}

	@Override
	public Optional<Integer> getPrecision() {

		return Optional.of(this.result.getColumnPrecision(this.columnIndex))
			.map(Long::intValue);
	}

	@Override
	public Integer getType() {
		return this.result.getColumnType(this.columnIndex);
	}
}
