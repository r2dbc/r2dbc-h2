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

import io.r2dbc.spi.Row;
import org.h2.value.Value;

/**
 * @author Greg Turnquist
 */
@ToString
@EqualsAndHashCode
public final class H2Row implements Row {

	private final H2RowMetadata rowMetadata;

	private final Value[] value;

	H2Row(H2RowMetadata rowMetadata, Value[] value) {
		
		this.rowMetadata = rowMetadata;
		this.value = value;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T get(Object identifier, Class<T> type) {

		int columnIndex = this.rowMetadata.getColumnMetadata(identifier).getColumnIndex();

		Value value = this.value[columnIndex];

		if (type.isInstance(Integer.class)) {
			return (T) Integer.valueOf(value.getInt());
		}

		return (T) value.getObject();
	}
}
