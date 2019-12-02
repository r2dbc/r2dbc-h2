/*
 * Copyright 2019 the original author or authors.
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

package io.r2dbc.h2.codecs;

import io.r2dbc.h2.client.Client;
import io.r2dbc.h2.util.Assert;
import org.h2.value.Value;
import org.h2.value.ValueNull;

import java.io.StringReader;

final class ClobToStringCodec extends AbstractCodec<String> {

	private final Client client;

	ClobToStringCodec(Client client) {
		super(String.class);
		this.client = client;
	}

	@Override
	boolean doCanDecode(int dataType) {
		return dataType == Value.CLOB;
	}

	@Override
	String doDecode(Value value, Class<? extends String> type) {
		if (value == null || value instanceof ValueNull) {
			return null;
		}

		return value.getString();
	}

	@Override
	Value doEncode(String value) {
		Assert.requireNonNull(value, "value must not be null");

		Value clob = this.client.getSession().getDataHandler().getLobStorage()
			.createClob(new StringReader(value), value.length());

		this.client.getSession().addTemporaryLob(clob);

		return clob;
	}
}
