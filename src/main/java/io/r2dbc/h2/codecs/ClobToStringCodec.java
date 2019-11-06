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

import java.io.CharArrayReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.CharBuffer;
import java.util.Iterator;

import io.r2dbc.h2.client.Client;
import io.r2dbc.h2.util.Assert;
import io.r2dbc.spi.Clob;
import org.h2.value.Value;
import org.h2.value.ValueNull;
import reactor.core.publisher.Flux;

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

		return new ValueLobClob(value).stream()
			.reduce((charSequence, charSequence2) -> charSequence.toString() + charSequence2.toString())
			.block()
			.toString();
	}

	@Override
	Value doEncode(String value) {
		Assert.requireNonNull(value, "value must not be null");

		Value clob = this.client.getSession().getDataHandler().getLobStorage().createClob(
			new AggregateCharArrayReader(value), -1);

		this.client.getSession().addTemporaryLob(clob);

		return clob;
	}

	/**
	 * Converts a {@link Flux} of {@link Clob}s into a {@link Reader} of {@link CharArrayReader}s.
	 */
	private final class AggregateCharArrayReader extends Reader {

		private final Iterator<CharArrayReader> readers;

		private CharArrayReader current;

		private AggregateCharArrayReader(String value) {
			this.readers = Flux.just(value)
				.map(CharBuffer::wrap)
				.map(charBuffer -> {
					if (charBuffer.hasArray()) {
						return charBuffer.array();
					} else {
						return charBuffer.toString().toCharArray();
					}
				})
				.map(CharArrayReader::new)
				.toIterable()
				.iterator();

			if (this.readers.hasNext()) {
				this.current = this.readers.next();
			}
		}

		@Override
		public int read(char[] cbuf, int off, int len) throws IOException {
			int results = this.current.read(cbuf, off, len);

			if (results == -1) {
				if (this.readers.hasNext()) {
					this.current = this.readers.next();
					return read(cbuf, off, len);
				}
			}

			return results;
		}

		@Override
		public void close() {
			this.current.close();
		}
	}
}
