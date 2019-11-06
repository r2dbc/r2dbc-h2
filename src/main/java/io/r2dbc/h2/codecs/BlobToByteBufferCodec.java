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

import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.ByteBuffer;
import java.util.Enumeration;
import java.util.Iterator;

import io.r2dbc.h2.client.Client;
import io.r2dbc.h2.util.Assert;
import io.r2dbc.spi.Blob;
import org.h2.value.Value;
import org.h2.value.ValueNull;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

final class BlobToByteBufferCodec extends AbstractCodec<ByteBuffer> {

	private final Client client;

	BlobToByteBufferCodec(Client client) {
		super(ByteBuffer.class);
		this.client = client;
	}

	@Override
	boolean doCanDecode(int dataType) {
		return dataType == Value.BLOB;
	}

	@Override
	ByteBuffer doDecode(Value value, Class<? extends ByteBuffer> type) {
		if (value == null || value instanceof ValueNull) {
			return null;
		}

		return new ValueLobBlob(value).valueLobToFlux().blockFirst();
	}

	@Override
	Value doEncode(ByteBuffer value) {
		Assert.requireNonNull(value, "value must not be null");

		Value blob = this.client.getSession().getDataHandler().getLobStorage().createBlob(
			new SequenceInputStream(
				new BlobInputStreamEnumeration(value)), -1);

		this.client.getSession().addTemporaryLob(blob);

		return blob;
	}

	/**
	 * Converts a {@link Flux} of {@link Blob}s into an {@link Enumeration} of {@link InputStream}s.
	 */
	private final class BlobInputStreamEnumeration implements Enumeration<InputStream> {

		private final Iterator<ByteBufferInputStream> inputStreams;

		BlobInputStreamEnumeration(ByteBuffer value) {
			this.inputStreams = Flux.just(value)
				.map(ByteBufferInputStream::new)
				.subscribeOn(Schedulers.elastic())
				.cancelOn(Schedulers.elastic())
				.toIterable()
				.iterator();
		}

		@Override
		public boolean hasMoreElements() {
			return inputStreams.hasNext();
		}

		@Override
		public InputStream nextElement() {
			return inputStreams.next();
		}
	}
}
