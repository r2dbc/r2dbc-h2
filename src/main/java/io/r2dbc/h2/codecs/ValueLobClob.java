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

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

import io.r2dbc.spi.Clob;
import org.h2.value.Value;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;
import reactor.core.scheduler.Schedulers;

/**
 * Implement {@link Clob}.
 */
class ValueLobClob implements Clob {

	private static final Charset ENCODING = Charset.forName("UTF-8");

	private final Value lobDb;

	private SynchronousSink<CharSequence> valueLobHandlerSink;

	ValueLobClob(Value value) {
		this.lobDb = value;
	}

	@Override
	public Flux<CharSequence> stream() {
		return Flux.<CharSequence, InputStreamReader> generate(
			() -> new InputStreamReader(this.lobDb.getInputStream(), ENCODING),
			(source, sink) -> {
				this.valueLobHandlerSink = sink;
				try {
					char[] data = new char[256];
					int readBytes = source.read(data);

					// End of the source's data.
					if (readBytes == -1) {
						sink.complete();
						return source;
					}

					// Wrap the data buffer in the target type and put it into the Flux
					sink.next(new String(data, 0, readBytes));
				} catch (IOException e) {
					sink.error(e);
				}

				return source;
			},
			source -> {
				// When the Flux is terminated or cancelled
				try {
					source.close();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			})
			.subscribeOn(Schedulers.boundedElastic())
			.cancelOn(Schedulers.boundedElastic());
	}

	@Override
	public Mono<Void> discard() {
		return Mono.fromRunnable(() -> this.valueLobHandlerSink.complete()).then();
	}
}
