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

import io.r2dbc.spi.Blob;
import org.h2.value.Value;
import org.h2.value.ValueLob;
import org.h2.value.ValueLobDb;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Implement {@link Blob}.
 */
class ValueLobBlob implements Blob {

    private final Value lobDb;

    private SynchronousSink<ByteBuffer> valueLobHandlerSink;

    ValueLobBlob(Value value) {
        this.lobDb = value;
    }

    /**
     * Open the {@link ValueLobDb}/{@link ValueLob}'s {@link InputStream} and pipe the bytes into a {@link Flux}.
     */
    Flux<ByteBuffer> valueLobToFlux() {
        return Flux.<ByteBuffer, InputStream>generate(
            this.lobDb::getInputStream,
            (source, sink) -> {
                this.valueLobHandlerSink = sink;
                try {
                    byte[] data = new byte[1024];
                    int readBytes = source.read(data);

                    // End of the source's data.
                    if (readBytes == -1) {
                        sink.complete();
                        return source;
                    }

                    // Wrap the data buffer into a ByteBuffer of proper length.
                    sink.next(wrap(data, readBytes));
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
            .subscribeOn(Schedulers.elastic())
            .cancelOn(Schedulers.elastic());
    }


    /**
     * Transform a raw {@link byte[]} into a {@link ByteBuffer}.
     *
     * @param data
     * @param readBytes
     */
    ByteBuffer wrap(byte[] data, int readBytes) {
        if (readBytes < data.length) {
            return ByteBuffer.wrap(Arrays.copyOfRange(data, 0, readBytes));
        }

        return ByteBuffer.wrap(data, 0, readBytes);
    }

    @Override
    public Publisher<ByteBuffer> stream() {
        return valueLobToFlux();
    }

    @Override
    public Publisher<Void> discard() {
        return Mono.fromRunnable(() -> this.valueLobHandlerSink.complete()).then();
    }

}
