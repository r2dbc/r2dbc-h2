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
import io.r2dbc.spi.Clob;
import org.h2.value.Value;
import org.h2.value.ValueLob;
import org.h2.value.ValueLobDb;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SynchronousSink;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.io.InputStream;

/**
 * Generic handler for H2 {@link ValueLobDb}/{@link ValueLob}s.
 * <p>
 * NOTE: Both modern and legacy BLOB/CLOB H2 types extend {@link Value#getInputStream()}} virtually, allowing this class to handle both.
 */
abstract class ValueLobHandler<T> {

    private final Value lobDb;

    private SynchronousSink<T> valueLobHandlerSink;

    /**
     * Capture the {@link ValueLobDb}/{@link ValueLob} as a generic {@link Value}, after verifying it's a Lob.
     */
    ValueLobHandler(Value value) {
        if (!(value instanceof ValueLobDb) && !(value instanceof ValueLob)) {
            throw new IllegalStateException("value must be ValueLobDb or ValueLob!");
        }
        this.lobDb = value;
    }

    /**
     * Open the {@link ValueLobDb}/{@link ValueLob}'s {@link InputStream} and pipe the bytes into a {@link Flux}.
     */
    Flux<T> valueLobToFlux() {
        return Flux.<T, InputStream>generate(
            this.lobDb::getInputStream,
            (source, sink) -> {
                this.valueLobHandlerSink = sink;
                try {
                    // Buffer that is divisible by 2, 3, and 5, ensuring any character combo will fit.
                    byte[] data = new byte[1050];
                    int readBytes = source.read(data);

                    // End of the source's data.
                    if (readBytes == -1) {
                        sink.complete();
                        return source;
                    }

                    // Wrap the data buffer in the target type and put it into the Flux
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
     * Transform a raw {@link byte[]} into the relevant {@link Blob}/{@link Clob} type.
     *
     * @param data
     * @param readBytes
     */
    abstract T wrap(byte[] data, int readBytes);

    /**
     * Stop transferring data.
     */
    void cancel() {
        this.valueLobHandlerSink.complete();
    }
}