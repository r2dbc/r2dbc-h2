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
import io.r2dbc.spi.Blob;
import org.h2.value.Value;
import org.h2.value.ValueBlob;
import org.h2.value.ValueNull;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

final class BlobCodec extends AbstractCodec<Blob> {

    private final Client client;

    BlobCodec(Client client) {
        super(Blob.class);
        this.client = client;
    }

    @Override
    boolean doCanDecode(int dataType) {
        return dataType == Value.BLOB;
    }

    @Override
    Blob doDecode(Value value, Class<? extends Blob> type) {
        if (value == null || value instanceof ValueNull) {
            return null;
        }

        return new ValueLobBlob(value);
    }

    @Override
    Value doEncode(Blob value) {
        return encodeReactive(value).block();
    }

    /**
     * Encode a {@link Blob} by materializing its stream reactively.
     * Does not block; consumption is deferred until the returned {@link Mono} is subscribed
     * (typically during statement execution).
     *
     * @param value the blob to encode
     * @return a mono emitting the H2 value
     */
    Mono<Value> encodeReactive(Blob value) {
        Assert.requireNonNull(value, "value must not be null");

        return Flux.from(value.stream())
            .reduceWith(ByteArrayOutputStream::new, BlobCodec::write)
            .map(out -> {
                ValueBlob blob = this.client.getSession().getDataHandler().getLobStorage().createBlob(
                    new ByteArrayInputStream(out.toByteArray()), out.size());

                this.client.getSession().addTemporaryLob(blob);

                return (Value) blob;
            })
            .flatMap(encoded -> Mono.from(value.discard())
                .onErrorResume(e -> Mono.empty())
                .thenReturn(encoded));
    }

    private static ByteArrayOutputStream write(ByteArrayOutputStream out, ByteBuffer buffer) {
        try {
            if (buffer.hasArray()) {
                out.write(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
                buffer.position(buffer.limit());
            } else {
                byte[] bytes = new byte[buffer.remaining()];
                buffer.get(bytes);
                out.write(bytes);
            }
            return out;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to materialize Blob stream", e);
        }
    }

}
