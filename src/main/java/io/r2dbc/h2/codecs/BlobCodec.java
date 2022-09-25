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
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.Pipe;

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
        Assert.requireNonNull(value, "value must not be null");

        ValueBlob blob = this.client.getSession().getDataHandler().getLobStorage().createBlob(
            getInputStreamFromBlob(value), -1);

        this.client.getSession().addTemporaryLob(blob);

        return blob;
    }

    /**
     * Converts a {@link Blob} into an {@link InputStream} using a {@link Pipe}.
     */
    private InputStream getInputStreamFromBlob(Blob value) {
        Pipe pipe;
        try {
            pipe = Pipe.open();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Flux.from(value.stream())
            .subscribeOn(Schedulers.boundedElastic())
            .cancelOn(Schedulers.boundedElastic())
            .concatMap(buffer -> {
                try {
                    pipe.sink().write(buffer);
                    return Mono.empty();
                } catch (IOException e) {
                    return Mono.error(e);
                }
            })
            .doOnError(t -> {
                // causes an AsynchronousCloseException on the source side
                try {
                    pipe.source().close();
                } catch (IOException e) {
                    t.addSuppressed(e);
                }
            })
            .doOnTerminate(() -> {
                // causes a valid EOF signal on the source side
                try {
                    pipe.sink().close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            })
            .subscribe();

        return Channels.newInputStream(pipe.source());
    }

}
