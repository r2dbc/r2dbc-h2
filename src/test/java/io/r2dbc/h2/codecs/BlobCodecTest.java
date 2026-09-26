/*
 * Copyright 2018 the original author or authors.
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
import io.r2dbc.spi.Blob;
import org.h2.engine.Session;
import org.h2.store.DataHandler;
import org.h2.store.LobStorageInterface;
import org.h2.value.Value;
import org.h2.value.ValueBlob;
import org.h2.value.ValueNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

final class BlobCodecTest {

    byte[] TEST_BYTES = "Hello".getBytes();

    private Client client;

    private LobStorageInterface lobStorage;

    @BeforeEach
    void setUp() {
        this.client = mock(Client.class);
        Session session = mock(Session.class);
        DataHandler dataHandler = mock(DataHandler.class);
        this.lobStorage = mock(LobStorageInterface.class);

        when(this.client.getSession()).thenReturn(session);
        when(session.getDataHandler()).thenReturn(dataHandler);
        when(dataHandler.getLobStorage()).thenReturn(this.lobStorage);
        when(session.addTemporaryLob(any(ValueBlob.class))).thenAnswer(invocation -> invocation.getArgument(0));
        when(this.lobStorage.createBlob(any(InputStream.class), anyLong())).thenAnswer(invocation -> {
            InputStream in = invocation.getArgument(0);
            java.io.ByteArrayOutputStream buffer = new java.io.ByteArrayOutputStream();
            byte[] chunk = new byte[256];
            int n;
            while ((n = in.read(chunk)) != -1) {
                buffer.write(chunk, 0, n);
            }
            return ValueBlob.createSmall(buffer.toByteArray());
        });
    }

    @Test
    void decode() {
        Flux.from(new BlobCodec(mock(Client.class)).decode(ValueBlob.createSmall(TEST_BYTES), Blob.class).stream())
            .as(StepVerifier::create)
            .expectNextMatches(byteBuffer -> {
                assertThat(Arrays.copyOfRange(byteBuffer.array(), 0, byteBuffer.remaining())).isEqualTo(TEST_BYTES);
                return true;
            })
            .verifyComplete();
    }

    @Test
    void decodeNull() {
        assertThat(new BlobCodec(mock(Client.class)).doDecode(null, Blob.class)).isNull();
    }

    @Test
    void doCanDecode() {
        BlobCodec codec = new BlobCodec(mock(Client.class));

        assertThat(codec.doCanDecode(Value.BLOB)).isTrue();
        assertThat(codec.doCanDecode(Value.CLOB)).isFalse();
        assertThat(codec.doCanDecode(Value.INTEGER)).isFalse();
    }

    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> {
            new BlobCodec(mock(Client.class)).doEncode(null);
        }).withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        assertThat(new BlobCodec(mock(Client.class)).encodeNull())
            .isEqualTo(ValueNull.INSTANCE);
    }

    @Test
    void encodeReactiveDoesNotSubscribeUntilSubscribed() {
        AtomicBoolean subscribed = new AtomicBoolean();
        Blob blob = Blob.from(Flux.defer(() -> {
            subscribed.set(true);
            return Flux.just(ByteBuffer.wrap(TEST_BYTES));
        }));

        Mono<Value> encoded = new BlobCodec(this.client).encodeReactive(blob);

        assertThat(subscribed).isFalse();

        encoded.as(StepVerifier::create)
            .assertNext(value -> assertThat(value.getBytesNoCopy()).isEqualTo(TEST_BYTES))
            .verifyComplete();

        assertThat(subscribed).isTrue();
    }

    @Test
    void encodeReactiveOnParallelScheduler() {
        Blob blob = Blob.from(Mono.just(ByteBuffer.wrap(TEST_BYTES)));

        Mono.defer(() -> new BlobCodec(this.client).encodeReactive(blob))
            .subscribeOn(Schedulers.parallel())
            .as(StepVerifier::create)
            .assertNext(value -> assertThat(value.getBytesNoCopy()).isEqualTo(TEST_BYTES))
            .verifyComplete();
    }

    @Test
    void encodeReactiveMultiChunk() {
        byte[] part1 = "Hel".getBytes();
        byte[] part2 = "lo".getBytes();
        Blob blob = Blob.from(Flux.just(ByteBuffer.wrap(part1), ByteBuffer.wrap(part2)));

        new BlobCodec(this.client).encodeReactive(blob)
            .as(StepVerifier::create)
            .assertNext(value -> assertThat(value.getBytesNoCopy()).isEqualTo(TEST_BYTES))
            .verifyComplete();
    }
}
