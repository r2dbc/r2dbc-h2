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
import io.r2dbc.spi.Clob;
import org.h2.engine.Session;
import org.h2.store.DataHandler;
import org.h2.store.LobStorageInterface;
import org.h2.value.Value;
import org.h2.value.ValueClob;
import org.h2.value.ValueNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

final class ClobCodecTest {

    String TEST = "hello你好こんにちはアロハ안녕하세요Здравствуйте";
    byte[] TEST_BYTES = TEST.getBytes(StandardCharsets.UTF_8);

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
        when(session.addTemporaryLob(any(ValueClob.class))).thenAnswer(invocation -> invocation.getArgument(0));
        when(this.lobStorage.createClob(any(Reader.class), anyLong())).thenAnswer(invocation -> {
            Reader reader = invocation.getArgument(0);
            StringBuilder sb = new StringBuilder();
            char[] buf = new char[256];
            int n;
            while ((n = reader.read(buf)) != -1) {
                sb.append(buf, 0, n);
            }
            return ValueClob.createSmall(sb.toString());
        });
    }

    @Test
    void decode() {
        Flux.from(new ClobCodec(mock(Client.class)).decode(ValueClob.createSmall(TEST_BYTES), Clob.class).stream())
            .as(StepVerifier::create)
            .expectNext(TEST)
            .verifyComplete();
    }

    @Test
    void decodeNull() {
        assertThat(new ClobCodec(mock(Client.class)).doDecode(null, Clob.class)).isNull();
    }

    @Test
    void doCanDecode() {
        ClobCodec codec = new ClobCodec(mock(Client.class));

        assertThat(codec.doCanDecode(Value.CLOB)).isTrue();
        assertThat(codec.doCanDecode(Value.BLOB)).isFalse();
        assertThat(codec.doCanDecode(Value.INTEGER)).isFalse();
    }

    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> {
            new ClobCodec(mock(Client.class)).doEncode(null);
        }).withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        assertThat(new ClobCodec(mock(Client.class)).encodeNull())
            .isEqualTo(ValueNull.INSTANCE);
    }

    @Test
    void encodeReactiveDoesNotSubscribeUntilSubscribed() {
        AtomicBoolean subscribed = new AtomicBoolean();
        Clob clob = Clob.from(Flux.defer(() -> {
            subscribed.set(true);
            return Flux.just(TEST);
        }));

        Mono<Value> encoded = new ClobCodec(this.client).encodeReactive(clob);

        assertThat(subscribed).isFalse();

        encoded.as(StepVerifier::create)
            .assertNext(value -> assertThat(value.getString()).isEqualTo(TEST))
            .verifyComplete();

        assertThat(subscribed).isTrue();
    }

    @Test
    void encodeReactiveOnParallelScheduler() {
        Clob clob = Clob.from(Mono.just(TEST));

        Mono.defer(() -> new ClobCodec(this.client).encodeReactive(clob))
            .subscribeOn(Schedulers.parallel())
            .as(StepVerifier::create)
            .assertNext(value -> assertThat(value.getString()).isEqualTo(TEST))
            .verifyComplete();
    }

    @Test
    void encodeReactiveMultiChunk() {
        Clob clob = Clob.from(Flux.just("hello", "你好"));

        new ClobCodec(this.client).encodeReactive(clob)
            .as(StepVerifier::create)
            .assertNext(value -> assertThat(value.getString()).isEqualTo("hello你好"))
            .verifyComplete();
    }
}
