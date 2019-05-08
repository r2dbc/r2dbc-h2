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
import org.h2.value.Value;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueLobDb;
import org.h2.value.ValueNull;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.Mockito.mock;

final class ClobCodecTest {

    String TEST = "foo你好";
    byte[] TEST_BYTES = TEST.getBytes();

    @Test
    void decode() {
        Flux.from(new ClobCodec(mock(Client.class)).decode(ValueLobDb.createSmallLob(Value.BLOB , TEST_BYTES), Clob.class).stream())
            .as(StepVerifier::create)
            .expectNext(TEST)
            .verifyComplete();
    }

    @Test
    void decodeNull() {
        assertThat(new ClobCodec(mock(Client.class)).doDecode(null, Clob.class)).isNull();
    }

    @Test
    void decodeNonLobType() {
        assertThatIllegalArgumentException().isThrownBy(() -> {
            new ClobCodec(mock(Client.class)).doDecode(ValueBoolean.get(true), Clob.class);
        }).withMessage("value must be ValueLobDb or ValueLob!");
    }

    @Test
    void doCanDecode() {
        ClobCodec codec = new ClobCodec(mock(Client.class));

        assertThat(codec.doCanDecode(Value.CLOB)).isTrue();
        assertThat(codec.doCanDecode(Value.BLOB)).isFalse();
        assertThat(codec.doCanDecode(Value.INT)).isFalse();
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
}
