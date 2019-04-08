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
import org.h2.value.ValueLobDb;
import org.h2.value.ValueNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

final class BlobCodecTest {

    ValueLobDb TEST_BLOB;

    @BeforeEach
    void setUp() {
        TEST_BLOB = ValueLobDb.createSmallLob(Value.BLOB, new byte[]{1, 2, 3, 4});
    }

    @Test
    void decode() {
        Mono.from(new BlobCodec().decode(TEST_BLOB, Blob.class)
            .stream())
            .as(StepVerifier::create)
            .expectNextMatches(byteBuffer -> {
                byte[] bytes = byteBuffer.array();

                assertThat(bytes[0]).isEqualTo(Byte.parseByte("1"));
                assertThat(bytes[1]).isEqualTo(Byte.parseByte("2"));
                assertThat(bytes[2]).isEqualTo(Byte.parseByte("3"));
                assertThat(bytes[3]).isEqualTo(Byte.parseByte("4"));

                return true;
            })
            .verifyComplete();
    }

    @Test
    void doCanDecode() {
        BlobCodec codec = new BlobCodec();

        assertThat(codec.doCanDecode(Value.BLOB)).isTrue();
        assertThat(codec.doCanDecode(Value.CLOB)).isFalse();
    }

    @Test
    void doEncode() {
        byte[] bytes = new BlobCodec().doEncode(Blob.from(Mono.just(ByteBuffer.wrap(new byte[]{1, 2, 3, 4})))).getSmall();

        assertThat(bytes[0]).isEqualTo(Byte.parseByte("48"));
        assertThat(bytes[1]).isEqualTo(Byte.parseByte("49"));
        assertThat(bytes[2]).isEqualTo(Byte.parseByte("48"));
        assertThat(bytes[3]).isEqualTo(Byte.parseByte("50"));
        assertThat(bytes[4]).isEqualTo(Byte.parseByte("48"));
        assertThat(bytes[5]).isEqualTo(Byte.parseByte("51"));
        assertThat(bytes[6]).isEqualTo(Byte.parseByte("48"));
        assertThat(bytes[7]).isEqualTo(Byte.parseByte("52"));
    }

    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new BlobCodec().doEncode(null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        assertThat(new BlobCodec().encodeNull())
            .isEqualTo(ValueNull.INSTANCE);
    }

}
