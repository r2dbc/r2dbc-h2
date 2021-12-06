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
import org.h2.value.Value;
import org.h2.value.ValueBlob;
import org.h2.value.ValueNull;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.Mockito.mock;

final class BlobCodecTest {

    byte[] TEST_BYTES = "Hello".getBytes();

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
}
