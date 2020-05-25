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
import org.h2.value.Value;
import org.h2.value.ValueLobInMemory;
import org.h2.value.ValueNull;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.Mockito.mock;

final class BlobToByteBufferCodecTest {

    byte[] TEST_BYTES = "Hello".getBytes();

    @Test
    void decode() {

        ByteBuffer decoded = new BlobToByteBufferCodec(mock(Client.class)).decode(ValueLobInMemory.createSmallLob(Value.BLOB, TEST_BYTES), ByteBuffer.class);
        assertThat(decoded).isEqualTo(ByteBuffer.wrap(TEST_BYTES));
    }

    @Test
    void decodeNull() {
        assertThat(new BlobToByteBufferCodec(mock(Client.class)).doDecode(null, ByteBuffer.class)).isNull();
    }

    @Test
    void doCanDecode() {
        BlobToByteBufferCodec codec = new BlobToByteBufferCodec(mock(Client.class));

        assertThat(codec.doCanDecode(Value.BLOB)).isTrue();
        assertThat(codec.doCanDecode(Value.CLOB)).isFalse();
        assertThat(codec.doCanDecode(Value.INTEGER)).isFalse();
    }

    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> {
            new BlobToByteBufferCodec(mock(Client.class)).doEncode(null);
        }).withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        assertThat(new BlobToByteBufferCodec(mock(Client.class)).encodeNull())
            .isEqualTo(ValueNull.INSTANCE);
    }
}
