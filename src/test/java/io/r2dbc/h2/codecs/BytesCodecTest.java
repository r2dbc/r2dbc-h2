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

import org.h2.value.Value;
import org.h2.value.ValueBytes;
import org.h2.value.ValueNull;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

final class BytesCodecTest {

    byte[] TEST_BYTE = "Hello".getBytes();

    @Test
    void decode() {
        assertThat(new BytesCodec().decode(ValueBytes.get(TEST_BYTE), byte[].class))
            .isEqualTo(TEST_BYTE);
    }

    @Test
    void doCanDecode() {
        BytesCodec codec = new BytesCodec();

        assertThat(codec.doCanDecode(Value.BYTES)).isTrue();
        assertThat(codec.doCanDecode(Value.INT)).isFalse();
    }

    @Test
    void doEncode() {
        assertThat(new BytesCodec().doEncode(TEST_BYTE))
            .isEqualTo(ValueBytes.get(TEST_BYTE));
    }

    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new BytesCodec().doEncode(null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        assertThat(new BytesCodec().encodeNull())
            .isEqualTo(ValueNull.INSTANCE);
    }
}
