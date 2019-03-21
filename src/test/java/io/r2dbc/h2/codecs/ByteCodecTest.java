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
import org.h2.value.ValueByte;
import org.h2.value.ValueNull;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

final class ByteCodecTest {

    byte TEST_BYTE = Byte.parseByte("1111", 2);

    @Test
    void decode() {
        assertThat(new ByteCodec().decode(ValueByte.get(TEST_BYTE), Byte.class))
            .isEqualTo(TEST_BYTE);
    }

    @Test
    void doCanDecode() {
        ByteCodec codec = new ByteCodec();

        assertThat(codec.doCanDecode(Value.BYTE)).isTrue();
        assertThat(codec.doCanDecode(Value.INT)).isFalse();
    }

    @Test
    void doEncode() {
        assertThat(new ByteCodec().doEncode(TEST_BYTE))
            .isEqualTo(ValueByte.get(TEST_BYTE));
    }

    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new ByteCodec().doEncode(null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        assertThat(new ByteCodec().encodeNull())
            .isEqualTo(ValueNull.INSTANCE);
    }
}
