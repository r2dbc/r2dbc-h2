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
import org.h2.value.ValueBoolean;
import org.h2.value.ValueNull;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

final class BooleanCodecTest {

    @Test
    void decode() {
        assertThat(new BooleanCodec().decode(ValueBoolean.get(false), Boolean.class))
            .isEqualTo(false);
    }

    @Test
    void doCanDecode() {
        BooleanCodec codec = new BooleanCodec();

        assertThat(codec.doCanDecode(Value.BOOLEAN)).isTrue();
        assertThat(codec.doCanDecode(Value.INT)).isFalse();
    }

    @Test
    void doEncode() {
        assertThat(new BooleanCodec().doEncode(true))
            .isEqualTo(ValueBoolean.get(true));
    }

    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new BooleanCodec().doEncode(null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        assertThat(new BooleanCodec().encodeNull())
            .isEqualTo(ValueNull.INSTANCE);
    }
}
