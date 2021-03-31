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
import org.h2.value.ValueInt;
import org.h2.value.ValueJson;
import org.h2.value.ValueNull;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

final class JsonCodecTest {

    @Test
    void decode() {
        assertThat(new JsonCodec().decode(ValueJson.fromJson("{}"), String.class))
            .isEqualTo("{}");
    }

    @Test
    void doCanDecode() {
        JsonCodec codec = new JsonCodec();

        assertThat(codec.doCanDecode(Value.JSON)).isTrue();
        assertThat(codec.doCanDecode(Value.DOUBLE)).isFalse();
    }

    @Test
    void doEncode() {
        assertThat(new JsonCodec().doEncode("{}"))
            .isEqualTo(ValueJson.fromJson("{}"));
    }

    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new JsonCodec().doEncode(null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        assertThat(new JsonCodec().encodeNull())
            .isEqualTo(ValueNull.INSTANCE);
    }
}
