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
import org.h2.value.ValueNull;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * @author Greg Turnquist
 */
final class DefaultCodecsTest {

    @Test
    void decode() {
        assertThat(new DefaultCodecs().decode(ValueInt.get(100), ValueInt.INT, Integer.class))
            .isEqualTo(100);
    }

    @Test
    void decodeDefaultType() {
        assertThat(new DefaultCodecs().decode(ValueInt.get(100), ValueInt.INT, Object.class))
            .isEqualTo(100);
    }

    @Test
    void decodeNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new DefaultCodecs().decode(ValueInt.get(100), ValueInt.INT, null))
            .withMessage("type must not be null");
    }

    @Test
    void decodeNull() {
        assertThat(new DefaultCodecs().decode(null, ValueInt.INT, Integer.class))
            .isNull();
    }

    @Test
    void decodeUnsupportedType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new DefaultCodecs().decode(ValueInt.get(100), ValueInt.INT, Void.class))
            .withMessage("Cannot decode value of type java.lang.Void");
    }

    @Test
    void encode() {
        Value parameter = new DefaultCodecs().encode(100);

        assertThat(parameter).isEqualTo(ValueInt.get(100));
    }

    @Test
    void encodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new DefaultCodecs().encode(null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        Value parameter = new DefaultCodecs().encodeNull(Integer.class);

        assertThat(parameter).isEqualTo(ValueNull.INSTANCE);
    }

    @Test
    void encodeNullNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new DefaultCodecs().encodeNull(null))
            .withMessage("type must not be null");
    }

    @Test
    void encodeNullUnsupportedType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new DefaultCodecs().encodeNull(Object.class))
            .withMessage("Cannot encode null parameter of type java.lang.Object");
    }

    @Test
    void encodeUnsupportedType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new DefaultCodecs().encode(new Object()))
            .withMessage("Cannot encode parameter of type java.lang.Object");
    }

}
