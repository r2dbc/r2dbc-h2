/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.r2dbc.h2.codecs;

import org.h2.value.Value;
import org.h2.value.ValueDate;
import org.h2.value.ValueNull;
import org.junit.jupiter.api.Test;

import java.sql.Date;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

final class DateCodecTest {

    @Test
    void decode() {
        assertThat(new DateCodec().decode(ValueDate.get(Date.valueOf("2018-10-31")), Date.class))
            .isEqualTo(Date.valueOf("2018-10-31"));
    }

    @Test
    void doCanDecode() {
        DateCodec codec = new DateCodec();

        assertThat(codec.doCanDecode(Value.DATE)).isTrue();
        assertThat(codec.doCanDecode(Value.UNKNOWN)).isFalse();
        assertThat(codec.doCanDecode(Value.INT)).isFalse();
    }

    @Test
    void doEncode() {
        assertThat(new DateCodec().doEncode(Date.valueOf("2018-10-31")))
            .isEqualTo(ValueDate.get(Date.valueOf("2018-10-31")));
    }

    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new DateCodec().doEncode(null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        assertThat(new DateCodec().encodeNull())
            .isEqualTo(ValueNull.INSTANCE);
    }
}
