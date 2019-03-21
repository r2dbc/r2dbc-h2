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
import org.h2.value.ValueNull;
import org.h2.value.ValueTimestamp;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

final class TimestampCodecTest {

    @Test
    void decode() {
        assertThat(new TimestampCodec().decode(ValueTimestamp.get(Timestamp.valueOf("2018-10-31 11:59:59")), Timestamp.class))
            .isEqualTo(Timestamp.valueOf("2018-10-31 11:59:59"));
    }

    @Test
    void doCanDecode() {
        TimestampCodec codec = new TimestampCodec();

        assertThat(codec.doCanDecode(Value.TIMESTAMP)).isTrue();
        assertThat(codec.doCanDecode(Value.UNKNOWN)).isFalse();
        assertThat(codec.doCanDecode(Value.INT)).isFalse();
    }

    @Test
    void doEncode() {
        assertThat(new TimestampCodec().doEncode(Timestamp.valueOf("2018-10-31 11:59:59")))
            .isEqualTo(ValueTimestamp.get(Timestamp.valueOf("2018-10-31 11:59:59")));
    }

    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new TimestampCodec().doEncode(null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        assertThat(new TimestampCodec().encodeNull())
            .isEqualTo(ValueNull.INSTANCE);
    }
}
