/*
 * Copyright 2018-2019 the original author or authors.
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

import org.h2.api.Interval;
import org.h2.api.IntervalQualifier;
import org.h2.value.Value;
import org.h2.value.ValueInterval;
import org.h2.value.ValueNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

final class IntervalCodecTest {

    private IntervalCodec codec;

    @BeforeEach
    void setUp() {
        codec = new IntervalCodec();
    }

    @Test
    void decode() {
        ValueInterval interval = ValueInterval.from(
            IntervalQualifier.DAY_TO_SECOND,
            false,
            999_999_999_999_999_999L,
            24 * 60 * 60 * 1_000_000_000L - 1
        );
        Interval expected = Interval.ofDaysHoursMinutesNanos(999_999_999_999_999_999L, 23, 59, 59_999_999_999L);
        Interval decoded = codec.decode(interval, Interval.class);
        assertThat(decoded).isEqualTo(expected);
    }

    @Test
    void doCanDecode() {
        assertThat(codec.doCanDecode(Value.INTERVAL_YEAR)).isTrue();
        assertThat(codec.doCanDecode(Value.INTERVAL_MONTH)).isTrue();
        assertThat(codec.doCanDecode(Value.INTERVAL_DAY)).isTrue();
        assertThat(codec.doCanDecode(Value.INTERVAL_HOUR)).isTrue();
        assertThat(codec.doCanDecode(Value.INTERVAL_MINUTE)).isTrue();
        assertThat(codec.doCanDecode(Value.INTERVAL_SECOND)).isTrue();
        assertThat(codec.doCanDecode(Value.INTERVAL_YEAR_TO_MONTH)).isTrue();
        assertThat(codec.doCanDecode(Value.INTERVAL_DAY_TO_HOUR)).isTrue();
        assertThat(codec.doCanDecode(Value.INTERVAL_DAY_TO_MINUTE)).isTrue();
        assertThat(codec.doCanDecode(Value.INTERVAL_DAY_TO_SECOND)).isTrue();
        assertThat(codec.doCanDecode(Value.INTERVAL_HOUR_TO_MINUTE)).isTrue();
        assertThat(codec.doCanDecode(Value.INTERVAL_HOUR_TO_SECOND)).isTrue();
        assertThat(codec.doCanDecode(Value.INTERVAL_MINUTE_TO_SECOND)).isTrue();
        assertThat(codec.doCanDecode(Value.TIMESTAMP_TZ)).isFalse();
        assertThat(codec.doCanDecode(Value.TIMESTAMP)).isFalse();
        assertThat(codec.doCanDecode(Value.UNKNOWN)).isFalse();
        assertThat(codec.doCanDecode(Value.INT)).isFalse();
    }

    @Test
    void doEncode() {
        Interval interval = Interval.ofDaysHoursMinutesNanos(999_999_999_999_999_999L, 23, 59, 59_999_999_999L);
        ValueInterval expected = ValueInterval.from(
            IntervalQualifier.DAY_TO_SECOND,
            false,
            999_999_999_999_999_999L,
            24 * 60 * 60 * 1_000_000_000L - 1
        );
        Value encoded = codec.doEncode(interval);
        assertThat(encoded).isEqualTo(expected);
    }

    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> codec.doEncode(null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        assertThat(codec.encodeNull()).isEqualTo(ValueNull.INSTANCE);
    }
}
