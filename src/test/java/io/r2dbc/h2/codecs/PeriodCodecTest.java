package io.r2dbc.h2.codecs;

import org.h2.api.IntervalQualifier;
import org.h2.value.Value;
import org.h2.value.ValueInterval;
import org.h2.value.ValueNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Period;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

public class PeriodCodecTest {
    private PeriodCodec codec;

    @BeforeEach
    void setUp() {
        codec = new PeriodCodec();
    }

    @Test
    void decode() {
        ValueInterval interval = ValueInterval.from(
                IntervalQualifier.YEAR,
                false,
                Integer.MAX_VALUE,
                0
        );
        Period expected = Period.ofYears(Integer.MAX_VALUE);

        Period decoded = codec.decode(interval, Period.class);
        assertThat(decoded).isEqualTo(expected);
    }

    @Test
    void doCanDecode() {
        assertThat(codec.doCanDecode(Value.INTERVAL_YEAR)).isTrue();
        assertThat(codec.doCanDecode(Value.INTERVAL_MONTH)).isTrue();
        assertThat(codec.doCanDecode(Value.INTERVAL_DAY)).isFalse();
        assertThat(codec.doCanDecode(Value.INTERVAL_HOUR)).isFalse();
        assertThat(codec.doCanDecode(Value.INTERVAL_MINUTE)).isFalse();
        assertThat(codec.doCanDecode(Value.INTERVAL_SECOND)).isFalse();
        assertThat(codec.doCanDecode(Value.INTERVAL_YEAR_TO_MONTH)).isTrue();
        assertThat(codec.doCanDecode(Value.INTERVAL_DAY_TO_HOUR)).isFalse();
        assertThat(codec.doCanDecode(Value.INTERVAL_DAY_TO_MINUTE)).isFalse();
        assertThat(codec.doCanDecode(Value.INTERVAL_DAY_TO_SECOND)).isFalse();
        assertThat(codec.doCanDecode(Value.INTERVAL_HOUR_TO_MINUTE)).isFalse();
        assertThat(codec.doCanDecode(Value.INTERVAL_HOUR_TO_SECOND)).isFalse();
        assertThat(codec.doCanDecode(Value.INTERVAL_MINUTE_TO_SECOND)).isFalse();
        assertThat(codec.doCanDecode(Value.TIMESTAMP_TZ)).isFalse();
        assertThat(codec.doCanDecode(Value.TIMESTAMP)).isFalse();
        assertThat(codec.doCanDecode(Value.UNKNOWN)).isFalse();
        assertThat(codec.doCanDecode(Value.INT)).isFalse();
    }

    @Test
    void doEncode() {
        Period interval = Period.ofYears(Integer.MAX_VALUE);
        ValueInterval expected = ValueInterval.from(
                IntervalQualifier.YEAR,
                false,
                Integer.MAX_VALUE,
                0
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