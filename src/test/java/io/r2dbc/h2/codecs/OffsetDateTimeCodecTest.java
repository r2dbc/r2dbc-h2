package io.r2dbc.h2.codecs;

import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.h2.value.ValueTimestampTimeZone;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

final class OffsetDateTimeCodecTest {

    @Test
    void decode() {
        assertThat(new OffsetDateTimeCodec().decode(ValueTimestampTimeZone.parse("2018-10-31 11:59:59+05:00"), OffsetDateTime.class))
                .isEqualTo(OffsetDateTime.of(2018, 10, 31, 11, 59, 59, 0, ZoneOffset.ofHours(5)));
    }

    @Test
    void doCanDecode() {
        OffsetDateTimeCodec codec = new OffsetDateTimeCodec();

        assertThat(codec.doCanDecode(Value.TIMESTAMP_TZ)).isTrue();
        assertThat(codec.doCanDecode(Value.UNKNOWN)).isFalse();
        assertThat(codec.doCanDecode(Value.INT)).isFalse();
    }

    @Test
    void doEncode() {
        assertThat(new OffsetDateTimeCodec().doEncode(OffsetDateTime.of(2018, 10, 31, 11, 59, 59, 0, ZoneOffset.ofHours(5))))
                .isEqualTo(ValueTimestampTimeZone.parse("2018-10-31 11:59:59+05:00"));
    }

    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new OffsetDateTimeCodec().doEncode(null))
                .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        assertThat(new OffsetDateTimeCodec().encodeNull())
                .isEqualTo(ValueNull.INSTANCE);
    }
}