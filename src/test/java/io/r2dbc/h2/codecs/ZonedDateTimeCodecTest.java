package io.r2dbc.h2.codecs;

import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.h2.value.ValueTimestampTimeZone;
import org.junit.jupiter.api.Test;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

final class ZonedDateTimeCodecTest {

    @Test
    void decode() {
        assertThat(new ZonedDateTimeCodec().doDecode(ValueTimestampTimeZone.parse("2018-10-31 11:59:59+05:00"), ZonedDateTime.class))
                .isEqualTo(ZonedDateTime.of(2018, 10, 31, 11, 59, 59, 0, ZoneOffset.ofHours(5)));
    }

    @Test
    void doCanDecode() {
        ZonedDateTimeCodec codec = new ZonedDateTimeCodec();

        assertThat(codec.doCanDecode(Value.TIMESTAMP_TZ)).isTrue();
        assertThat(codec.doCanDecode(Value.UNKNOWN)).isFalse();
        assertThat(codec.doCanDecode(Value.INT)).isFalse();
    }

    @Test
    void doEncode() {
        assertThat(new ZonedDateTimeCodec().doEncode(ZonedDateTime.of(2018, 10, 31, 11, 59, 59, 0, ZoneOffset.ofHours(5))))
                .isEqualTo(ValueTimestampTimeZone.parse("2018-10-31 11:59:59+05:00"));
    }

    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new ZonedDateTimeCodec().doEncode(null))
                .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        assertThat(new ZonedDateTimeCodec().encodeNull())
                .isEqualTo(ValueNull.INSTANCE);
    }
}