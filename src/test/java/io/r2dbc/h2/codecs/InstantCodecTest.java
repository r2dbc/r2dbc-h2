package io.r2dbc.h2.codecs;

import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.h2.value.ValueTimestampTimeZone;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class InstantCodecTest {

    @Test
    void decode() {
        assertThat(new InstantCodec().decode(ValueTimestampTimeZone.parse("2018-10-31 11:59:59+00:00"), Instant.class))
                .isEqualTo(Instant.parse("2018-10-31T11:59:59Z"));
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
        assertThat(new InstantCodec().encode(Instant.parse("2018-10-31T11:59:59Z")))
                .isEqualTo(ValueTimestampTimeZone.parse("2018-10-31 11:59:59+00:00"));
    }

    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new InstantCodec().doEncode(null))
                .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        assertThat(new InstantCodec().encodeNull())
                .isEqualTo(ValueNull.INSTANCE);
    }
}