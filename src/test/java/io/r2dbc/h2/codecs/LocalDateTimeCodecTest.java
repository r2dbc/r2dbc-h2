package io.r2dbc.h2.codecs;

import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.h2.value.ValueTimestamp;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

final class LocalDateTimeCodecTest {

    @Test
    void decode() {
        assertThat(new LocalDateTimeCodec().decode(ValueTimestamp.get(Timestamp.valueOf("2018-10-31 11:59:59")), LocalDateTime.class))
                .isEqualTo(LocalDateTime.of(2018, 10, 31, 11, 59, 59));
    }

    @Test
    void doCanDecode() {
        LocalDateTimeCodec codec = new LocalDateTimeCodec();

        assertThat(codec.doCanDecode(Value.TIMESTAMP)).isTrue();
        assertThat(codec.doCanDecode(Value.UNKNOWN)).isFalse();
        assertThat(codec.doCanDecode(Value.INT)).isFalse();
    }

    @Test
    void doEncode() {
        assertThat(new LocalDateTimeCodec().doEncode(LocalDateTime.of(2018, 10, 31, 11, 59, 59)))
                .isEqualTo(ValueTimestamp.get(Timestamp.valueOf("2018-10-31 11:59:59")));
    }

    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new LocalDateTimeCodec().doEncode(null))
                .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        assertThat(new LocalDateTimeCodec().encodeNull())
                .isEqualTo(ValueNull.INSTANCE);
    }
}