package io.r2dbc.h2.codecs;

import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.h2.value.ValueTime;
import org.junit.jupiter.api.Test;

import java.time.LocalTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

final class LocalTimeCodecTest {

    @Test
    void decode() {
        assertThat(new LocalTimeCodec().decode(ValueTime.parse("11:59:59"), LocalTime.class))
                .isEqualTo(LocalTime.of(11, 59, 59));
    }

    @Test
    void doCanDecode() {
        LocalTimeCodec codec = new LocalTimeCodec();

        assertThat(codec.doCanDecode(Value.TIME)).isTrue();
        assertThat(codec.doCanDecode(Value.UNKNOWN)).isFalse();
        assertThat(codec.doCanDecode(Value.INTEGER)).isFalse();
    }

    @Test
    void doEncode() {
        assertThat(new LocalTimeCodec().doEncode(LocalTime.of(11, 59, 59)))
                .isEqualTo(ValueTime.parse("11:59:59"));
    }

    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new LocalTimeCodec().doEncode(null))
                .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        assertThat(new LocalTimeCodec().encodeNull())
                .isEqualTo(ValueNull.INSTANCE);
    }
}
