package io.r2dbc.h2.codecs;

import org.h2.value.Value;
import org.h2.value.ValueDate;
import org.h2.value.ValueNull;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;

import static io.r2dbc.h2.codecs.LocalDateCodec.transform;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

final class LocalDateCodecTest {

    @Test
    void decode() {
        assertThat(new LocalDateCodec().decode(ValueDate.get(transform(LocalDate.of(2018, 10, 31))), LocalDate.class))
                .isEqualTo(LocalDate.of(2018, 10, 31));
    }

    @Test
    void doCanDecode() {
        LocalDateCodec codec = new LocalDateCodec();

        assertThat(codec.doCanDecode(Value.DATE)).isTrue();
        assertThat(codec.doCanDecode(Value.UNKNOWN)).isFalse();
        assertThat(codec.doCanDecode(Value.INT)).isFalse();
    }

    @Test
    void doEncode() {
        assertThat(new LocalDateCodec().doEncode(LocalDate.of(2018, 10, 31)))
                .isEqualTo(ValueDate.get(transform(LocalDate.of(2018, 10, 31))));
    }

    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new LocalDateCodec().doEncode(null))
                .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        assertThat(new LocalDateCodec().encodeNull())
                .isEqualTo(ValueNull.INSTANCE);
    }
}