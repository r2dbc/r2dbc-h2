package io.r2dbc.h2.codecs;

import org.h2.value.Value;
import org.h2.value.ValueDate;
import org.h2.value.ValueNull;
import org.junit.jupiter.api.Test;

import java.text.ParseException;
import java.time.LocalDate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

final class LocalDateCodecTest {

    @Test
    void decode() throws ParseException {
        ValueDate valueDate = ValueDate.parse("2018-10-31");

        assertThat(new LocalDateCodec().decode(valueDate, LocalDate.class))
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
    void doEncode() throws ParseException {
        ValueDate valueDate = ValueDate.parse("2018-10-31");

        assertThat(new LocalDateCodec().doEncode(LocalDate.of(2018, 10, 31)))
            .isEqualTo(valueDate);
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
