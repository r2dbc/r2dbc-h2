package io.r2dbc.h2.codecs;

import io.r2dbc.h2.util.TestCastDataProvider;
import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.h2.value.ValueTimestampTimeZone;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

final class InstantCodecTest {

    private InstantCodec instantCodec;

    @BeforeEach
    void setUp() {
        instantCodec = new InstantCodec(TestCastDataProvider.mockedClient());
    }

    @Test
    void decode() {
        ValueTimestampTimeZone valueTimestamp = ValueTimestampTimeZone.parse("2018-10-31 11:59:59+05:00", TestCastDataProvider.INSTANCE);

        Instant instant = instantCodec.decode(valueTimestamp, Instant.class);

        assertThat(instant).isEqualTo(LocalDateTime.of(2018, 10, 31, 11, 59, 59).toInstant(ZoneOffset.ofHours(5)));
    }

    @Test
    void doCanDecode() {
        assertThat(instantCodec.doCanDecode(Value.TIMESTAMP_TZ)).isTrue();
        assertThat(instantCodec.doCanDecode(Value.TIMESTAMP)).isFalse();
        assertThat(instantCodec.doCanDecode(Value.UNKNOWN)).isFalse();
        assertThat(instantCodec.doCanDecode(Value.INTEGER)).isFalse();
    }

    @Test
    void doEncode() {
        Instant instant = LocalDateTime.of(2018, 10, 31, 11, 59, 59).toInstant(ZoneOffset.UTC);

        Value valueTimestamp = instantCodec.doEncode(instant);

        assertThat(valueTimestamp).isEqualTo(ValueTimestampTimeZone.parse("2018-10-31 11:59:59", TestCastDataProvider.INSTANCE));
    }
    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> instantCodec.doEncode(null))
                                            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        assertThat(instantCodec.encodeNull()).isEqualTo(ValueNull.INSTANCE);
    }
}
