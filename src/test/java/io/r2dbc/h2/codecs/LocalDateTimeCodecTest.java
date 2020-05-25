package io.r2dbc.h2.codecs;

import io.r2dbc.h2.client.Client;
import io.r2dbc.h2.util.TestCastDataProvider;
import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.h2.value.ValueTimestamp;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

final class LocalDateTimeCodecTest {

    private Client client;

    @BeforeEach
    void setUp() {
        this.client = TestCastDataProvider.mockedClient();
    }

    @Test
    void decode() {
        assertThat(new LocalDateTimeCodec(client).decode(ValueTimestamp.parse("2018-10-31 11:59:59", TestCastDataProvider.INSTANCE), LocalDateTime.class))
                .isEqualTo(LocalDateTime.of(2018, 10, 31, 11, 59, 59));
    }

    @Test
    void doCanDecode() {
        LocalDateTimeCodec codec = new LocalDateTimeCodec(client);

        assertThat(codec.doCanDecode(Value.TIMESTAMP)).isTrue();
        assertThat(codec.doCanDecode(Value.UNKNOWN)).isFalse();
        assertThat(codec.doCanDecode(Value.INTEGER)).isFalse();
    }

    @Test
    void doEncode() {
        assertThat(new LocalDateTimeCodec(client).doEncode(LocalDateTime.of(2018, 10, 31, 11, 59, 59)))
            .isEqualTo(ValueTimestamp.parse("2018-10-31 11:59:59", TestCastDataProvider.INSTANCE));
    }

    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new LocalDateTimeCodec(client).doEncode(null))
                .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        assertThat(new LocalDateTimeCodec(client).encodeNull())
                .isEqualTo(ValueNull.INSTANCE);
    }
}
