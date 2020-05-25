package io.r2dbc.h2.codecs;

import io.r2dbc.h2.client.Client;
import io.r2dbc.h2.util.TestCastDataProvider;
import org.h2.engine.Session;
import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.h2.value.ValueTimestampTimeZone;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

final class ZonedDateTimeCodecTest {

    private Client client;

    @BeforeEach
    void setUp() {
        this.client = mock(Client.class);

        when(this.client.getSession()).thenReturn(mock(Session.class));
    }

    @Test
    void decode() {
        assertThat(new ZonedDateTimeCodec(client).doDecode(ValueTimestampTimeZone.parse("2018-10-31 11:59:59+05:00", TestCastDataProvider.INSTANCE), ZonedDateTime.class))
                .isEqualTo(ZonedDateTime.of(2018, 10, 31, 11, 59, 59, 0, ZoneOffset.ofHours(5)));
    }

    @Test
    void doCanDecode() {
        ZonedDateTimeCodec codec = new ZonedDateTimeCodec(client);

        assertThat(codec.doCanDecode(Value.TIMESTAMP_TZ)).isTrue();
        assertThat(codec.doCanDecode(Value.UNKNOWN)).isFalse();
        assertThat(codec.doCanDecode(Value.INTEGER)).isFalse();
    }

    @Test
    void doEncode() {
        assertThat(new ZonedDateTimeCodec(client).doEncode(ZonedDateTime.of(2018, 10, 31, 11, 59, 59, 0, ZoneOffset.ofHours(5))))
            .isEqualTo(ValueTimestampTimeZone.parse("2018-10-31 11:59:59+05:00", TestCastDataProvider.INSTANCE));
    }

    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new ZonedDateTimeCodec(client).doEncode(null))
                .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        assertThat(new ZonedDateTimeCodec(client).encodeNull())
                .isEqualTo(ValueNull.INSTANCE);
    }
}
