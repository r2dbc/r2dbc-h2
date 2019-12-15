package io.r2dbc.h2.codecs;

import io.r2dbc.h2.client.Client;
import org.h2.engine.Session;
import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.h2.value.ValueTimestamp;
import org.h2.value.ValueTimestampTimeZone;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

final class InstantCodecTest {

    private Client client;
    private InstantCodec instantCodec;

    @BeforeEach
    void setUp() {
        this.client = mock(Client.class);
        when(this.client.getSession()).thenReturn(mock(Session.class));
        instantCodec = new InstantCodec(client);
    }

    @Test
    void decode() {
        ValueTimestamp valueTimestamp = ValueTimestamp.parse("2018-10-31 11:59:59");

        Instant instant = instantCodec.decode(valueTimestamp, Instant.class);

        assertThat(instant).isEqualTo(LocalDateTime.of(2018, 10, 31, 11, 59, 59).toInstant(ZoneOffset.UTC));
    }

    @Test
    void doCanDecode() {
        assertThat(instantCodec.doCanDecode(Value.TIMESTAMP)).isTrue();
        assertThat(instantCodec.doCanDecode(Value.UNKNOWN)).isFalse();
        assertThat(instantCodec.doCanDecode(Value.INT)).isFalse();
    }

    @Test
    void doEncode() {
        Instant instant = LocalDateTime.of(2018, 10, 31, 11, 59, 59).toInstant(ZoneOffset.UTC);

        Value valueTimestamp = instantCodec.doEncode(instant);

        assertThat(valueTimestamp).isEqualTo(ValueTimestampTimeZone.parse("2018-10-31 11:59:59"));
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