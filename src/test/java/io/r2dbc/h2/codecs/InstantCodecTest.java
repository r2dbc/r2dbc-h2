/*
 * Copyright 2018-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.r2dbc.h2.codecs;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import io.r2dbc.h2.client.Client;
import org.h2.engine.Session;
import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.h2.value.ValueTimestampTimeZone;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
        ValueTimestampTimeZone valueTimestamp = ValueTimestampTimeZone.parse("2018-10-31 11:59:59+05:00");

        Instant instant = instantCodec.decode(valueTimestamp, Instant.class);

        assertThat(instant).isEqualTo(LocalDateTime.of(2018, 10, 31, 11, 59, 59).toInstant(ZoneOffset.ofHours(5)));
    }

    @Test
    void doCanDecode() {
        assertThat(instantCodec.doCanDecode(Value.TIMESTAMP_TZ)).isTrue();
        assertThat(instantCodec.doCanDecode(Value.TIMESTAMP)).isFalse();
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
