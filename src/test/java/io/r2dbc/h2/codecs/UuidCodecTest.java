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

import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.h2.value.ValueUuid;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

final class UuidCodecTest {

    private static final String SAMPLE_UUID = "79e9eb45-2835-49c8-ad3b-c951b591bc7f";

    @Test
    void decode() {
        assertThat(new UuidCodec().decode(ValueUuid.get(SAMPLE_UUID), UUID.class))
            .isEqualTo(UUID.fromString(SAMPLE_UUID));
    }

    @Test
    void doCanDecode() {
        UuidCodec codec = new UuidCodec();

        assertThat(codec.doCanDecode(Value.UUID)).isTrue();
        assertThat(codec.doCanDecode(Value.UNKNOWN)).isFalse();
        assertThat(codec.doCanDecode(Value.INT)).isFalse();
    }

    @Test
    void doEncode() {
        assertThat(new UuidCodec().doEncode(UUID.fromString(SAMPLE_UUID)))
            .isEqualTo(ValueUuid.get(SAMPLE_UUID));
    }

    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new UuidCodec().doEncode(null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        assertThat(new UuidCodec().encodeNull())
            .isEqualTo(ValueNull.INSTANCE);
    }
}
