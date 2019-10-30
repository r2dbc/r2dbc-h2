/*
 * Copyright 2018 the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.Mockito.mock;

import java.util.Arrays;

import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueNull;
import org.h2.value.ValueString;
import org.junit.jupiter.api.Test;

final class ArrayCodecTest {

    private final String[] TEST_ARRAY = { "Element1", "Element2", "Element3" };

    @Test
    void decode() {
        Value[] values = Arrays.stream(TEST_ARRAY).map(ValueString::get).toArray(Value[]::new);
        ValueArray valueArray = ValueArray.get(values);

        Object[] decoded = new ArrayCodec(mock(Codecs.class)).decode(valueArray, String[].class);

        assertThat(decoded).containsExactlyElementsOf(Arrays.asList(TEST_ARRAY));

    }

    @Test
    void decodeNull() {
        assertThat(new ArrayCodec(mock(Codecs.class)).decode(null, String[].class)).isNull();
    }

    @Test
    void doCanDecode() {
        ArrayCodec codec = new ArrayCodec(mock(Codecs.class));
        assertThat(codec.doCanDecode(Value.ARRAY)).isTrue();
        assertThat(codec.doCanDecode(Value.STRING)).isFalse();
        assertThat(codec.doCanDecode(Value.JAVA_OBJECT)).isFalse();
    }

    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new ArrayCodec(mock(Codecs.class)).doEncode(null))
                .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        assertThat(new ArrayCodec(mock(Codecs.class)).encodeNull())
            .isEqualTo(ValueNull.INSTANCE);
    }

    @Test
    void encode() {
        MockCodecs codecs = MockCodecs.builder()
                .encoding(TEST_ARRAY[0], ValueString.get(TEST_ARRAY[0]))
                .encoding(TEST_ARRAY[1], ValueString.get(TEST_ARRAY[1]))
                .encoding(TEST_ARRAY[2], ValueString.get(TEST_ARRAY[2]))
                .build();

        Value value = new ArrayCodec(codecs).doEncode(TEST_ARRAY);
        assertThat(value).isInstanceOf(ValueArray.class);

        Value[] list = ((ValueArray) value).getList();

        assertThat(list).containsExactly(ValueString.get(TEST_ARRAY[0]), ValueString.get(TEST_ARRAY[1]),
                ValueString.get(TEST_ARRAY[2]));
    }

}
