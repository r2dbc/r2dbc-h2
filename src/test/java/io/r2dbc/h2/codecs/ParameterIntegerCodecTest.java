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

import io.r2dbc.spi.Parameter;
import io.r2dbc.spi.Parameters;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueInteger;
import org.h2.value.ValueNull;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.Mockito.mock;

final class ParameterIntegerCodecTest {

    private final Parameter[] TEST_ARRAY = {Parameters.in(1), Parameters.in(2), Parameters.in(3)};

    @Test
    void decode() {
        Value[] values = Arrays.stream(TEST_ARRAY).map(Parameter::getValue).map(o -> (Integer) o).map(ValueInteger::get).toArray(Value[]::new);
        ValueArray valueArray = ValueArray.get(values, null);

        MockCodecs codecs = MockCodecs.builder()
            .decoding(ValueInteger.get((Integer) TEST_ARRAY[0].getValue()), Value.INTEGER, Object.class, TEST_ARRAY[0])
            .decoding(ValueInteger.get((Integer) TEST_ARRAY[1].getValue()), Value.INTEGER, Object.class, TEST_ARRAY[1])
            .decoding(ValueInteger.get((Integer) TEST_ARRAY[2].getValue()), Value.INTEGER, Object.class, TEST_ARRAY[2])
            .build();

        Object[] decoded = new ArrayCodec(codecs).decode(valueArray, Integer[].class);

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
        assertThat(codec.doCanDecode(Value.INTEGER)).isFalse();
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
            .encoding(TEST_ARRAY[0], ValueInteger.get((Integer) TEST_ARRAY[0].getValue()))
            .encoding(TEST_ARRAY[1], ValueInteger.get((Integer) TEST_ARRAY[1].getValue()))
            .encoding(TEST_ARRAY[2], ValueInteger.get((Integer) TEST_ARRAY[2].getValue()))
            .build();

        Value value = new ArrayCodec(codecs).doEncode(TEST_ARRAY);
        assertThat(value).isInstanceOf(ValueArray.class);

        Value[] list = ((ValueArray) value).getList();

        assertThat(list).containsExactly(ValueInteger.get((Integer) TEST_ARRAY[0].getValue()), ValueInteger.get((Integer) TEST_ARRAY[1].getValue()),
            ValueInteger.get((Integer) TEST_ARRAY[2].getValue()));
    }

}
