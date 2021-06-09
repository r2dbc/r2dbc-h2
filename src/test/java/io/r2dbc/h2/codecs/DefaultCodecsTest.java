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

import io.r2dbc.h2.client.Client;
import org.h2.value.Value;
import org.h2.value.ValueInt;
import org.h2.value.ValueNull;
import org.junit.jupiter.api.Test;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.*;

final class DefaultCodecsTest {

    @Test
    void addOptionalCodecsGeometry() throws Exception {
        ClassLoader mockClassLoader = mock(ClassLoader.class);
        willReturn(Object.class)
            .given(mockClassLoader)
            .loadClass(eq("org.locationtech.jts.geom.Geometry"));

        Codec<?> result = DefaultCodecs.addOptionalCodecs(mockClassLoader)
            .findFirst()
            .get();

        assertThat(result).isExactlyInstanceOf(GeometryCodec.class);
    }

    @Test
    void addOptionalCodecsGeometryNotFound() throws Exception {
        ClassLoader mockClassLoader = mock(ClassLoader.class);
        willThrow(new ClassNotFoundException())
            .given(mockClassLoader)
            .loadClass(eq("org.locationtech.jts.geom.Geometry"));

        long result = DefaultCodecs.addOptionalCodecs(mockClassLoader).count();

        assertThat(result).isEqualTo(0L);
    }

    @Test
    void createCodecsWithNonOptionalCodecsAndNoDuplicates() throws Exception {
        ClassLoader mockClassLoader = mock(ClassLoader.class);
        willThrow(new ClassNotFoundException())
            .given(mockClassLoader)
            .loadClass(any());

        Stream<Class<?>> result = DefaultCodecs.createCodecs(mock(Client.class), mockClassLoader, null)
            .stream()
            .map(Codec::getClass);

        assertThat(result).containsOnlyOnce(
            BigDecimalCodec.class,
            BlobToByteBufferCodec.class,
            BlobCodec.class,
            BooleanCodec.class,
            ByteCodec.class,
            BytesCodec.class,
            ClobCodec.class,
            DoubleCodec.class,
            FloatCodec.class,
            IntegerCodec.class,
            JsonCodec.class,
            LocalDateCodec.class,
            LocalDateTimeCodec.class,
            LocalTimeCodec.class,
            LongCodec.class,
            ShortCodec.class,
            StringCodec.class,
            UuidCodec.class,
            ZonedDateTimeCodec.class,
            InstantCodec.class,
            ArrayCodec.class
        );
    }

    @Test
    void createCodecsWithOptionalCodecsAndNoDuplicates() throws Exception {
        ClassLoader mockClassLoader = mock(ClassLoader.class);
        willReturn(Object.class)
            .given(mockClassLoader)
            .loadClass(eq("org.locationtech.jts.geom.Geometry"));

        Stream<Class<?>> result = DefaultCodecs.createCodecs(mock(Client.class), mockClassLoader, null)
            .stream()
            .map(Codec::getClass);

        assertThat(result).containsOnlyOnce(
            BigDecimalCodec.class,
            BlobToByteBufferCodec.class,
            BlobCodec.class,
            BooleanCodec.class,
            ByteCodec.class,
            BytesCodec.class,
            ClobCodec.class,
            DoubleCodec.class,
            FloatCodec.class,
            GeometryCodec.class,
            IntegerCodec.class,
            JsonCodec.class,
            LocalDateCodec.class,
            LocalDateTimeCodec.class,
            LocalTimeCodec.class,
            LongCodec.class,
            ShortCodec.class,
            StringCodec.class,
            UuidCodec.class,
            ZonedDateTimeCodec.class,
            InstantCodec.class,
            ArrayCodec.class
        );
    }

    @Test
    void decode() {
        assertThat(new DefaultCodecs(mock(Client.class)).decode(ValueInt.get(100), ValueInt.INT, Integer.class))
            .isEqualTo(100);
    }

    @Test
    void decodeDefaultType() {
        assertThat(new DefaultCodecs(mock(Client.class)).decode(ValueInt.get(100), ValueInt.INT, Object.class))
            .isEqualTo(100);
    }

    @Test
    void decodeNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new DefaultCodecs(mock(Client.class)).decode(ValueInt.get(100), ValueInt.INT, null))
            .withMessage("type must not be null");
    }

    @Test
    void decodeNull() {
        assertThat(new DefaultCodecs(mock(Client.class)).decode(null, ValueInt.INT, Integer.class))
            .isNull();
    }

    @Test
    void decodeUnsupportedType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new DefaultCodecs(mock(Client.class)).decode(ValueInt.get(100), ValueInt.INT, Void.class))
            .withMessage("Cannot decode value of type java.lang.Void");
    }

    @Test
    void encode() {
        Value parameter = new DefaultCodecs(mock(Client.class)).encode(100);

        assertThat(parameter).isEqualTo(ValueInt.get(100));
    }

    @Test
    void encodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new DefaultCodecs(mock(Client.class)).encode(null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        Value parameter = new DefaultCodecs(mock(Client.class)).encodeNull(Integer.class);

        assertThat(parameter).isEqualTo(ValueNull.INSTANCE);
    }

    @Test
    void encodeNullNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new DefaultCodecs(mock(Client.class)).encodeNull(null))
            .withMessage("type must not be null");
    }

    @Test
    void encodeNullUnsupportedType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new DefaultCodecs(mock(Client.class)).encodeNull(Object.class))
            .withMessage("Cannot encode null parameter of type java.lang.Object");
    }

    @Test
    void encodeUnsupportedType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new DefaultCodecs(mock(Client.class)).encode(new Object()))
            .withMessage("Cannot encode parameter of type java.lang.Object");
    }

    @Test
    void isPresent() {
        assertThat(DefaultCodecs.isPresent(this.getClass().getClassLoader(), "java.lang.Boolean")).isTrue();
    }

    @Test
    void isPresentNotFound() {
        assertThat(DefaultCodecs.isPresent(this.getClass().getClassLoader(), "java.lang.Boolean123456789")).isFalse();
    }
}
