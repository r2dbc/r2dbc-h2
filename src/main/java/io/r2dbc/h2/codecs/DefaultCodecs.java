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

import io.r2dbc.h2.util.Assert;
import org.h2.value.Value;
import reactor.util.annotation.Nullable;

import java.util.Arrays;
import java.util.List;

/**
 * The default {@link Codecs} implementation. Delegates to type-specific codec implementations.
 */
public final class DefaultCodecs implements Codecs {

    private final List<Codec<?>> codecs;

    public DefaultCodecs() {
        this.codecs = Arrays.asList(
            new BigDecimalCodec(),
            new BooleanCodec(),
            new ByteCodec(),
            new BytesCodec(),
            new DateCodec(),
            new DoubleCodec(),
            new FloatCodec(),
            new IntegerCodec(),
            new LongCodec(),
            new ShortCodec(),
            new StringCodec(),
            new TimeCodec(),
            new TimestampCodec()
        );
    }

    @Override
    @Nullable
    @SuppressWarnings("unchecked")
    public <T> T decode(Value value, int dataType, Class<? extends T> type) {
        Assert.requireNonNull(type, "type must not be null");

        if (value == null) {
            return null;
        }

        for (Codec<?> codec : this.codecs) {
            if (codec.canDecode(dataType, type)) {
                return ((Codec<T>) codec).decode(value, type);
            }
        }

        throw new IllegalArgumentException(String.format("Cannot decode value of type %s", type.getName()));
    }

    @Override
    public Value encode(Object value) {
        Assert.requireNonNull(value, "value must not be null");

        for (Codec<?> codec : this.codecs) {
            if (codec.canEncode(value)) {
                return codec.encode(value);
            }
        }

        throw new IllegalArgumentException(String.format("Cannot encode parameter of type %s", value.getClass().getName()));
    }

    @Override
    public Value encodeNull(Class<?> type) {
        Assert.requireNonNull(type, "type must not be null");

        for (Codec<?> codec : this.codecs) {
            if (codec.canEncodeNull(type)) {
                return codec.encodeNull();
            }
        }

        throw new IllegalArgumentException(String.format("Cannot encode null parameter of type %s", type.getName()));
    }

    @Override
    public Class<?> preferredType(int dataType) {
        for (Codec<?> codec : this.codecs) {
            if (codec.canDecode(dataType, Object.class)) {
                return codec.type();
            }
        }

        return null;
    }
}
