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

import io.r2dbc.h2.client.Client;
import io.r2dbc.h2.util.Assert;
import org.h2.value.Value;
import org.h2.value.ValueNull;
import reactor.util.annotation.Nullable;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The default {@link Codecs} implementation. Delegates to type-specific codec implementations.
 */
public final class DefaultCodecs implements Codecs {

    private final List<Codec<?>> codecs;

    /**
     * Constructs a new DefaultCodecs (The Default {@link Codec}s list).
     *
     * @param client for Lobs {@link Codec}s and whose class loader is used to search for optional {@link Codec}s.
     */
    public DefaultCodecs(Client client) {
        this.codecs = createCodecs(client, client.getClass().getClassLoader(), this);
    }

    @Override
    @Nullable
    @SuppressWarnings("unchecked")
    public <T> T decode(Value value, int dataType, Class<? extends T> type) {
        Assert.requireNonNull(type, "type must not be null");

        if (value == null || value instanceof ValueNull) {
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

        if (dataType == Value.NULL) {
            return Void.class;
        }

        for (Codec<?> codec : this.codecs) {
            if (codec.canDecode(dataType, Object.class)) {
                return codec.type();
            }
        }

        return null;
    }

    /**
     * Creates Default {@link Codec}s list
     *
     * @param client      for Lobs {@link Codec}s
     * @param classLoader to scan for classes
     * @param codecs      for codecs that rely on other codecs
     * @return a {@link List} of default {@link Codec}s
     */
    static List<Codec<?>> createCodecs(Client client, ClassLoader classLoader, Codecs codecs) {
        return Stream.concat(
            Stream.of(
                new BigDecimalCodec(),
                new BlobToByteBufferCodec(client),
                new BlobCodec(client),
                new BooleanCodec(),
                new ByteCodec(),
                new BytesCodec(),
                new ClobToStringCodec(client),
                new ClobCodec(client),
                new DoubleCodec(),
                new FloatCodec(),
                new IntegerCodec(),
                new LocalDateCodec(),
                new LocalDateTimeCodec(client),
                new LocalTimeCodec(),
                new LongCodec(),
                new OffsetDateTimeCodec(client),
                new ShortCodec(),
                new StringCodec(),
                new UuidCodec(),
                new ZonedDateTimeCodec(client),
                new InstantCodec(client),
                new IntervalCodec(),
                new PeriodCodec(),
                new DurationCodec(),

                // De-prioritized codecs
                new ArrayCodec(codecs)
            ),
            addOptionalCodecs(classLoader)
        ).collect(Collectors.toList());
    }

    /**
     * Adds optional {@link Codec}s based on different conditions, e.g. classpath availability.
     *
     * @param classLoader to scan for classes
     * @return a {@link Stream} of additional {@link Codec}s
     */
    static Stream<Codec<?>> addOptionalCodecs(ClassLoader classLoader) {
        Stream.Builder<Codec<?>> optionalCodecs = Stream.builder();

        if (isPresent(classLoader, "org.locationtech.jts.geom.Geometry")) {
            optionalCodecs.accept(new GeometryCodec());
        }

        return optionalCodecs.build();
    }

    /**
     * Checks if the class is found in the current class loader.
     *
     * @param classLoader             the desired ClassLoader to use
     * @param fullyQualifiedClassName the fully qualified name of the desired class
     * @return true, if the class is found
     */
    static boolean isPresent(ClassLoader classLoader, String fullyQualifiedClassName) {
        try {
            classLoader.loadClass(fullyQualifiedClassName);
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }
}
