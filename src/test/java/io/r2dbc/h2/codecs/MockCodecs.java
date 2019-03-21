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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public final class MockCodecs implements Codecs {

    private final Map<Decoding, Object> decodings;

    private final Map<Object, Value> encodings;

    private final Map<Integer, Class<?>> preferredTypes;

    private MockCodecs(Map<Decoding, Object> decodings, Map<Object, Value> encodings, Map<Integer, Class<?>> preferredTypes) {
        this.decodings = Assert.requireNonNull(decodings, "decodings must not be null");
        this.encodings = Assert.requireNonNull(encodings, "encodings must not be null");
        this.preferredTypes = Assert.requireNonNull(preferredTypes, "preferredTypes must not be null");
    }

    public static Builder builder() {
        return new Builder();
    }

    public static MockCodecs empty() {
        return builder().build();
    }

    @Override
    @Nullable
    @SuppressWarnings("unchecked")
    public <T> T decode(Value value, int dataType, Class<? extends T> type) {
        Assert.requireNonNull(type, "type must not be null");

        Decoding decoding = new Decoding(value, dataType, type);

        if (!this.decodings.containsKey(decoding)) {
            throw new AssertionError(String.format("Unexpected call to decode(value,int,Class<?>) with values '%s, %d, %s'", value, dataType, type.getName()));
        }

        return (T) this.decodings.get(decoding);
    }

    @Override
    public Value encode(Object value) {
        Assert.requireNonNull(value, "value must not be null");

        if (!this.encodings.containsKey(value)) {
            throw new AssertionError(String.format("Unexpected call to encode(Object) with value '%s'", value));
        }

        return this.encodings.get(value);
    }

    @Override
    public Value encodeNull(Class<?> type) {
        Assert.requireNonNull(type, "type must not be null");

        if (!this.encodings.containsKey(type)) {
            throw new AssertionError(String.format("Unexpected call to encodeNull(Class<?>) with value '%s'", type));
        }

        return this.encodings.get(type);
    }

    @Override
    public Class<?> preferredType(int dataType) {
        if (!this.preferredTypes.containsKey(dataType)) {
            throw new AssertionError(String.format("Unexpected call to preferredType(int) with value '%d'", dataType));
        }

        return this.preferredTypes.get(dataType);
    }

    @Override
    public String toString() {
        return "MockCodecs{" +
            "decodings=" + this.decodings +
            ", encodings=" + this.encodings +
            ", preferredTypes=" + this.preferredTypes +
            '}';
    }

    public static final class Builder {

        private final Map<Decoding, Object> decodings = new HashMap<>();

        private final Map<Object, Value> encodings = new HashMap<>();

        private final Map<Integer, Class<?>> preferredTypes = new HashMap<>();

        private Builder() {
        }

        public MockCodecs build() {
            return new MockCodecs(this.decodings, this.encodings, this.preferredTypes);
        }

        public <T> Builder decoding(@Nullable Value encodedValue, int dataType, Class<T> type, T value) {
            Assert.requireNonNull(type, "type must not be null");

            this.decodings.put(new Decoding(encodedValue, dataType, type), value);
            return this;
        }

        public Builder encoding(@Nullable Object value, Value parameter) {
            Assert.requireNonNull(parameter, "parameter must not be null");

            this.encodings.put(value, parameter);
            return this;
        }

        public Builder preferredType(int dataType, Class<?> type) {
            Assert.requireNonNull(type, "type must not be null");

            this.preferredTypes.put(dataType, type);
            return this;
        }

        @Override
        public String toString() {
            return "Builder{" +
                "decodings=" + this.decodings +
                ", encodings=" + this.encodings +
                ", preferredTypes=" + this.preferredTypes +
                '}';
        }
    }

    private static final class Decoding {

        private final int dataType;

        private final Class<?> type;

        private final Value value;

        private Decoding(@Nullable Value value, int dataType, Class<?> type) {
            this.value = value;
            this.dataType = dataType;
            this.type = Assert.requireNonNull(type, "type must not be null");
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Decoding decoding = (Decoding) o;
            return this.dataType == decoding.dataType &&
                Objects.equals(this.value, decoding.value) &&
                Objects.equals(this.type, decoding.type);
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.value, this.dataType, this.type);
        }

        @Override
        public String toString() {
            return "Decoding{" +
                "value=" + this.value +
                ", dataType=" + this.dataType +
                ", type=" + this.type +
                '}';
        }
    }
}