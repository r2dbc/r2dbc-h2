/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.r2dbc.h2.codecs;

import org.h2.value.Value;
import reactor.util.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public final class MockCodecs implements Codecs {

    private final Map<Decoding, Object> decodings;

    private final Map<Object, Value> encodings;

    private MockCodecs(Map<Decoding, Object> decodings, Map<Object, Value> encodings) {
        this.decodings = Objects.requireNonNull(decodings);
        this.encodings = Objects.requireNonNull(encodings);
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
        Objects.requireNonNull(type);

        Decoding decoding = new Decoding(value, dataType, type);

        if (!this.decodings.containsKey(decoding)) {
            throw new AssertionError(String.format("Unexpected call to decode(value,int,Class<?>) with values '%s, %d, %s'", value, dataType, type.getName()));
        }

        return (T) this.decodings.get(decoding);
    }

    @Override
    public Value encode(Object value) {
        Objects.requireNonNull(value);

        if (!this.encodings.containsKey(value)) {
            throw new AssertionError(String.format("Unexpected call to encode(Object) with value '%s'", value));
        }

        return this.encodings.get(value);
    }

    @Override
    public Value encodeNull(Class<?> type) {
        Objects.requireNonNull(type);

        if (!this.encodings.containsKey(type)) {
            throw new AssertionError(String.format("Unexpected call to encodeNull(Class<?>) with value '%s'", type));
        }

        return this.encodings.get(type);
    }

    public static final class Builder {

        private final Map<Decoding, Object> decodings = new HashMap<>();

        private final Map<Object, Value> encodings = new HashMap<>();

        private Builder() {
        }

        public MockCodecs build() {
            return new MockCodecs(this.decodings, this.encodings);
        }

        public <T> Builder decoding(@Nullable Value encodedValue, int dataType, Class<T> type, T value) {
            Objects.requireNonNull(type);

            this.decodings.put(new Decoding(encodedValue, dataType, type), value);
            return this;
        }

        public Builder encoding(@Nullable Object value, Value parameter) {
            Objects.requireNonNull(parameter);

            this.encodings.put(value, parameter);
            return this;
        }

        @Override
        public String toString() {
            return "Builder{" +
                "decodings=" + decodings +
                ", encodings=" + encodings +
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
            this.type = Objects.requireNonNull(type);
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
            return dataType == decoding.dataType &&
                Objects.equals(value, decoding.value) &&
                Objects.equals(type, decoding.type);
        }

        @Override
        public int hashCode() {

            return Objects.hash(value, dataType, type);
        }

        @Override
        public String toString() {
            return "Decoding{" +
                "value=" + value +
                ", dataType=" + dataType +
                ", type=" + type +
                '}';
        }
    }
}