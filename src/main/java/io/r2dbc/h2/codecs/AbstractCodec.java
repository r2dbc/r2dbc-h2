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
import org.h2.value.ValueNull;
import reactor.util.annotation.Nullable;

abstract class AbstractCodec<T> implements Codec<T> {

    private final Class<T> type;

    AbstractCodec(Class<T> type) {
        this.type = Assert.requireNonNull(type, "type must not be null");
    }

    @Override
    public boolean canDecode(int dataType, Class<?> type) {
        Assert.requireNonNull(type, "type must not be null");

        return type.isAssignableFrom(this.type) && doCanDecode(dataType);
    }

    @Override
    public boolean canEncode(Object value) {
        Assert.requireNonNull(value, "value must not be null");

        return this.type.isInstance(value);
    }

    @Override
    public boolean canEncodeNull(Class<?> type) {
        Assert.requireNonNull(type, "type must not be null");

        return this.type.isAssignableFrom(type);
    }

    @Nullable
    @Override
    public T decode(Value value, Class<? extends T> type) {
        if (value == null) {
            return null;
        }

        return doDecode(value, type);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Value encode(Object value) {
        Assert.requireNonNull(value, "value must not be null");

        return doEncode((T) value);
    }

    @Override
    public Value encodeNull() {
        return ValueNull.INSTANCE;
    }

    @Override
    public Class<?> type() {
        return this.type;
    }

    abstract boolean doCanDecode(int dataType);

    abstract T doDecode(Value value, Class<? extends T> type);

    abstract Value doEncode(T value);
}
