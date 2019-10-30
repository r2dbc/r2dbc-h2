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
import org.h2.value.ValueArray;

import java.util.Arrays;

final class ArrayCodec extends AbstractCodec<Object[]> {

    private final Codecs codecs;

    ArrayCodec(Codecs codecs) {
        super(Object[].class);
        this.codecs = codecs;
    }

    @Override
    boolean doCanDecode(int dataType) {
        return dataType == Value.ARRAY;
    }

    @Override
    Object[] doDecode(Value value, Class<? extends Object[]> type) {
        ValueArray valueArray = (ValueArray) value.convertTo(Value.ARRAY);
        return Arrays.stream(valueArray.getList())
                .map(val -> codecs.decode(val, val.getValueType(), Object.class)).toArray();
    }

    @Override
    Value doEncode(Object[] value) {
        return ValueArray.get(Arrays.stream(Assert.requireNonNull(value, "value must not be null"))
            .map(codecs::encode)
            .toArray(Value[]::new));
    }
}
