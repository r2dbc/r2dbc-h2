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
import org.h2.value.ValueByte;

import java.util.Objects;

/**
 * @author Greg Turnquist
 */
final class ByteCodec extends AbstractCodec<Byte> {

    ByteCodec() {
        super(Byte.class);
    }

    @Override
    boolean doCanDecode(int dataType) {
        return dataType == Value.BYTE;
    }

    @Override
    Byte doDecode(Value value, Class<? extends Byte> type) {
        return value.getByte();
    }

    @Override
    Value doEncode(Byte value) {
        return ValueByte.get(Assert.requireNonNull(value, "value must not be null"));
    }
}
