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
import org.h2.value.ValueBoolean;

import java.util.Objects;

/**
 * @author Greg Turnquist
 */
final class BooleanCodec extends AbstractCodec<Boolean> {

    BooleanCodec() {
        super(Boolean.class);
    }

    @Override
    boolean doCanDecode(int dataType) {
        return dataType == Value.BOOLEAN;
    }

    @Override
    Boolean doDecode(Value value, Class<? extends Boolean> type) {
        return value.getBoolean();
    }

    @Override
    Value doEncode(Boolean value) {
        return ValueBoolean.get(Assert.requireNonNull(value, "value must not be null"));
    }
}
