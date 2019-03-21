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
import org.h2.value.ValueString;

/**
 * @author Greg Turnquist
 */
final class StringCodec extends AbstractCodec<String> {

    StringCodec() {
        super(String.class);
    }

    @Override
    boolean doCanDecode(int dataType) {
        return Value.STRING == dataType ||
            Value.STRING_FIXED == dataType ||
            Value.STRING_IGNORECASE == dataType;
    }

    @Override
    String doDecode(Value value, Class<? extends String> type) {
        return value.getString();
    }

    @Override
    Value doEncode(String value) {
        return ValueString.get(Assert.requireNonNull(value, "value must not be null"));
    }
}
