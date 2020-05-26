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

import io.r2dbc.h2.util.Assert;
import org.h2.util.JSR310Utils;
import org.h2.value.Value;

import java.time.Duration;

final class DurationCodec extends AbstractCodec<Duration> {

    DurationCodec() {
        super(Duration.class);
    }

    @Override
    boolean doCanDecode(int dataType) {
        return dataType != Value.INTERVAL_YEAR_TO_MONTH && Value.INTERVAL_DAY <= dataType && dataType <= Value.INTERVAL_MINUTE_TO_SECOND;
    }

    @Override
    Duration doDecode(Value value, Class<? extends Duration> type) {
        return (Duration) JSR310Utils.valueToDuration(value);
    }

    @Override
    Value doEncode(Duration value) {
        Assert.requireNonNull(value, "value must not be null");
        return JSR310Utils.durationToValue(value);
    }
}
