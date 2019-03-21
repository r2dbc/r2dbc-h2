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
import org.h2.value.ValueDate;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;

/**
 * @author Greg Turnquist
 */
final class DateCodec extends AbstractCodec<Date> {

    DateCodec() {
        super(Date.class);
    }

    @Override
    boolean doCanDecode(int dataType) {
        return dataType == Value.DATE;
    }

    @Override
    Date doDecode(Value value, Class<? extends Date> type) {
        return transform(value.getDate());
    }

    @Override
    Value doEncode(Date value) {
        return ValueDate.get(transform(Assert.requireNonNull(value, "value must not be null")));
    }

    static java.sql.Date transform(Date utilDate) {
        ZonedDateTime zdt = ZonedDateTime.ofInstant(utilDate.toInstant(), ZoneId.systemDefault());
        LocalDate localDate = zdt.toLocalDate();
        return java.sql.Date.valueOf(localDate);
    }

    static Date transform(java.sql.Date sqlDate) {
        return new Date(sqlDate.getTime());
    }
}
