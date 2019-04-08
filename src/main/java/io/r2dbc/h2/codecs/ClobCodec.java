/*
 * Copyright 2019 the original author or authors.
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

import io.r2dbc.h2.util.Assert;
import io.r2dbc.spi.Clob;
import org.h2.value.Value;
import org.h2.value.ValueLobDb;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

final class ClobCodec extends AbstractCodec<Clob> {

    ClobCodec() {
        super(Clob.class);
    }

    @Override
    boolean doCanDecode(int dataType) {
        return dataType == Value.CLOB;
    }

    @Override
    Clob doDecode(Value value, Class<? extends Clob> type) {
        ValueLobDb clobValue = (ValueLobDb) value.convertTo(Value.CLOB);
        ByteBuffer buff = ByteBuffer.wrap(clobValue.getSmall());
        CharBuffer data = buff.asCharBuffer();

        return Clob.from(Mono.just(data));
    }

    @Override
    Value doEncode(Clob value) {
        Assert.requireNonNull(value, "value must not be null");

        return Mono.from(value.stream())
            .map(charSequence -> {
                byte[] bytes = String.valueOf(charSequence).getBytes(StandardCharsets.UTF_16);

                // Drop the "endian" marker byte added by UTF_16
                return Arrays.copyOfRange(bytes, 2, bytes.length);
            })
            .map(bytes -> ValueLobDb.createSmallLob(Value.CLOB, bytes))
            .block();
    }
}
