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
import io.r2dbc.spi.Blob;
import org.h2.util.StringUtils;
import org.h2.value.Value;
import org.h2.value.ValueLobDb;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;

final class BlobCodec extends AbstractCodec<Blob> {

    BlobCodec() {
        super(Blob.class);
    }

    @Override
    boolean doCanDecode(int dataType) {
        return dataType == Value.BLOB;
    }

    @Override
    Blob doDecode(Value value, Class<? extends Blob> type) {
        Value blobValue = value.convertTo(Value.BLOB);
        ByteBuffer byteBuffer = ByteBuffer.wrap(blobValue.getBytes());
        return Blob.from(Mono.just(byteBuffer));
    }

    @Override
    Value doEncode(Blob value) {
        Assert.requireNonNull(value, "value must not be null");

        return Mono.from(value.stream())
            .map(byteBuffer -> StringUtils.convertBytesToHex(byteBuffer.array()))
            .map(s -> ValueLobDb.createSmallLob(Value.BLOB, s.getBytes()))
            .block();
    }
}
