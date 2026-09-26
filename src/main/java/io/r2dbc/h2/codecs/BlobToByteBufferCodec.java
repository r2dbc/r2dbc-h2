/*
 * Copyright 2019 the original author or authors.
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

import io.r2dbc.h2.client.Client;
import io.r2dbc.h2.util.Assert;
import org.h2.value.Value;
import org.h2.value.ValueBlob;
import org.h2.value.ValueNull;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;

final class BlobToByteBufferCodec extends AbstractCodec<ByteBuffer> {

    private final Client client;

    BlobToByteBufferCodec(Client client) {
        super(ByteBuffer.class);
        this.client = client;
    }

    @Override
    boolean doCanDecode(int dataType) {
        return dataType == Value.BLOB;
    }

    @Override
    ByteBuffer doDecode(Value value, Class<? extends ByteBuffer> type) {
        if (value == null || value instanceof ValueNull) {
            return null;
        }

        byte[] bytes = value.getBytesNoCopy();
        ByteBuffer buffer = ByteBuffer.allocate(bytes.length);
        buffer.put(bytes);
        buffer.flip();

        return buffer;
    }

    @Override
    Value doEncode(ByteBuffer value) {
        Assert.requireNonNull(value, "value must not be null");

        ByteBuffer buffer = value.duplicate();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);

        ValueBlob blob = this.client.getSession().getDataHandler().getLobStorage().createBlob(
            new ByteArrayInputStream(bytes), bytes.length);

        this.client.getSession().addTemporaryLob(blob);

        return blob;
    }
}
