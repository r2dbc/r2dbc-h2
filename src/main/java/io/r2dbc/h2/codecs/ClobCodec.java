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

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufUtil;
import io.r2dbc.h2.client.Client;
import io.r2dbc.h2.util.Assert;
import io.r2dbc.spi.Clob;
import org.h2.value.Value;
import org.h2.value.ValueLob;
import org.h2.value.ValueLobDb;
import org.h2.value.ValueNull;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.SequenceInputStream;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.Enumeration;
import java.util.Iterator;

final class ClobCodec extends AbstractCodec<Clob> {

    private final Client client;

    ClobCodec(Client client) {
        super(Clob.class);
        this.client = client;
    }

    @Override
    boolean doCanDecode(int dataType) {
        return dataType == Value.CLOB;
    }

    @Override
    Clob doDecode(Value value, Class<? extends Clob> type) {
        if (value == null || value instanceof ValueNull) {
            return null;
        }

        if (value instanceof ValueLobDb || value instanceof ValueLob) {
            return new ValueLobClob(value);
        }

        throw new IllegalArgumentException("value must be ValueLobDb or ValueLob!");
    }

    @Override
    Value doEncode(Clob value) {
        Assert.requireNonNull(value, "value must not be null");
        
        return this.client.getSession().getDataHandler().getLobStorage().createClob(
            new InputStreamReader(
                new SequenceInputStream(
                    new ClobInputStreamEnumeration(value))), -1);
    }

    /**
     * Converts a {@link Flux} of {@link Clob}s into an {@link Enumeration} of {@link InputStream}s.
     */
    private final class ClobInputStreamEnumeration implements Enumeration<InputStream> {

        private final Iterator<ByteBufInputStream> inputStreams;

        ClobInputStreamEnumeration(Clob value) {
            this.inputStreams = Flux.from(value.stream())
                .map(it -> ByteBufUtil.encodeString(ByteBufAllocator.DEFAULT, CharBuffer.wrap(it), Charset.defaultCharset()))
                .map(ByteBufInputStream::new)
                .subscribeOn(Schedulers.elastic())
                .cancelOn(Schedulers.elastic())
                .toIterable()
                .iterator();
        }

        @Override
        public boolean hasMoreElements() {
            return inputStreams.hasNext();
        }

        @Override
        public InputStream nextElement() {
            return inputStreams.next();
        }
    }
}
