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
import io.r2dbc.spi.Clob;
import org.h2.value.Value;
import org.h2.value.ValueClob;
import org.h2.value.ValueNull;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.StringReader;

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

        return new ValueLobClob(value);
    }

    @Override
    Value doEncode(Clob value) {
        return encodeReactive(value).block();
    }

    /**
     * Encode a {@link Clob} by materializing its stream reactively.
     * Does not block; consumption is deferred until the returned {@link Mono} is subscribed
     * (typically during statement execution).
     *
     * @param value the clob to encode
     * @return a mono emitting the H2 value
     */
    Mono<Value> encodeReactive(Clob value) {
        Assert.requireNonNull(value, "value must not be null");

        return Flux.from(value.stream())
            .reduceWith(StringBuilder::new, StringBuilder::append)
            .map(sb -> {
                String content = sb.toString();
                ValueClob clob = this.client.getSession().getDataHandler().getLobStorage().createClob(
                    new StringReader(content), content.length());

                this.client.getSession().addTemporaryLob(clob);

                return (Value) clob;
            })
            .flatMap(encoded -> Mono.from(value.discard())
                .onErrorResume(e -> Mono.empty())
                .thenReturn(encoded));
    }

}
