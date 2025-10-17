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

package io.r2dbc.h2;

import io.r2dbc.h2.codecs.DefaultCodecs;
import io.r2dbc.h2.util.IntegrationTestSupport;
import io.r2dbc.spi.Option;
import io.r2dbc.spi.TransactionDefinition;
import org.h2.engine.Constants;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

final class H2ConnectionIntegrationTest extends IntegrationTestSupport {

    @Test
    void getMetadata() {

        H2ConnectionMetadata metadata = TestSessionClient.create(options).doWithClient(sessionClient -> {
            H2Connection connection = new H2Connection(sessionClient, new DefaultCodecs(sessionClient));
            return connection.getMetadata();
        });

        assertThat(metadata.getDatabaseVersion()).isEqualTo(Constants.VERSION);
    }

    @Test
    void beginTransaction() {

        TransactionDefinition definition = new TransactionDefinition() {

            @Override
            public <T> T getAttribute(Option<T> option) {

                if (TransactionDefinition.READ_ONLY == option) {
                    return (T) (Boolean.TRUE);
                }

                return null;
            }
        };

        connection.beginTransaction(definition).as(StepVerifier::create).verifyComplete();
    }
}
