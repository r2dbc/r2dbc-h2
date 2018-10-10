/*
 * Copyright 2018 the original author or authors.
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

package io.r2dbc.h2;

import io.r2dbc.h2.client.Client;
import org.h2.message.DbException;
import org.h2.result.LocalResult;
import org.h2.result.ResultWithGeneratedKeys;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.mockito.Mockito.RETURNS_SMART_NULLS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

final class H2BatchTest {

    private final Client client = mock(Client.class, RETURNS_SMART_NULLS);

    @Test
    void addNoSql() {
        assertThatNullPointerException().isThrownBy(() -> new H2Batch(this.client).add(null))
            .withMessage("sql must not be null");
    }

    @Disabled("Not yet implemented")
    @Test
    void addWithParameter() {
        assertThatIllegalArgumentException().isThrownBy(() -> new H2Batch(this.client).add("test-query-$1"))
            .withMessage("Statement 'test-query-$1' is not supported.  This is often due to the presence of parameters.");
    }

    @Test
    void constructorNoClient() {
        assertThatNullPointerException().isThrownBy(() -> new H2Batch(null))
            .withMessage("client must not be null");
    }

    @Test
    void execute() {
        when(this.client.update("test-query-1", Collections.emptyList())).thenReturn(Flux.just(new ResultWithGeneratedKeys.WithKeys(0, new LocalResult())));
        when(this.client.update("test-query-2", Collections.emptyList())).thenReturn(Flux.just(new ResultWithGeneratedKeys.WithKeys(0, new LocalResult())));

        new H2Batch(this.client)
            .add("test-query-1")
            .add("test-query-2")
            .execute()
            .as(StepVerifier::create)
            .expectNextCount(2)
            .verifyComplete();
    }

    @Test
    void executeErrorResponse() {
        when(this.client.update("test-query", Collections.emptyList())).thenReturn(Flux.error(DbException.get(0)));

        new H2Batch(this.client)
            .add("test-query")
            .execute()
            .as(StepVerifier::create)
            .verifyError(H2DatabaseException.class);
    }

}
