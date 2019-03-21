/*
 * Copyright 2017-2018 the original author or authors.
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

import io.r2dbc.h2.codecs.MockCodecs;
import org.h2.message.DbException;
import org.h2.result.ResultInterface;
import org.h2.value.Value;
import org.h2.value.ValueInt;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.Mockito.RETURNS_SMART_NULLS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

final class H2ResultTest {

    private final ResultInterface result = mock(ResultInterface.class, RETURNS_SMART_NULLS);

    @Test
    void constructorNoRowMetadata() {
        assertThatIllegalArgumentException().isThrownBy(() -> new H2Result(null, Flux.empty(), Mono.empty()))
            .withMessage("rowMetadata must not be null");
    }

    @Test
    void constructorNoRows() {
        assertThatIllegalArgumentException().isThrownBy(() -> new H2Result(Mono.empty(), null, Mono.empty()))
            .withMessage("rows must not be null");
    }

    @Test
    void constructorNoRowsUpdated() {
        assertThatIllegalArgumentException().isThrownBy(() -> new H2Result(Mono.empty(), Flux.empty(), null))
            .withMessage("rowsUpdated must not be null");
    }

    @Test
    void toResultErrorResponse() {
        when(this.result.next()).thenThrow(DbException.get(0));

        H2Result result = H2Result.toResult(MockCodecs.empty(), this.result, null);

        result.map((row, rowMetadata) -> row)
            .as(StepVerifier::create)
            .verifyError(H2DatabaseException.class);

        result.getRowsUpdated()
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void toResultNoCodecs() {
        assertThatIllegalArgumentException().isThrownBy(() -> H2Result.toResult(null, this.result, 0))
            .withMessage("codecs must not be null");
    }

    @Test
    void toResultNoResult() {
        assertThatIllegalArgumentException().isThrownBy(() -> H2Result.toResult(MockCodecs.empty(), null, 0))
            .withMessage("result must not be null");
    }

    @Test
    void toResultRowDescription() {
        when(this.result.next()).thenReturn(true, true, false);
        when(this.result.currentRow()).thenReturn(new Value[]{ValueInt.get(100)}, new Value[]{ValueInt.get(200)});

        H2Result result = H2Result.toResult(MockCodecs.empty(), this.result, Integer.MAX_VALUE);

        result.map((row, rowMetadata) -> row)
            .as(StepVerifier::create)
            .expectNextCount(2)
            .verifyComplete();

        result.getRowsUpdated()
            .as(StepVerifier::create)
            .expectNext(Integer.MAX_VALUE)
            .verifyComplete();
    }

}