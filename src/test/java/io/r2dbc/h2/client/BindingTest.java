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

package io.r2dbc.h2.client;

import org.h2.value.Value;
import org.h2.value.ValueInteger;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

final class BindingTest {

    @Test
    void addNoIndex() {
        assertThatIllegalArgumentException().isThrownBy(() -> new Binding().add(null, ValueInteger.get(0)))
            .withMessage("index must not be null");
    }

    @Test
    void addNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new Binding().add(1, (Value) null))
            .withMessage("value must not be null");
    }

    @Test
    void addNoDeferredValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new Binding().add(1, (Mono<Value>) null))
            .withMessage("value must not be null");
    }

    @Test
    void resolveDeferredParameters() {
        Binding binding = new Binding()
            .add(0, ValueInteger.get(1))
            .add(1, Mono.just(ValueInteger.get(2)));

        assertThat(binding.hasDeferredParameters()).isTrue();

        binding.resolve()
            .as(StepVerifier::create)
            .assertNext(resolved -> {
                assertThat(resolved.hasDeferredParameters()).isFalse();
                assertThat(resolved.getParameters()).containsEntry(0, ValueInteger.get(1));
                assertThat(resolved.getParameters()).containsEntry(1, ValueInteger.get(2));
            })
            .verifyComplete();
    }

}
