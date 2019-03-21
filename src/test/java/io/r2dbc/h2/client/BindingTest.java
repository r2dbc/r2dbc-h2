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

import org.h2.value.ValueInt;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

final class BindingTest {

    @Test
    void addNoIndex() {
        assertThatIllegalArgumentException().isThrownBy(() -> new Binding().add(null, ValueInt.get(0)))
            .withMessage("index must not be null");
    }

    @Test
    void addNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new Binding().add(1, null))
            .withMessage("value must not be null");
    }

}