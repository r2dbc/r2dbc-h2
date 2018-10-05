/*
 * Copyright 2017-2018 the original author or authors.
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

import org.h2.result.ResultInterface;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.mockito.Mockito.RETURNS_SMART_NULLS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

final class H2ColumnMetadataTest {

    private final ResultInterface result = mock(ResultInterface.class, RETURNS_SMART_NULLS);

    @Test
    void constructorNoName() {
        assertThatNullPointerException().isThrownBy(() -> new H2ColumnMetadata(null, (long) 100, 200))
            .withMessage("name must not be null");
    }

    @Test
    void constructorNoPrecision() {
        assertThatNullPointerException().isThrownBy(() -> new H2ColumnMetadata("test-name", null, 200))
            .withMessage("precision must not be null");
    }

    @Test
    void constructorNoType() {
        assertThatNullPointerException().isThrownBy(() -> new H2ColumnMetadata("test-name", (long) 100, null))
            .withMessage("type must not be null");
    }

    @Test
    void toColumnMetadata() {
        when(this.result.getColumnName(0)).thenReturn("test-name");
        when(this.result.getColumnPrecision(0)).thenReturn(400L);
        when(this.result.getColumnType(0)).thenReturn(200);

        H2ColumnMetadata columnMetadata = H2ColumnMetadata.toColumnMetadata(this.result, 0);

        assertThat(columnMetadata.getName()).isEqualTo("test-name");
        assertThat(columnMetadata.getPrecision()).hasValue(400);
        assertThat(columnMetadata.getType()).isEqualTo(200);
    }

    @Test
    void toColumnMetadataNoResult() {
        assertThatNullPointerException().isThrownBy(() -> H2ColumnMetadata.toColumnMetadata(null, 0))
            .withMessage("result must not be null");
    }
}