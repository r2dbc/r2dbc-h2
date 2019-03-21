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
import org.h2.result.ResultInterface;
import org.h2.table.Column;
import org.h2.value.TypeInfo;
import org.junit.jupiter.api.Test;

import static io.r2dbc.spi.Nullability.NULLABLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.Mockito.RETURNS_SMART_NULLS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

final class H2ColumnMetadataTest {

    private final ResultInterface result = mock(ResultInterface.class, RETURNS_SMART_NULLS);

    @Test
    void constructorNoName() {
        assertThatIllegalArgumentException().isThrownBy(() -> new H2ColumnMetadata(String.class, null, 200, NULLABLE, 100L, 500))
            .withMessage("name must not be null");
    }

    @Test
    void constructorNoNativeType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new H2ColumnMetadata(String.class, "test-name", null, NULLABLE, 100L, 500))
            .withMessage("nativeType must not be null");
    }

    @Test
    void constructorNoNullability() {
        assertThatIllegalArgumentException().isThrownBy(() -> new H2ColumnMetadata(String.class, "test-name", 200, null, 100L, 500))
            .withMessage("nullability must not be null");
    }

    @Test
    void constructorNoPrecision() {
        assertThatIllegalArgumentException().isThrownBy(() -> new H2ColumnMetadata(String.class, "test-name", 200, NULLABLE, null, 500))
            .withMessage("precision must not be null");
    }

    @Test
    void constructorNoScale() {
        assertThatIllegalArgumentException().isThrownBy(() -> new H2ColumnMetadata(String.class, "test-name", 200, NULLABLE, 100L, null))
            .withMessage("scale must not be null");
    }

    @Test
    void toColumnMetadata() {
        TypeInfo typeInfo = TypeInfo.TYPE_INT;
        when(this.result.getColumnName(0)).thenReturn("test-name");
        when(this.result.getColumnType(0)).thenReturn(typeInfo);
        when(this.result.getNullable(0)).thenReturn(Column.NULLABLE);

        MockCodecs codecs = MockCodecs.builder()
            .preferredType(4, String.class)
            .build();

        H2ColumnMetadata columnMetadata = H2ColumnMetadata.toColumnMetadata(codecs, this.result, 0);

        assertThat(columnMetadata.getJavaType()).isEqualTo(String.class);
        assertThat(columnMetadata.getName()).isEqualTo("test-name");
        assertThat(columnMetadata.getNativeTypeMetadata()).isEqualTo(4);
        assertThat(columnMetadata.getNullability()).isEqualTo(NULLABLE);
        assertThat(columnMetadata.getPrecision()).isEqualTo(10);
        assertThat(columnMetadata.getScale()).isEqualTo(0);
    }

    @Test
    void toColumnMetadataNoCodecs() {
        assertThatIllegalArgumentException().isThrownBy(() -> H2ColumnMetadata.toColumnMetadata(null, this.result, 0))
            .withMessage("codecs must not be null");
    }

    @Test
    void toColumnMetadataNoResult() {
        assertThatIllegalArgumentException().isThrownBy(() -> H2ColumnMetadata.toColumnMetadata(MockCodecs.empty(), null, 0))
            .withMessage("result must not be null");
    }
}