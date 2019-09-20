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

import io.r2dbc.h2.client.Client;
import io.r2dbc.h2.codecs.Codecs;
import io.r2dbc.h2.codecs.DefaultCodecs;
import io.r2dbc.h2.codecs.MockCodecs;
import org.h2.result.ResultInterface;
import org.h2.table.Column;
import org.h2.value.TypeInfo;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static io.r2dbc.spi.Nullability.NULLABLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.Mockito.RETURNS_SMART_NULLS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

final class H2RowMetadataTest {

    Client client = mock(Client.class);

    Codecs codecs = new DefaultCodecs(client);

    private final List<H2ColumnMetadata> columnMetadatas = Arrays.asList(
        new H2ColumnMetadata(codecs, "TEST-NAME-1", 200, NULLABLE, 100L, 500),
        new H2ColumnMetadata(codecs, "TEST-NAME-2", 400, NULLABLE, 300L, 600)
    );

    private final ResultInterface result = mock(ResultInterface.class, RETURNS_SMART_NULLS);

    @Test
    void constructorNoColumnMetadata() {
        assertThatIllegalArgumentException().isThrownBy(() -> new H2RowMetadata(null))
            .withMessage("columnMetadatas must not be null");
    }

    @Test
    void getColumnMetadataIndex() {
        assertThat(new H2RowMetadata(this.columnMetadatas).getColumnMetadata(1))
            .isEqualTo(new H2ColumnMetadata(codecs, "TEST-NAME-2", 400, NULLABLE, 300L, 600));
    }

    @Test
    void getColumnMetadataInvalidIndex() {
        assertThatIllegalArgumentException().isThrownBy(() -> new H2RowMetadata(this.columnMetadatas).getColumnMetadata(2))
            .withMessage("Column index 2 is larger than the number of columns 2");
    }

    @Test
    void getColumnMetadataInvalidName() {
        assertThatIllegalArgumentException().isThrownBy(() -> new H2RowMetadata(this.columnMetadatas).getColumnMetadata("test-name-3"))
            .withMessage("Column name 'TEST-NAME-3' does not exist in column names [TEST-NAME-1, TEST-NAME-2]");
    }

    @Test
    void getColumnMetadataName() {
        assertThat(new H2RowMetadata(this.columnMetadatas).getColumnMetadata("test-name-2"))
            .isEqualTo(new H2ColumnMetadata(codecs, "TEST-NAME-2", 400, NULLABLE, 300L, 600));
    }

    @Test
    void getColumnMetadataNoIdentifier() {
        assertThatIllegalArgumentException().isThrownBy(() -> new H2RowMetadata(this.columnMetadatas).getColumnMetadata(null))
            .withMessage("name must not be null");
    }

    @Test
    void getColumnMetadatas() {
        assertThat(new H2RowMetadata(this.columnMetadatas).getColumnMetadatas()).containsAll(this.columnMetadatas);
    }

    @Test
    void toRowMetadata() {
        TypeInfo typeInfo = TypeInfo.TYPE_FLOAT;
        when(this.result.getVisibleColumnCount()).thenReturn(1);
        when(this.result.getColumnName(0)).thenReturn("test-name");
        when(this.result.getColumnType(0)).thenReturn(typeInfo);
        when(this.result.getNullable(0)).thenReturn(Column.NULLABLE);

        MockCodecs codecs = MockCodecs.builder()
            .preferredType(8, String.class)
            .build();

        H2RowMetadata rowMetadata = H2RowMetadata.toRowMetadata(codecs, this.result);

        assertThat(rowMetadata.getColumnMetadatas()).hasSize(1);
    }

    @Test
    void toRowMetadataNoCodecs() {
        assertThatIllegalArgumentException().isThrownBy(() -> H2RowMetadata.toRowMetadata(null, this.result))
            .withMessage("codecs must not be null");
    }

    @Test
    void toRowMetadataNoResult() {
        assertThatIllegalArgumentException().isThrownBy(() -> H2RowMetadata.toRowMetadata(MockCodecs.empty(), null))
            .withMessage("result must not be null");
    }
}
