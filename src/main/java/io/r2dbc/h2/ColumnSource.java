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

import io.r2dbc.h2.util.Assert;
import reactor.util.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Object that provides access to {@link H2ColumnMetadata}s by index and by name. Column index starts at {@literal 0} (zero-based index).
 */
abstract class ColumnSource {

    private final List<H2ColumnMetadata> columns;

    private final Map<String, H2ColumnMetadata> nameKeyedColumns;

    ColumnSource(List<H2ColumnMetadata> columns) {
        this.columns = columns;
        this.nameKeyedColumns = getNameKeyedColumns(columns);
    }

    private static Map<String, H2ColumnMetadata> getNameKeyedColumns(List<H2ColumnMetadata> columns) {

        if (columns.size() == 1) {
            return Collections.singletonMap(columns.get(0).getName(), columns.get(0));
        }

        Map<String, H2ColumnMetadata> byName = new LinkedHashMap<>(columns.size(), 1);

        for (H2ColumnMetadata column : columns) {
            H2ColumnMetadata old = byName.put(column.getName(), column);
            if (old != null) {
                byName.put(column.getName(), old);
            }
        }

        return byName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ColumnSource)) {
            return false;
        }
        ColumnSource that = (ColumnSource) o;
        return Objects.equals(columns, that.columns) &&
            Objects.equals(nameKeyedColumns, that.nameKeyedColumns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columns, nameKeyedColumns);
    }

    List<H2ColumnMetadata> getColumnMetadatas() {
        return this.columns;
    }

    int getColumnCount() {
        return this.columns.size();
    }

    /**
     * Lookup {@link H2ColumnMetadata} by {@link #getColumn(int)} () index} or by its {@link #getColumn(String)}  name}.
     *
     * @param identifier the index or name.
     * @return the column.
     * @throws IllegalArgumentException if the column cannot be retrieved.
     * @throws IllegalArgumentException when {@code identifier} is {@code null}.
     */
    H2ColumnMetadata getColumn(Object identifier) {

        Assert.requireNonNull(identifier, "identifier must not be null");

        if (identifier instanceof Integer) {
            return getColumn((int) identifier);
        }

        if (identifier instanceof String) {
            return getColumn((String) identifier);
        }

        throw new IllegalArgumentException(String.format("Identifier '%s' is not a valid identifier. Should either be an Integer index or a String column name.", identifier));
    }

    /**
     * Lookup {@link H2ColumnMetadata} by its {@code index}.
     *
     * @param index the column index. Must be greater zero and less than the number of columns.
     * @return the {@link H2ColumnMetadata}.
     */
    H2ColumnMetadata getColumn(int index) {

        if (this.columns.size() > index && index >= 0) {
            return this.columns.get(index);
        }

        throw new IllegalArgumentException(String.format("Column index %d is larger than the number of columns %d", index, this.columns.size()));
    }

    /**
     * Lookup {@link H2ColumnMetadata} by its {@code name}.
     *
     * @param name the column name.
     * @return the {@link H2ColumnMetadata}.
     */
    H2ColumnMetadata getColumn(String name) {

        H2ColumnMetadata column = findColumn(name);

        if (column == null) {
            throw new IllegalArgumentException(String.format("Column name '%s' does not exist in column names %s", name.toUpperCase(), this.nameKeyedColumns.keySet()));
        }

        return column;
    }

    /**
     * Lookup {@link H2ColumnMetadata} by its {@code name}.
     *
     * @param name the column name.
     * @return the {@link H2ColumnMetadata}.
     */
    @Nullable
    H2ColumnMetadata findColumn(String name) {

        H2ColumnMetadata column = this.nameKeyedColumns.get(name);

        if (column == null) {
            name = getColumnName(name, this.nameKeyedColumns.keySet());
            if (name != null) {
                column = this.nameKeyedColumns.get(name);
            }
        }

        return column;
    }

    @Nullable
    private static String getColumnName(String name, Collection<String> names) {

        for (String s : names) {
            if (s.equalsIgnoreCase(name)) {
                return s;
            }
        }

        return null;
    }
}
