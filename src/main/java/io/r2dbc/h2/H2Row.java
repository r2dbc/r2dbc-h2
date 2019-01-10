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

import io.r2dbc.h2.codecs.Codecs;
import io.r2dbc.h2.util.Assert;
import io.r2dbc.spi.Row;
import org.h2.result.ResultInterface;
import org.h2.value.Value;
import reactor.util.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * An implementation of {@link Row} for an H2 database.
 */
public final class H2Row implements Row {

    private final Codecs codecs;

    private final List<Column> columns;

    private final Map<String, Column> nameKeyedColumns;

    H2Row(List<Column> columns, Codecs codecs) {
        this.columns = Assert.requireNonNull(columns, "columns must not be null");
        this.codecs = Assert.requireNonNull(codecs, "codecs must not be null");

        this.nameKeyedColumns = getNameKeyedColumns(this.columns);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        H2Row that = (H2Row) o;
        return Objects.equals(this.columns, that.columns);
    }

    @SuppressWarnings("unchecked")
    @Nullable
    @Override
    public <T> T get(Object identifier, Class<T> type) {
        Assert.requireNonNull(identifier, "identifier must not be null");
        Assert.requireNonNull(type, "type must not be null");

        Column column;
        if (identifier instanceof Integer) {
            column = getColumn((Integer) identifier);
        } else if (identifier instanceof String) {
            column = getColumn((String) identifier);
        } else {
            throw new IllegalArgumentException(String.format("Identifier '%s' is not a valid identifier. Should either be an Integer index or a String column name.", identifier));
        }

        return this.codecs.decode(column.getValue(), column.getDataType(), type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.columns);
    }

    @Override
    public String toString() {
        return "H2Row{" +
            "columns=" + this.columns +
            ", nameKeyedColumns=" + this.nameKeyedColumns +
            '}';
    }

    static H2Row toRow(Value[] values, ResultInterface result, Codecs codecs) {
        Assert.requireNonNull(values, "values must not be null");
        Assert.requireNonNull(result, "result must not be null");
        Assert.requireNonNull(codecs, "codecs must not null");

        List<Column> columns = getColumns(values, result);

        return new H2Row(columns, codecs);
    }

    private static List<Column> getColumns(Value[] values, ResultInterface result) {
        List<Column> columns = new ArrayList<>(values.length);

        for (int i = 0; i < values.length; i++) {
            columns.add(new Column(result.getColumnType(i), result.getColumnName(i).toUpperCase(), values[i]));
        }

        return columns;
    }

    private Column getColumn(String name) {
        String normalized = name.toUpperCase();

        if (!this.nameKeyedColumns.containsKey(normalized)) {
            throw new IllegalArgumentException(String.format("Column name '%s' does not exist in column names %s", normalized, this.nameKeyedColumns.keySet()));
        }

        return this.nameKeyedColumns.get(normalized);
    }

    private Column getColumn(Integer index) {
        if (index >= this.columns.size()) {
            throw new IllegalArgumentException(String.format("Column index %d is larger than the number of columns %d", index, this.columns.size()));
        }

        return this.columns.get(index);
    }

    private Map<String, Column> getNameKeyedColumns(List<Column> columns) {
        Map<String, Column> nameKeyedColumns = new HashMap<>(columns.size());

        for (Column column : columns) {
            nameKeyedColumns.put(column.getName(), column);
        }

        return nameKeyedColumns;
    }

    static final class Column {

        private final Integer dataType;

        private final String name;

        private final Value value;

        Column(Integer dataType, String name, Value value) {
            this.dataType = Assert.requireNonNull(dataType, "dataType must not be null");
            this.name = Assert.requireNonNull(name, "name must not be null");
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Column)) {
                return false;
            }
            Column that = (Column) o;
            return Objects.equals(this.dataType, that.dataType) &&
                Objects.equals(this.name, that.name) &&
                Objects.equals(this.value, that.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.dataType, this.name, this.value);
        }

        @Override
        public String toString() {
            return "Column{" +
                "dataType=" + this.dataType +
                ", name='" + this.name + '\'' +
                ", value=" + this.value +
                '}';
        }

        private Integer getDataType() {
            return this.dataType;
        }

        private String getName() {
            return this.name;
        }

        private Value getValue() {
            return this.value;
        }

    }
}
