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

import io.r2dbc.h2.util.Assert;
import io.r2dbc.spi.ColumnMetadata;
import org.h2.result.ResultInterface;

import java.util.Objects;
import java.util.Optional;

/**
 * An implementation of {@link ColumnMetadata} for an H2 database.
 */
public final class H2ColumnMetadata implements ColumnMetadata {

    private final String name;

    private final Long precision;

    private final Integer type;

    H2ColumnMetadata(String name, Long precision, Integer type) {
        this.name = Assert.requireNonNull(name, "name must not be null");
        this.precision = Assert.requireNonNull(precision, "precision must not be null");
        this.type = Assert.requireNonNull(type, "type must not be null");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        H2ColumnMetadata that = (H2ColumnMetadata) o;
        return Objects.equals(this.name, that.name) &&
            Objects.equals(this.precision, that.precision) &&
            Objects.equals(this.type, that.type);
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public Optional<Integer> getPrecision() {
        return Optional.of(this.precision)
            .map(Long::intValue);
    }

    @Override
    public Integer getType() {
        return this.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.name, this.precision, this.type);
    }

    @Override
    public String toString() {
        return "H2ColumnMetadata{" +
            "name='" + this.name + '\'' +
            ", precision=" + this.precision +
            ", type=" + this.type +
            '}';
    }

    static H2ColumnMetadata toColumnMetadata(ResultInterface result, int index) {
        Assert.requireNonNull(result, "result must not be null");

        return new H2ColumnMetadata(result.getColumnName(index), result.getColumnPrecision(index), result.getColumnType(index));
    }

}
