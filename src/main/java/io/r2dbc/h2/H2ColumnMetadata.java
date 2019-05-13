/*
 * Copyright 2018 the original author or authors.
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

import io.r2dbc.h2.codecs.Codecs;
import io.r2dbc.h2.util.Assert;
import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Nullability;
import org.h2.result.ResultInterface;
import org.h2.table.Column;
import org.h2.value.TypeInfo;
import reactor.util.annotation.Nullable;

import java.util.Objects;

import static io.r2dbc.spi.Nullability.NON_NULL;
import static io.r2dbc.spi.Nullability.NULLABLE;
import static io.r2dbc.spi.Nullability.UNKNOWN;

/**
 * An implementation of {@link ColumnMetadata} for an H2 database.
 */
public final class H2ColumnMetadata implements ColumnMetadata {

    private final Class<?> javaType;

    private final String name;

    private final Integer nativeType;

    private final Nullability nullability;

    private final Long precision;

    private final Integer scale;

    H2ColumnMetadata(@Nullable Class<?> javaType, String name, Integer nativeType, Nullability nullability, Long precision, Integer scale) {
        this.javaType = javaType;
        this.name = Assert.requireNonNull(name, "name must not be null");
        this.nativeType = Assert.requireNonNull(nativeType, "nativeType must not be null");
        this.nullability = Assert.requireNonNull(nullability, "nullability must not be null");
        this.precision = Assert.requireNonNull(precision, "precision must not be null");
        this.scale = Assert.requireNonNull(scale, "scale must not be null");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof H2ColumnMetadata)) {
            return false;
        }
        H2ColumnMetadata that = (H2ColumnMetadata) o;
        return Objects.equals(this.javaType, that.javaType) &&
            this.name.equals(that.name) &&
            this.nativeType.equals(that.nativeType) &&
            this.nullability == that.nullability &&
            this.precision.equals(that.precision) &&
            this.scale.equals(that.scale);
    }

    @Override
    public Class<?> getJavaType() {
        return this.javaType;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public Object getNativeTypeMetadata() {
        return this.nativeType;
    }

    @Override
    public Nullability getNullability() {
        return this.nullability;
    }

    @Override
    public Integer getPrecision() {
        return this.precision.intValue();
    }

    @Override
    public Integer getScale() {
        return this.scale;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.javaType, this.name, this.nativeType, this.nullability, this.precision, this.scale);
    }

    @Override
    public String toString() {
        return "H2ColumnMetadata{" +
            "javaType=" + this.javaType +
            ", name='" + this.name + '\'' +
            ", nativeType=" + this.nativeType +
            ", nullability=" + this.nullability +
            ", precision=" + this.precision +
            ", scale=" + this.scale +
            '}';
    }

    static H2ColumnMetadata toColumnMetadata(Codecs codecs, ResultInterface result, int index) {
        Assert.requireNonNull(codecs, "codecs must not be null");
        Assert.requireNonNull(result, "result must not be null");

        TypeInfo typeInfo = result.getColumnType(index);

        String alias = result.getAlias(index);
        return new H2ColumnMetadata(codecs.preferredType(typeInfo.getValueType()), alias, typeInfo.getValueType(), toNullability(result.getNullable(index)),
            typeInfo.getPrecision(), typeInfo.getScale());
    }

    private static Nullability toNullability(int n) {
        switch (n) {
            case Column.NOT_NULLABLE:
                return NON_NULL;
            case Column.NULLABLE:
                return NULLABLE;
            default:
                return UNKNOWN;
        }
    }

}
