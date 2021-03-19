/*
 * Copyright 2021 the original author or authors.
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

import io.r2dbc.spi.Type;
import org.h2.value.TypeInfo;
import reactor.util.annotation.Nullable;

import java.util.Objects;

/**
 * An implementation of {@link Type} for an H2 database.
 */
public class H2Type implements Type {

    private final TypeInfo typeInfo;

    @Nullable
    private Class<?> javaType;

    H2Type(TypeInfo typeInfo, @Nullable Class<?> javaType) {
        this.typeInfo = typeInfo;
        this.javaType = javaType;
    }

    @Override
    public Class<?> getJavaType() {
        return this.javaType;
    }

    @Override
    public String getName() {
        return this.typeInfo.toString();
    }

    TypeInfo getTypeInfo() {
        return typeInfo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof H2Type)) return false;
        H2Type h2Type = (H2Type) o;
        return Objects.equals(typeInfo, h2Type.typeInfo) && Objects.equals(javaType, h2Type.javaType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(typeInfo, javaType);
    }

    @Override
    public String toString() {
        return "H2Type{" +
            "typeInfo=" + typeInfo +
            ", javaType=" + javaType +
            '}';
    }
}
