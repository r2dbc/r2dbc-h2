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

import io.r2dbc.h2.util.Assert;
import org.h2.value.Value;

import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * A collection of {@link Value}s for a single bind invocation of an {@link Client}.
 */
public final class Binding {

    static final Binding EMPTY = new Binding();

    private final SortedMap<Integer, Value> parameters = new TreeMap<>();

    /**
     * Add a {@link Value} to the binding.
     *
     * @param index the index of the {@link Value}
     * @param value the {@link Value}
     * @return this {@link Binding}
     * @throws NullPointerException if {@code index} or {@code parameter} is {@code null}
     */
    public Binding add(Integer index, Value value) {
        Assert.requireNonNull(index, "index must not be null");
        Assert.requireNonNull(value, "value must not be null");

        this.parameters.put(index, value);

        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Binding)) {
            return false;
        }
        Binding that = (Binding) o;
        return Objects.equals(this.parameters, that.parameters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.parameters);
    }

    @Override
    public String toString() {
        return "Binding{" +
            "parameters=" + this.parameters +
            '}';
    }

    SortedMap<Integer, Value> getParameters() {
        return this.parameters;
    }
}
