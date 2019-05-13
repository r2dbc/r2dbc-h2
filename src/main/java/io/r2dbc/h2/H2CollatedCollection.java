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

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Spliterator;
import java.util.TreeSet;

final class H2CollatedCollection implements Collection<String> {

    private final List<String> columns;

    private final TreeSet<String> lookup;

    H2CollatedCollection(List<String> columns) {
        this.columns = Collections.unmodifiableList(columns);
        this.lookup = new TreeSet<>(String::compareToIgnoreCase);
        this.lookup.addAll(columns);
    }

    @Override
    public int size() {
        return this.columns.size();
    }

    @Override
    public boolean isEmpty() {
        return this.columns.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return this.lookup.contains(o);
    }

    @Override
    public Iterator<String> iterator() {
        return this.columns.iterator();
    }

    @Override
    public Object[] toArray() {
        return this.columns.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return this.columns.toArray(a);
    }

    @Override
    public boolean add(String s) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return this.lookup.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends String> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Spliterator<String> spliterator() {
        return this.columns.spliterator();
    }

    @Override
    public String toString() {
        return this.columns.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof H2CollatedCollection)) {
            return false;
        }
        H2CollatedCollection other = (H2CollatedCollection) o;
        return Objects.equals(this.columns, other.columns);
    }

    @Override
    public int hashCode() {
        return this.columns.hashCode();
    }
}
