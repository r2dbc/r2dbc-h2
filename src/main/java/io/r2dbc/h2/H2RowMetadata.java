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
import io.r2dbc.spi.RowMetadata;
import org.h2.result.ResultInterface;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * An implementation of {@link RowMetadata} for an H2 database.
 */
public class H2RowMetadata extends ColumnSource implements RowMetadata, Collection<String> {

    H2RowMetadata(List<H2ColumnMetadata> columnMetadatas) {
        super(Assert.requireNonNull(columnMetadatas, "columnMetadatas must not be null"));
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalArgumentException if {@code identifier} does not correspond to a column
     */
    @Override
    public ColumnMetadata getColumnMetadata(Object identifier) {
        return getColumn(identifier);
    }

    @Override
    public List<H2ColumnMetadata> getColumnMetadatas() {
        return Collections.unmodifiableList(super.getColumnMetadatas());
    }

    static H2RowMetadata toRowMetadata(Codecs codecs, ResultInterface result) {
        Assert.requireNonNull(codecs, "codecs must not be null");
        Assert.requireNonNull(result, "result must not be null");

        return new H2RowMetadata(getColumnMetadatas(codecs, result));
    }

    private static List<H2ColumnMetadata> getColumnMetadatas(Codecs codecs, ResultInterface result) {
        List<H2ColumnMetadata> columnMetadatas = new ArrayList<>(result.getVisibleColumnCount());

        for (int i = 0; i < result.getVisibleColumnCount(); i++) {
            columnMetadatas.add(H2ColumnMetadata.toColumnMetadata(codecs, result, i));
        }

        return columnMetadatas;
    }

    @Override
    public Collection<String> getColumnNames() {
        return this;
    }

    @Override
    public int size() {
        return this.getColumnCount();
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean contains(Object o) {

        if (o instanceof String) {
            return this.findColumn((String) o) != null;
        }

        return false;
    }

    @Override
    public boolean containsAll(Collection<?> c) {

        for (Object o : c) {
            if (!contains(o)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public Iterator<String> iterator() {

        Iterator<H2ColumnMetadata> iterator = super.getColumnMetadatas().iterator();

        return new Iterator<String>() {


            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public String next() {
                return iterator.next().getName();
            }
        };
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T[] toArray(T[] a) {
        return (T[]) toArray();
    }

    @Override
    public Object[] toArray() {
        Object[] result = new Object[size()];

        for (int i = 0; i < size(); i++) {
            result[i] = this.getColumn(i).getName();
        }

        return result;
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
    public boolean addAll(Collection<? extends String> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }
}
