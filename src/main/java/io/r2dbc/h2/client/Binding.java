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
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * A collection of {@link Value}s for a single bind invocation of an {@link Client}.
 * <p>
 * Parameters may be bound immediately as {@link Value}s or deferred as {@link Mono}s
 * (for example LOB streams that must not be consumed until statement execution).
 */
public final class Binding {

    static final Binding EMPTY = new Binding();

    private final SortedMap<Integer, Value> parameters = new TreeMap<>();

    private final SortedMap<Integer, Mono<Value>> deferredParameters = new TreeMap<>();

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
        this.deferredParameters.remove(index);

        return this;
    }

    /**
     * Add a deferred {@link Value} publisher to the binding. The publisher is not
     * subscribed until {@link #resolve()} is called (during statement execution).
     *
     * @param index the index of the parameter
     * @param value the deferred {@link Value}
     * @return this {@link Binding}
     * @throws NullPointerException if {@code index} or {@code value} is {@code null}
     */
    public Binding add(Integer index, Mono<Value> value) {
        Assert.requireNonNull(index, "index must not be null");
        Assert.requireNonNull(value, "value must not be null");

        this.deferredParameters.put(index, value);
        this.parameters.remove(index);

        return this;
    }

    /**
     * Resolve deferred parameters into concrete {@link Value}s.
     *
     * @return a mono emitting a binding that contains only resolved {@link Value}s
     */
    public Mono<Binding> resolve() {
        if (this.deferredParameters.isEmpty()) {
            return Mono.just(this);
        }

        return Flux.fromIterable(this.deferredParameters.entrySet())
            .concatMap(entry -> entry.getValue().map(value -> Tuples.of(entry.getKey(), value)))
            .collectList()
            .map(resolved -> {
                Binding binding = new Binding();
                this.parameters.forEach(binding::add);
                for (Tuple2<Integer, Value> tuple : resolved) {
                    binding.add(tuple.getT1(), tuple.getT2());
                }
                return binding;
            });
    }

    /**
     * Whether this binding still has unresolved deferred parameters.
     *
     * @return {@code true} if deferred parameters remain
     */
    public boolean hasDeferredParameters() {
        return !this.deferredParameters.isEmpty();
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
        return Objects.equals(this.parameters, that.parameters)
            && Objects.equals(this.deferredParameters.keySet(), that.deferredParameters.keySet());
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.parameters, this.deferredParameters.keySet());
    }

    @Override
    public String toString() {
        return "Binding{" +
            "parameters=" + this.parameters +
            ", deferredParameters=" + this.deferredParameters.keySet() +
            '}';
    }

    SortedMap<Integer, Value> getParameters() {
        return this.parameters;
    }
}
