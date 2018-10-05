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

import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * @author Ben Hale
 * @author Greg Turnquist
 */
@ToString
@EqualsAndHashCode
final class Binding {

	private final SortedMap<Integer, Object> parameters = new TreeMap<>();

	Binding add(Integer index, Object parameter) {

		Objects.requireNonNull(index, "index must not be null");
		Objects.requireNonNull(parameter, "parameter must not be null");

		this.parameters.put(index, parameter);

		return this;
	}

	Set<Map.Entry<Integer, Object>> getParameters() {
		return this.parameters.entrySet();
	}
}
