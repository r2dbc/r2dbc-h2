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
package io.r2dbc.h2.helper;

import org.h2.value.Value;
import org.h2.value.ValueDouble;
import org.h2.value.ValueInt;
import org.h2.value.ValueNull;
import org.h2.value.ValueString;

/**
 * Utility functions to help manage {@link Value}s.
 * 
 * @author Greg Turnquist
 */
public class ValueUtils {

	public static Value toValue(Object object) {

		if (object == String.class) {
			return ValueString.get((String) object);
		} else if (object instanceof Integer) {
			return ValueInt.get((Integer) object);
		} else if (object == Double.class) {
			return ValueDouble.get((Double) object);
		} else if (object == ValueNull.INSTANCE) {
			return ValueNull.INSTANCE;
		}

		throw new UnsupportedOperationException(String.format("Can't handle '%s' types", object.getClass()));
	}

	/**
	 * Convert a {@link Value} read from H2 back into a {@literal T}.
	 * 
	 * @param type
	 * @param value
	 * @param <T>
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <T> T toObject(Class<T> type, Value value) {

		if (type.isInstance(Integer.class)) {
			return (T) Integer.valueOf(value.getInt());
		}

		return (T) value.getObject();

	}

}
