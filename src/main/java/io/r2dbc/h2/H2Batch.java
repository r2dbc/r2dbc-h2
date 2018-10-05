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

import static reactor.function.TupleUtils.function;

import lombok.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import io.r2dbc.spi.Batch;
import org.h2.engine.SessionInterface;
import reactor.core.publisher.Flux;

/**
 * @author Greg Turnquist
 */
@Value
public final class H2Batch implements Batch {

	private final SessionInterface session;

	private final List<String> statements = new ArrayList<>();

	H2Batch(SessionInterface session) {

		Objects.requireNonNull(session, "session must not be null");

		this.session = session;
	}

	@Override
	public H2Batch add(String sql) {

		Objects.requireNonNull(sql, "sql must not be null");

		this.statements.add(sql);

		return this;
	}

	@Override
	public Flux<H2Result> execute() {
		
		return Flux.fromIterable(this.statements)
			.flatMap(statement -> {
				if (statement.toLowerCase().startsWith("select")) {
					return H2Utils.query(this.session, statement, Integer.MAX_VALUE)
						.map(H2Result::toResult);
				} else {
					return H2Utils.update(this.session, statement)
						.map(function(H2Result::toResult));
				}
			});
	}
}
