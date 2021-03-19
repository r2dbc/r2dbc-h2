package io.r2dbc.h2;

import io.r2dbc.h2.util.Assert;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.Option;
import io.r2dbc.spi.TransactionDefinition;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

final class H2TransactionDefinition implements TransactionDefinition {

    public static final H2TransactionDefinition EMPTY = new H2TransactionDefinition(Collections.emptyMap());

    private final Map<Option<?>, Object> options;


    private H2TransactionDefinition(Map<Option<?>, Object> options) {
        this.options = options;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getAttribute(Option<T> option) {
        return (T) this.options.get(option);
    }

    public H2TransactionDefinition with(Option<?> option, Object value) {
        Map<Option<?>, Object> options = new HashMap<>(this.options);
        options.put(Assert.requireNonNull(option, "option must not be null"), Assert.requireNonNull(value, "value must not be null"));

        return new H2TransactionDefinition(options);
    }

    public H2TransactionDefinition isolationLevel(IsolationLevel isolationLevel) {
        return with(H2TransactionDefinition.ISOLATION_LEVEL, isolationLevel);
    }
}
