package io.r2dbc.h2.codecs;

import io.r2dbc.h2.util.Assert;
import org.h2.util.LocalDateTimeUtils;
import org.h2.value.Value;

import java.time.Instant;

final class InstantCodec extends AbstractCodec<Instant> {

    InstantCodec() {
        super(Instant.class);
    }

    @Override
    boolean doCanDecode(int dataType) {
        return dataType == Value.TIMESTAMP_TZ;
    }

    @Override
    Instant doDecode(Value value, Class<? extends Instant> type) {
        return (Instant) LocalDateTimeUtils.valueToInstant(value);
    }

    @Override
    Value doEncode(Instant value) {
        return LocalDateTimeUtils.instantToValue(Assert.requireNonNull(value, "value must not be null"));
    }
}
