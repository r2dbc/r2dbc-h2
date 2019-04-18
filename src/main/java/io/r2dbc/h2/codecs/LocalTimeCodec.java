package io.r2dbc.h2.codecs;

import io.r2dbc.h2.util.Assert;
import org.h2.value.Value;
import org.h2.value.ValueTime;

import java.sql.Time;
import java.time.LocalTime;

final class LocalTimeCodec extends AbstractCodec<LocalTime> {
    LocalTimeCodec() {
        super(LocalTime.class);
    }

    @Override
    boolean doCanDecode(int dataType) {
        return dataType == Value.TIME;
    }

    @Override
    LocalTime doDecode(Value value, Class<? extends LocalTime> type) {
        return value.getTime().toLocalTime();
    }

    @Override
    Value doEncode(LocalTime value) {
        return ValueTime.get(Time.valueOf(Assert.requireNonNull(value, "value must not be null")));
    }
}
