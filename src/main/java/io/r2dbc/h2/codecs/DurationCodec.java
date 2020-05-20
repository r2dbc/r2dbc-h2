package io.r2dbc.h2.codecs;

import io.r2dbc.h2.util.Assert;
import org.h2.util.JSR310Utils;
import org.h2.value.Value;

import java.time.Duration;

final class DurationCodec extends AbstractCodec<Duration> {
    DurationCodec() {
        super(Duration.class);
    }

    @Override
    boolean doCanDecode(int dataType) {
        return dataType != Value.INTERVAL_YEAR_TO_MONTH && Value.INTERVAL_DAY <= dataType && dataType <= Value.INTERVAL_MINUTE_TO_SECOND;
    }

    @Override
    Duration doDecode(Value value, Class<? extends Duration> type) {
        return (Duration) JSR310Utils.valueToDuration(value);
    }

    @Override
    Value doEncode(Duration value) {
        Assert.requireNonNull(value, "value must not be null");
        return JSR310Utils.durationToValue(value);
    }
}
