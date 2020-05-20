package io.r2dbc.h2.codecs;

import io.r2dbc.h2.util.Assert;
import org.h2.api.Interval;
import org.h2.value.Value;
import org.h2.value.ValueInterval;

final class IntervalCodec extends AbstractCodec<Interval> {
    IntervalCodec() {
        super(Interval.class);
    }

    @Override
    boolean doCanDecode(int dataType) {
        return Value.INTERVAL_YEAR <= dataType && dataType <= Value.INTERVAL_MINUTE_TO_SECOND;
    }

    @Override
    Interval doDecode(Value value, Class<? extends Interval> type) {
        ValueInterval valueInterval = (ValueInterval) value;
        return new Interval(valueInterval.getQualifier(), valueInterval.isNegative(), valueInterval.getLeading(), valueInterval.getRemaining());
    }

    @Override
    Value doEncode(Interval value) {
        Assert.requireNonNull(value, "value must not be null");
        return ValueInterval.from(value.getQualifier(), value.isNegative(), value.getLeading(), value.getRemaining());
    }
}
