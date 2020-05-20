package io.r2dbc.h2.codecs;

import io.r2dbc.h2.util.Assert;
import org.h2.util.JSR310Utils;
import org.h2.value.Value;

import java.time.Period;

final class PeriodCodec extends AbstractCodec<Period> {
    PeriodCodec() {
        super(Period.class);
    }

    @Override
    boolean doCanDecode(int dataType) {
        return dataType == Value.INTERVAL_YEAR
                || dataType == Value.INTERVAL_MONTH
                || dataType == Value.INTERVAL_YEAR_TO_MONTH;
    }

    @Override
    Period doDecode(Value value, Class<? extends Period> type) {
        return (Period) JSR310Utils.valueToPeriod(value);
    }

    @Override
    Value doEncode(Period value) {
        Assert.requireNonNull(value, "value must not be null");
        return JSR310Utils.periodToValue(value);
    }
}
