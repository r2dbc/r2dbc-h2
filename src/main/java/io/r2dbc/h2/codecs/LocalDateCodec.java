package io.r2dbc.h2.codecs;

import io.r2dbc.h2.util.Assert;
import org.h2.util.LocalDateTimeUtils;
import org.h2.value.Value;

import java.time.LocalDate;

final class LocalDateCodec extends AbstractCodec<LocalDate> {

    LocalDateCodec() {
        super(LocalDate.class);
    }

    @Override
    boolean doCanDecode(int dataType) {
        return dataType == Value.DATE;
    }

    @Override
    LocalDate doDecode(Value value, Class<? extends LocalDate> type) {
        return (LocalDate) LocalDateTimeUtils.valueToLocalDate(value);
    }

    @Override
    Value doEncode(LocalDate value) {
        return LocalDateTimeUtils.localDateToDateValue(Assert.requireNonNull(value, "value must not be null"));
    }
}
