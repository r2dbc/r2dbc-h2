package io.r2dbc.h2.codecs;

import io.r2dbc.h2.util.Assert;
import org.h2.util.JSR310Utils;
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
        return (LocalDate) JSR310Utils.valueToLocalDate(value, null);
    }

    @Override
    Value doEncode(LocalDate value) {
        return JSR310Utils.localDateToValue(Assert.requireNonNull(value, "value must not be null"));
    }
}
