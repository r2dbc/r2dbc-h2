package io.r2dbc.h2.codecs;

import io.r2dbc.h2.util.Assert;
import org.h2.value.Value;
import org.h2.value.ValueDate;

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
        return transform(value.getDate());
    }

    @Override
    Value doEncode(LocalDate value) {
        return ValueDate.get(transform(Assert.requireNonNull(value, "value must not be null")));
    }

    static java.sql.Date transform(LocalDate date) {
        return java.sql.Date.valueOf(date);
    }

    static LocalDate transform(java.sql.Date date) {
        return date.toLocalDate();
    }
}
