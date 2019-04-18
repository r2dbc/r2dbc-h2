package io.r2dbc.h2.codecs;

import io.r2dbc.h2.util.Assert;
import org.h2.util.LocalDateTimeUtils;
import org.h2.value.Value;

import java.time.LocalDateTime;

final class LocalDateTimeCodec extends AbstractCodec<LocalDateTime> {

    LocalDateTimeCodec() {
        super(LocalDateTime.class);
    }

    @Override
    boolean doCanDecode(int dataType) {
        return dataType == Value.TIMESTAMP;
    }

    @Override
    LocalDateTime doDecode(Value value, Class<? extends LocalDateTime> type) {
        return (LocalDateTime) LocalDateTimeUtils.valueToLocalDateTime(value);
    }

    @Override
    Value doEncode(LocalDateTime value) {
        return LocalDateTimeUtils.localDateTimeToValue(Assert.requireNonNull(value, "value must not be null"));
    }
}
