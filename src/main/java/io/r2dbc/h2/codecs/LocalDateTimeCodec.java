package io.r2dbc.h2.codecs;

import io.r2dbc.h2.util.Assert;
import org.h2.value.Value;
import org.h2.value.ValueTimestamp;

import java.sql.Timestamp;
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
        return value.getTimestamp().toLocalDateTime();
    }

    @Override
    Value doEncode(LocalDateTime value) {
        return ValueTimestamp.get(Timestamp.valueOf(Assert.requireNonNull(value, "value must not be null")));
    }
}
