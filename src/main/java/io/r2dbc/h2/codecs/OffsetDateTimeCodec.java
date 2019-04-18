package io.r2dbc.h2.codecs;

import io.r2dbc.h2.util.Assert;
import org.h2.util.LocalDateTimeUtils;
import org.h2.value.Value;

import java.time.OffsetDateTime;

final class OffsetDateTimeCodec extends AbstractCodec<OffsetDateTime> {

    OffsetDateTimeCodec() {
        super(OffsetDateTime.class);
    }

    @Override
    boolean doCanDecode(int dataType) {
        return dataType == Value.TIMESTAMP_TZ;
    }

    @Override
    OffsetDateTime doDecode(Value value, Class<? extends OffsetDateTime> type) {
        return (OffsetDateTime) LocalDateTimeUtils.valueToOffsetDateTime(value);
    }

    @Override
    Value doEncode(OffsetDateTime value) {
        return LocalDateTimeUtils.offsetDateTimeToValue(Assert.requireNonNull(value, "value must not be null"));
    }
}
