package io.r2dbc.h2.codecs;

import io.r2dbc.h2.util.Assert;
import org.h2.util.LocalDateTimeUtils;
import org.h2.value.Value;

import java.time.OffsetDateTime;
import java.time.ZonedDateTime;

final class ZonedDateTimeCodec extends AbstractCodec<ZonedDateTime> {

    ZonedDateTimeCodec() {
        super(ZonedDateTime.class);
    }

    @Override
    boolean doCanDecode(int dataType) {
        return dataType == Value.TIMESTAMP_TZ;
    }

    @Override
    ZonedDateTime doDecode(Value value, Class<? extends ZonedDateTime> type) {
        return ((OffsetDateTime) LocalDateTimeUtils.valueToOffsetDateTime(value)).toZonedDateTime();
    }

    @Override
    Value doEncode(ZonedDateTime value) {
        return LocalDateTimeUtils.offsetDateTimeToValue(Assert.requireNonNull(value, "value must not be null").toOffsetDateTime());
    }
}
