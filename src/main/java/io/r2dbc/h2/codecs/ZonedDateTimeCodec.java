package io.r2dbc.h2.codecs;

import io.r2dbc.h2.client.Client;
import io.r2dbc.h2.util.Assert;
import org.h2.engine.CastDataProvider;
import org.h2.util.JSR310Utils;
import org.h2.value.Value;

import java.time.ZonedDateTime;

final class ZonedDateTimeCodec extends AbstractCodec<ZonedDateTime> {

    private final Client client;

    ZonedDateTimeCodec(Client client) {
        super(ZonedDateTime.class);
        this.client = client;
    }

    @Override
    boolean doCanDecode(int dataType) {
        return dataType == Value.TIMESTAMP_TZ;
    }

    @Override
    ZonedDateTime doDecode(Value value, Class<? extends ZonedDateTime> type) {
        Assert.requireType(this.client.getSession(), CastDataProvider.class, "The session must implement CastDataProvider.");
        return (ZonedDateTime) JSR310Utils.valueToZonedDateTime(value, (CastDataProvider) this.client.getSession());
    }

    @Override
    Value doEncode(ZonedDateTime value) {
        return JSR310Utils.zonedDateTimeToValue(Assert.requireNonNull(value, "value must not be null"));
    }
}
