package io.r2dbc.h2.codecs;

import io.r2dbc.h2.client.Client;
import io.r2dbc.h2.util.Assert;
import org.h2.engine.CastDataProvider;
import org.h2.util.JSR310Utils;
import org.h2.value.Value;

import java.time.OffsetDateTime;

final class OffsetDateTimeCodec extends AbstractCodec<OffsetDateTime> {

    private final Client client;

    OffsetDateTimeCodec(Client client) {
        super(OffsetDateTime.class);
        this.client = client;
    }

    @Override
    boolean doCanDecode(int dataType) {
        return dataType == Value.TIMESTAMP_TZ;
    }

    @Override
    OffsetDateTime doDecode(Value value, Class<? extends OffsetDateTime> type) {
        Assert.requireType(this.client.getSession(), CastDataProvider.class, "The session must implement CastDataProvider.");
        return (OffsetDateTime) JSR310Utils.valueToOffsetDateTime(value, (CastDataProvider) this.client.getSession());
    }

    @Override
    Value doEncode(OffsetDateTime value) {
        return JSR310Utils.offsetDateTimeToValue(Assert.requireNonNull(value, "value must not be null"));
    }
}
