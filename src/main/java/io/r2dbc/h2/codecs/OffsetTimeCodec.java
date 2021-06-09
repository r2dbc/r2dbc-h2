package io.r2dbc.h2.codecs;

import io.r2dbc.h2.client.Client;
import io.r2dbc.h2.util.Assert;
import org.h2.engine.CastDataProvider;
import org.h2.util.JSR310Utils;
import org.h2.value.Value;

import java.time.OffsetTime;

final class OffsetTimeCodec extends AbstractCodec<OffsetTime> {

    private final Client client;

    OffsetTimeCodec(Client client) {
        super(OffsetTime.class);
        this.client = client;
    }

    @Override
    boolean doCanDecode(int dataType) {
        return dataType == Value.TIME_TZ;
    }

    @Override
    OffsetTime doDecode(Value value, Class<? extends OffsetTime> type) {
        Assert.requireType(this.client.getSession(), CastDataProvider.class, "The session must implement CastDataProvider.");
        return (OffsetTime) JSR310Utils.valueToOffsetTime(value, (CastDataProvider) this.client.getSession());
    }

    @Override
    Value doEncode(OffsetTime value) {
        return JSR310Utils.offsetTimeToValue(Assert.requireNonNull(value, "value must not be null"));
    }
}
