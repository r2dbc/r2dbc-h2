package io.r2dbc.h2.codecs;

import java.time.Instant;

import io.r2dbc.h2.client.Client;
import io.r2dbc.h2.util.Assert;
import org.h2.engine.CastDataProvider;
import org.h2.util.JSR310Utils;
import org.h2.value.Value;

public class InstantCodec extends AbstractCodec<Instant> {

    private final Client client;

    public InstantCodec(Client client) {
        super(Instant.class);
        this.client = client;
    }

    @Override
    boolean doCanDecode(int dataType) {
        return dataType == Value.TIMESTAMP_TZ;
    }

    @Override
    Instant doDecode(Value value, Class<? extends Instant> type) {
        Assert.requireType(this.client.getSession(), CastDataProvider.class, "The session must implement CastDataProvider.");
        return (Instant) JSR310Utils.valueToInstant(value, (CastDataProvider) this.client.getSession());
    }

    @Override
    Value doEncode(Instant value) {
        return JSR310Utils.instantToValue(Assert.requireNonNull(value, "value must not be null"));
    }
}
