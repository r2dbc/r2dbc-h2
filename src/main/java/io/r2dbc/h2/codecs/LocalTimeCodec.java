package io.r2dbc.h2.codecs;

import io.r2dbc.h2.client.Client;
import io.r2dbc.h2.util.Assert;
import org.h2.util.JSR310Utils;
import org.h2.value.Value;

import java.time.LocalTime;

final class LocalTimeCodec extends AbstractCodec<LocalTime> {

    private final Client client;

    LocalTimeCodec(Client client) {
        super(LocalTime.class);
        this.client = client;
    }

    @Override
    boolean doCanDecode(int dataType) {
        return dataType == Value.TIME;
    }

    @Override
    LocalTime doDecode(Value value, Class<? extends LocalTime> type) {
        return (LocalTime) JSR310Utils.valueToLocalTime(value, this.client.getSession());
    }

    @Override
    Value doEncode(LocalTime value) {
        return JSR310Utils.localTimeToValue(Assert.requireNonNull(value, "value must not be null"));
    }
}
