package io.r2dbc.h2.codecs;

import io.r2dbc.h2.client.Client;
import io.r2dbc.h2.util.Assert;
import org.h2.engine.CastDataProvider;
import org.h2.util.JSR310Utils;
import org.h2.value.Value;

import java.time.LocalDateTime;

final class LocalDateTimeCodec extends AbstractCodec<LocalDateTime> {

    private final Client client;

    LocalDateTimeCodec(Client client) {
        super(LocalDateTime.class);
        this.client = client;
    }

    @Override
    boolean doCanDecode(int dataType) {
        return dataType == Value.TIMESTAMP;
    }

    @Override
    LocalDateTime doDecode(Value value, Class<? extends LocalDateTime> type) {
        Assert.requireType(this.client.getSession(), CastDataProvider.class, "The session must implement CastDataProvider.");
        return (LocalDateTime) JSR310Utils.valueToLocalDateTime(value, (CastDataProvider) this.client.getSession());
    }

    @Override
    Value doEncode(LocalDateTime value) {
        return JSR310Utils.localDateTimeToValue(Assert.requireNonNull(value, "value must not be null"));
    }
}
