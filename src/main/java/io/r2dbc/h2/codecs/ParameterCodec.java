package io.r2dbc.h2.codecs;

import io.r2dbc.spi.Parameter;
import org.h2.value.Value;

final class ParameterCodec extends AbstractCodec<Parameter> {

    private final Codecs codecs;

    ParameterCodec(Codecs codecs) {
        super(Parameter.class);
        this.codecs = codecs;
    }

    @Override
    boolean doCanDecode(int dataType) {
        return true;
    }

    @Override
    Parameter doDecode(Value value, Class<? extends Parameter> type) {
        return this.codecs.decode(value, value.getValueType(), type);
    }

    @Override
    Value doEncode(Parameter value) {
        if ( value == null || value.getValue() == null) {
            return this.encodeNull();
        }

        return this.codecs.encode(value.getValue());
    }
}
