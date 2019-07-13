package io.r2dbc.h2.codecs;

import io.r2dbc.h2.util.Assert;
import org.h2.value.Value;
import org.h2.value.ValueGeometry;

import org.locationtech.jts.geom.Geometry;

final class GeometryCodec extends AbstractCodec<Geometry> {

    GeometryCodec() {
        super(Geometry.class);
    }

    @Override
    boolean doCanDecode(int dataType) {
        return Value.GEOMETRY == dataType;
    }

    @Override
    Geometry doDecode(Value value, Class<? extends Geometry> type) {
        return (Geometry) value.convertTo(Value.GEOMETRY).getObject();
    }

    @Override
    Value doEncode(Geometry value) {
        return ValueGeometry.getFromGeometry(value);
    }
}
