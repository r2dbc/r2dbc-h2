package io.r2dbc.h2.codecs;

import org.h2.value.Value;
import org.h2.value.ValueGeometry;
import org.locationtech.jts.geom.Geometry;

/**
 * Maps H2's GEOMETRY data type to org.locationtech.jts.geom.Geometry class
 * Note: Geometry support is an extension and is subject to change when Geo support is added to the R2DBC SPI level.
 */
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
        return ((ValueGeometry) value.convertTo(Value.GEOMETRY)).getGeometry();
    }

    @Override
    Value doEncode(Geometry value) {
        return ValueGeometry.getFromGeometry(value);
    }
}
