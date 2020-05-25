package io.r2dbc.h2.codecs;

import org.h2.value.Value;
import org.h2.value.ValueGeometry;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

import static org.assertj.core.api.Assertions.assertThat;

final class GeometryCodecTest {
    private static final Geometry SAMPLE_GEOMETRY = new GeometryFactory().createPoint(new Coordinate(1, 1));

    @Test
    void doCanDecode() {
        GeometryCodec codec = new GeometryCodec();

        assertThat(codec.doCanDecode(Value.GEOMETRY)).isTrue();
        assertThat(codec.doCanDecode(Value.UNKNOWN)).isFalse();
        assertThat(codec.doCanDecode(Value.INTEGER)).isFalse();
    }

    @Test
    @SuppressWarnings("unchecked")
    void doDecode() {
        assertThat(new GeometryCodec().doDecode(ValueGeometry.getFromGeometry(SAMPLE_GEOMETRY), Geometry.class))
            .isEqualTo(SAMPLE_GEOMETRY);
    }

    @Test
    void doEncode() {
        assertThat(new GeometryCodec().doEncode(SAMPLE_GEOMETRY.copy()))
            .isEqualTo(ValueGeometry.getFromGeometry(SAMPLE_GEOMETRY));
    }
}
