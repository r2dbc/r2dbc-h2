/*
 * Copyright 2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.r2dbc.h2.codecs;

import io.r2dbc.h2.H2Connection;
import io.r2dbc.h2.H2Result;
import io.r2dbc.h2.util.IntegrationTestSupport;
import org.h2.api.Interval;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link DefaultCodecs} testing all known codecs with pre-defined values and {@code null} values.
 */
public class CodecIntegrationTests extends IntegrationTestSupport {

    static {
        Hooks.onOperatorDebug();
    }

    @Test
    void shouldEncodeBooleanAsBit() {
        testType(connection, "BIT", true);
    }

    @Test
    void shouldEncodeBooleanAsBoolean() {
        testType(connection, "BOOLEAN", true, Boolean.class, true);
    }

    @Test
    void shouldEncodeBooleanAsBool() {
        testType(connection, "BOOL", true, Boolean.class, true);
    }

    @Test
    void shouldEncodeByteAsTinyint() {
        testType(connection, "TINYINT", (byte) 0x42);
    }

    @Test
    void shouldEncodeShortAsSmallint() {
        testType(connection, "SMALLINT", Short.MAX_VALUE);
    }

    @Test
    void shouldEncodeIntegerAInt() {
        testType(connection, "INT", Integer.MAX_VALUE);
    }

    @Test
    void shouldEncodeLongABigint() {
        testType(connection, "BIGINT", Long.MAX_VALUE);
    }

    @Test
    void shouldEncodeFloatAsReal() {
        testType(connection, "REAL", Float.MAX_VALUE);
    }

    @Test
    void shouldEncodeDoubleAsFloat() {
        testType(connection, "FLOAT", Double.MAX_VALUE);
    }

    @Test
    void shouldEncodeDoubleAsNumeric() {
        testType(connection, "NUMERIC(38,5)", new BigDecimal("12345.12345"));
    }

    @Test
    void shouldEncodeDoubleAsDecimal() {
        testType(connection, "DECIMAL(38,5)", new BigDecimal("12345.12345"));
    }

    @Test
    void shouldEncodeDate() {
        testType(connection, "DATE", LocalDate.parse("2018-11-08"));
    }

    @Test
    void shouldEncodeTime() {
        testType(connection, "TIME(1)", LocalTime.parse("11:08:27.1"));
    }

    @Test
    void shouldEncodeDateTime() {
        testType(connection, "DATETIME", LocalDateTime.parse("2018-11-08T11:08:28.2"));
    }

    @Test
    void shouldEncodeDateTime2() {
        testType(connection, "DATETIME2", LocalDateTime.parse("2018-11-08T11:08:28.2"));
    }

    @Test
    void shouldEncodeGuid() {
        testType(connection, "uniqueidentifier", UUID.randomUUID());
    }

    @Test
    void shouldEncodeStringAsVarchar() {
        testType(connection, "VARCHAR(255)", "Hello, World!");
    }

    @Test
    void shouldEncodeStringAsNVarchar() {
        testType(connection, "NVARCHAR(255)", "Hello, World! äöü");
    }

    @Test
    void shouldEncodeByteArrayAsBinary() {
        testType(connection, "BINARY(9)", "foobarbaz".getBytes());
    }

    @Test
    void shouldEncodeByteArrayAsVarBinary() {
        testType(connection, "VARBINARY(9)", "foobarbaz".getBytes());
    }

    @Test
    void shouldEncodeIntervalAsIntervalYear() {
        testType(connection, "INTERVAL YEAR(18)", Interval.ofYears(1234567891011L));
    }

    @Test
    void shouldEncodePeriodAsIntervalYear() {
        testType(connection, "INTERVAL YEAR(18)", Period.ofYears(987654321), Interval.ofYears(987654321));
    }

    @Test
    void shouldEncodeIntervalAsIntervalMonth() {
        testType(connection, "INTERVAL MONTH(18)", Interval.ofMonths(314159265358979323L));
    }

    @Test
    void shouldEncodePeriodAsIntervalMonth() {
        testType(connection, "INTERVAL MONTH(18)", Period.ofMonths(Integer.MAX_VALUE), Interval.ofMonths(Integer.MAX_VALUE));
    }

    @Test
    void shouldEncodeIntervalAsIntervalDay() {
        testType(connection, "INTERVAL DAY(18)", Interval.ofDays(271828182845904523L));
    }

    @Test
    void shouldEncodeDurationAsIntervalDay() {
        testType(connection, "INTERVAL Day(18)", Duration.ofDays(2718281828459L), Interval.ofDays(2718281828459L));
    }

    @Test
    void shouldEncodeIntervalAsIntervalHour() {
        testType(connection, "INTERVAL HOUR(18)", Interval.ofHours(361056));
    }

    @Test
    void shouldEncodeDurationAsIntervalHour() {
        testType(connection, "INTERVAL HOUR(18)", Duration.ofHours(154869), Interval.ofHours(154869));
    }

    @Test
    void shouldEncodeIntervalAsIntervalMinute() {
        testType(connection, "INTERVAL MINUTE(18)", Interval.ofMinutes(328));
    }

    @Test
    void shouldEncodeDurationAsIntervalMinute() {
        testType(connection, "INTERVAL MINUTE(18)", Duration.ofMinutes(Integer.MAX_VALUE), Interval.ofMinutes(Integer.MAX_VALUE));
    }

    @Test
    void shouldEncodeIntervalAsIntervalSecond() {
        testType(connection, "INTERVAL SECOND(18,9)", Interval.ofSeconds(3, 141592654));
    }

    @Test
    void shouldEncodeDurationAsIntervalSecond() {
        testType(connection, "INTERVAL SECOND(18,9)", Duration.ofSeconds(3, 141592654), Interval.ofSeconds(3, 141592654));
    }

    @Test
    void shouldEncodeIntervalAsYearToMonth() {
        testType(connection, "INTERVAL YEAR(18) TO MONTH", Interval.ofYearsMonths(1024, 2));
    }

    @Test
    void shouldEncodePeriodAsYearToMonth() {
        testType(connection, "INTERVAL YEAR(18) TO MONTH", Period.of(1024, 2, 0), Interval.ofYearsMonths(1024, 2));
    }

    @Test
    void shouldEncodeIntervalAsIntervalDayToHour() {
        testType(connection, "INTERVAL DAY(18) TO HOUR", Interval.ofDaysHours(365, 23));
    }

    @Test
    void shouldEncodeDurationAsIntervalDayToHour() {
        testType(connection, "INTERVAL DAY(18) TO HOUR", Duration.ofDays(365).plusHours(23), Interval.ofDaysHours(365, 23));
    }

    @Test
    void shouldEncodeIntervalAsIntervalDayToMinute() {
        testType(connection, "INTERVAL DAY(18) TO MINUTE", Interval.ofDaysHoursMinutes(666, 7, 42));
    }

    @Test
    void shouldEncodeDurationAsIntervalDayToMinute() {
        testType(connection, "INTERVAL DAY(18) TO MINUTE", Duration.ofDays(666).plusHours(7).plusMinutes(42), Interval.ofDaysHoursMinutes(666, 7, 42));
    }

    @Test
    void shouldEncodeIntervalAsIntervalDayToSecond() {
        testType(connection, "INTERVAL DAY(18) TO SECOND(9)", Interval.ofDaysHoursMinutesNanos(666, 13, 7, 31415926535L));
    }

    @Test
    void shouldEncodeDurationAsIntervalDayToSecond() {
        testType(connection, "INTERVAL DAY(18) TO SECOND(9)", Duration.ofDays(666).plusHours(13).plusMinutes(7).plusSeconds(31).plusNanos(415926535), Interval.ofDaysHoursMinutesNanos(666, 13, 7, 31415926535L));
    }

    @Test
    void shouldEncodeIntervalAsIntervalHourToMinute() {
        testType(connection, "INTERVAL HOUR(18) TO MINUTE", Interval.ofHoursMinutes(Integer.MAX_VALUE, 59));
    }

    @Test
    void shouldEncodeDurationAsIntervalHourToMinute() {
        testType(connection, "INTERVAL HOUR(18) TO MINUTE", Duration.ofHours(Integer.MAX_VALUE).plusMinutes(59), Interval.ofHoursMinutes(Integer.MAX_VALUE, 59));
    }

    @Test
    void shouldEncodeIntervalAsIntervalHourToSecond() {
        testType(connection, "INTERVAL HOUR(18) TO SECOND(9)", Interval.ofHoursMinutesNanos(13, 7, 31415926535L));
    }

    @Test
    void shouldEncodeDurationAsIntervalHourToSecond() {
        testType(connection, "INTERVAL HOUR(18) TO SECOND(9)", Duration.ofHours(13).plusMinutes(7).plusSeconds(31).plusNanos(415926535L), Interval.ofHoursMinutesNanos(13, 7, 31415926535L));
    }

    @Test
    void shouldEncodeIntervalAsIntervalMinuteToSecond() {
        testType(connection, "INTERVAL MINUTE(18) TO SECOND(9)", Interval.ofMinutesNanos(Integer.MAX_VALUE, 31415926535L));

    }

    @Test
    void shouldEncodeDurationAsIntervalMinuteToSecond() {
        testType(connection, "INTERVAL MINUTE(18) TO SECOND(9)", Duration.ofMinutes(Integer.MAX_VALUE).plusNanos(31415926535L), Interval.ofMinutesNanos(Integer.MAX_VALUE, 31415926535L));
    }

    @Test
    void shouldEncodeOffsetDateTimeAsTimestampWithTimeZone() {
        testType(connection, "TIMESTAMP(2) WITH TIME ZONE", OffsetDateTime.parse("2018-08-27T17:41:14.890+00:45"));
    }

    @Test
    void shouldEncodeZonedDateTimeAsTimestampWithTimeZone() {
        ZonedDateTime value = ZonedDateTime.of(LocalDateTime.parse("2018-11-08T11:08:28.2"), ZoneId.of("UT"));

        testType(connection, "TIMESTAMP(1) WITH TIME ZONE", value, value.toOffsetDateTime());
    }

    @Test
    void shouldDecodeScalarNull() {
        createTable(connection, "VARCHAR");

        Flux.from(connection.createStatement("INSERT INTO codec_test values($1)")
            .bind("$1", "foo")
            .execute())
            .flatMap(H2Result::getRowsUpdated)
            .as(StepVerifier::create)
            .expectNext(1)
            .verifyComplete();

        connection.createStatement("SELECT null, my_col FROM codec_test")
            .execute()
            .flatMap(it -> it.map((row, rowMetadata) -> Optional.ofNullable(row.get(0))))
            .as(StepVerifier::create)
            .expectNext(Optional.empty())
            .verifyComplete();

        connection.createStatement("SELECT null, my_col FROM codec_test")
            .execute()
            .flatMap(it -> it.map((row, rowMetadata) -> Optional.ofNullable(row.get(0, String.class))))
            .as(StepVerifier::create)
            .expectNext(Optional.empty())
            .verifyComplete();
    }

    private void testType(H2Connection connection, String columnType, Object value) {
        testType(connection, columnType, value, value.getClass(), value);
        testType(connection, "ARRAY", new Object[]{value}, Object[].class,
            actual -> assertThat(((Object[]) actual)).containsExactly(value));
    }

    private void testType(H2Connection connection, String columnType, Object value, Object expectedGetObjectValue) {
        testType(connection, columnType, value, value.getClass(), expectedGetObjectValue);
    }

    private void testType(H2Connection connection, String columnType, Object value, Class<?> valueClass, Object expectedGetObjectValue) {
        testType(connection, columnType, value, valueClass, actual -> assertThat(actual).isEqualTo(expectedGetObjectValue));
    }

    private void testType(H2Connection connection, String columnType, Object value, Class<?> valueClass, Consumer<Object> nativeValueConsumer) {

        createTable(connection, columnType);

        Flux.from(connection.createStatement("INSERT INTO codec_test values($1)")
            .bind("$1", value)
            .execute())
            .flatMap(H2Result::getRowsUpdated)
            .as(StepVerifier::create)
            .expectNext(1)
            .verifyComplete();

        if (value instanceof ByteBuffer) {
            ((ByteBuffer) value).rewind();
        }

        connection.createStatement("SELECT my_col FROM codec_test")
            .execute()
            .flatMap(it -> it.map((row, rowMetadata) -> row.get("my_col")))
            .as(StepVerifier::create)
            .consumeNextWith(nativeValueConsumer)
            .verifyComplete();

        connection.createStatement("SELECT my_col FROM codec_test")
            .execute()
            .flatMap(it -> it.map((row, rowMetadata) -> row.get("my_col")))
            .as(StepVerifier::create)
            .consumeNextWith(nativeValueConsumer)
            .verifyComplete();

        Flux.from(connection.createStatement("UPDATE codec_test SET my_col = $1")
            .bindNull("$1", value.getClass())
            .execute())
            .flatMap(H2Result::getRowsUpdated)
            .as(StepVerifier::create)
            .expectNext(1)
            .verifyComplete();

        connection.createStatement("SELECT my_col FROM codec_test")
            .execute()
            .flatMap(it -> it.map((row, rowMetadata) -> Optional.ofNullable((Object) row.get("my_col", valueClass))))
            .as(StepVerifier::create)
            .expectNext(Optional.empty())
            .verifyComplete();
    }

    private void createTable(H2Connection connection, String columnType) {

        connection.createStatement("DROP TABLE IF EXISTS codec_test").execute()
            .flatMap(H2Result::getRowsUpdated)
            .onErrorResume(e -> Mono.empty())
            .thenMany(connection.createStatement("CREATE TABLE codec_test (my_col " + columnType + ")")
                .execute().flatMap(H2Result::getRowsUpdated))
            .as(StepVerifier::create)
            .expectNext(0)
            .verifyComplete();
    }
}
