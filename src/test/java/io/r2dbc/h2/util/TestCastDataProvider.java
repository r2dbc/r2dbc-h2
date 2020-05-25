/*
 * Copyright 2020 the original author or authors.
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

package io.r2dbc.h2.util;

import io.r2dbc.h2.client.Client;
import org.h2.api.JavaObjectSerializer;
import org.h2.engine.CastDataProvider;
import org.h2.engine.Mode;
import org.h2.engine.SessionInterface;
import org.h2.util.DateTimeUtils;
import org.h2.util.TimeZoneProvider;
import org.h2.value.ValueTimestampTimeZone;
import org.mockito.Mockito;

import static org.mockito.Mockito.when;

public enum TestCastDataProvider implements CastDataProvider {

    INSTANCE;

    public static Client mockedClient() {

        Client clientMock = Mockito.mock(Client.class);
        SessionInterface sessionMock = Mockito.mock(SessionInterface.class);

        when(clientMock.getSession()).thenReturn(sessionMock);
        when(sessionMock.currentTimeZone()).thenReturn(TestCastDataProvider.INSTANCE.currentTimeZone());
        when(sessionMock.currentTimestamp()).thenReturn(TestCastDataProvider.INSTANCE.currentTimestamp());
        when(sessionMock.getMode()).thenReturn(TestCastDataProvider.INSTANCE.getMode());


        return clientMock;
    }

    @Override
    public ValueTimestampTimeZone currentTimestamp() {
        return ValueTimestampTimeZone.fromDateValueAndNanos(0, 0, 0);
    }

    @Override
    public TimeZoneProvider currentTimeZone() {
        return new TestTimeZoneProvider();
    }

    @Override
    public Mode getMode() {
        return Mode.getRegular();
    }

    @Override
    public JavaObjectSerializer getJavaObjectSerializer() {
        throw new UnsupportedOperationException();
    }

    static class TestTimeZoneProvider extends TimeZoneProvider {

        private final int offset = 0;

        public int getTimeZoneOffsetUTC(long epochSeconds) {
            return this.offset;
        }

        public int getTimeZoneOffsetLocal(long dateValue, long timeNanos) {
            return this.offset;
        }

        public long getEpochSecondsFromLocal(long dateValue, long timeNanos) {
            return DateTimeUtils.getEpochSeconds(dateValue, timeNanos, this.offset);
        }

        public String getId() {
            return DateTimeUtils.timeZoneNameFromOffsetSeconds(this.offset);
        }

        public String getShortId(long epochSeconds) {
            return this.getId();
        }

        public boolean hasFixedOffset() {
            return true;
        }

        public String toString() {
            return "TimeZoneProvider " + this.getId();
        }
    }
}
