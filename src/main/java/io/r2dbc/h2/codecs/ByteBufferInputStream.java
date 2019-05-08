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

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Borrowed from JDK 8's {@link sun.security.ssl.ByteBufferInputStream}.
 *
 * @author  Brad Wetmore
 */
class ByteBufferInputStream extends InputStream {

    ByteBuffer bb;

    ByteBufferInputStream(ByteBuffer bb) {
        this.bb = bb;
    }

    /**
     * Returns a byte from the ByteBuffer.
     *
     * Increments position().
     */
    @Override
    public int read() throws IOException {

        if (bb == null) {
            throw new IOException("read on a closed InputStream");
        }

        if (bb.remaining() == 0) {
            return -1;
        }

        return (bb.get() & 0xFF);   // need to be in the range 0 to 255
    }

    /**
     * Returns a byte array from the ByteBuffer.
     *
     * Increments position().
     */
    @Override
    public int read(byte b[]) throws IOException {

        if (bb == null) {
            throw new IOException("read on a closed InputStream");
        }

        return read(b, 0, b.length);
    }

    /**
     * Returns a byte array from the ByteBuffer.
     *
     * Increments position().
     */
    @Override
    public int read(byte b[], int off, int len) throws IOException {

        if (bb == null) {
            throw new IOException("read on a closed InputStream");
        }

        if (b == null) {
            throw new NullPointerException();
        } else if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }

        int length = Math.min(bb.remaining(), len);
        if (length == 0) {
            return -1;
        }

        bb.get(b, off, length);
        return length;
    }

    /**
     * Skips over and discards <code>n</code> bytes of data from this input
     * stream.
     */
    @Override
    public long skip(long n) throws IOException {

        if (bb == null) {
            throw new IOException("skip on a closed InputStream");
        }

        if (n <= 0) {
            return 0;
        }

        /*
         * ByteBuffers have at most an int, so lose the upper bits.
         * The contract allows this.
         */
        int nInt = (int) n;
        int skip = Math.min(bb.remaining(), nInt);

        bb.position(bb.position() + skip);

        return nInt;
    }

    /**
     * Returns the number of bytes that can be read (or skipped over)
     * from this input stream without blocking by the next caller of a
     * method for this input stream.
     */
    @Override
    public int available() throws IOException {

        if (bb == null) {
            throw new IOException("available on a closed InputStream");
        }

        return bb.remaining();
    }

    /**
     * Closes this input stream and releases any system resources associated
     * with the stream.
     *
     * @exception  IOException  if an I/O error occurs.
     */
    @Override
    public void close() throws IOException {
        bb = null;
    }

    /**
     * Marks the current position in this input stream.
     */
    @Override
    public synchronized void mark(int readlimit) {}

    /**
     * Repositions this stream to the position at the time the
     * <code>mark</code> method was last called on this input stream.
     */
    @Override
    public synchronized void reset() throws IOException {
        throw new IOException("mark/reset not supported");
    }

    /**
     * Tests if this input stream supports the <code>mark</code> and
     * <code>reset</code> methods.
     */
    @Override
    public boolean markSupported() {
        return false;
    }
}
