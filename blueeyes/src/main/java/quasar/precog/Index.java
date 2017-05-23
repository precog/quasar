/*
 * Copyright 2014–2017 SlamData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar.precog;

import java.io.ObjectStreamException;

public final class Index extends Number implements Comparable<Index> {
    public static int compareUnsigned(int left, int right) {
        return (int) ((left & LONG_MASK) - (right & LONG_MASK));
    }

    private static final Index[] INSTANCES = new Index[1024];
    private static final long LONG_MASK = 0xFFFFFFFFL;

    private static final long serialVersionUID = 0x610L; // Version.

    /**
     * Holds the index unsigned {@code int} value.
     */
    private final int unsigned;

    static {
        for (int i = 0; i < INSTANCES.length; i++) {
            INSTANCES[i] = new Index(i);
        }
    }

    /**
     * Holds the index zero ({@code Index.of(0)}).
     */
    public static final Index ZERO = Index.of(0);

    /**
     * Holds the index one({@code Index.of(1)}).
     */
    public static final Index ONE = Index.of(1);

    /**
     * Holds the index maximum ({@code Index.of(0xFFFFFFFF)}).
     */
    public static final Index MAX = Index.of(-1);

    public static Index valueOf(int unsigned) {
        return Index.of(unsigned);
    }

    /**
     * Returns the index for the specified 32-bits unsigned value
     * (a preallocated instance if the specified value is small).
     *
     * @param unsigned the unsigned 32-bits value.
     * @return the corresponding index.
     */
    public static Index of(int unsigned) {
        return (unsigned >= 0) & (unsigned < INSTANCES.length) ?
            INSTANCES[unsigned] : new Index(unsigned);
    }

    /**
     * Creates an index having the specified 32-bits unsigned value.
     */
    private Index(int unsigned) {
        this.unsigned = unsigned;
    }

    @Override
    public int compareTo(Index that) {
        return Index.compareUnsigned(this.unsigned, that.unsigned);
    }

    /**
     * Compares this index with the specified 32-bits unsigned value for order.
     * @see #compareTo(Index)
     */
    public int compareTo(int unsignedValue) {
        return Index.compareUnsigned(this.unsigned, unsignedValue);
    }

    @Override
    public double doubleValue() {
        return (double) longValue();
    }

    /**
     * Indicates if this index has the same value as the one specified.
     */
    public boolean equals(Index that) {
        return (this.unsigned == that.unsigned);
    }

    /**
     * Indicates if this index has the specified 32-bits unsigned value.
     * @see #equals(Index)
     */
    public boolean equals(int unsigned) {
        return (this.unsigned == unsigned);
    }

    @Override
    public boolean equals(Object obj) {
        return (this == obj) || (obj instanceof Index) ? equals((Index) obj)
                : false;
    }

    @Override
    public float floatValue() {
        return (float) longValue();
    }

    /**
     * Returns {@code intValue()}
     */
    @Override
    public int hashCode() {
        return unsigned;
    }

    @Override
    public int intValue() {
        return unsigned;
    }

    @Override
    public long longValue() {
        return unsigned & LONG_MASK;
    }

    /**
     * Returns the index after this one.
     *
     * @throws IndexOutOfBoundsException if {@code this.equals(Index.MAX)}
     */
    public Index next() {
        if (unsigned == -1)
            throw new IndexOutOfBoundsException();
        return Index.of(unsigned + 1);
    }

    /**
     * Returns the index before this one.
     *
     * @throws IndexOutOfBoundsException if {@code this.equals(Index.ZERO)}
     */
    public Index previous() {
        if (unsigned == 0)
            throw new IndexOutOfBoundsException();
        return Index.of(unsigned - 1);
    }

    @Override
    public String toString() {
        return "" + longValue();
    }

    /**
     * Ensures index unicity during deserialization.
     */
    protected Object readResolve() throws ObjectStreamException {
        return Index.of(unsigned);
    }
}
