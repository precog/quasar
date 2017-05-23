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

/*
 * Javolution - Java(TM) Solution for Real-Time and Embedded Systems
 * Copyright (C) 2008 - Javolution (http://javolution.org/)
 * All rights reserved.
 *
 * Permission to use, copy, modify, and distribute this software is
 * freely granted, provided that this notice is preserved.
 *
 * 20-sep-2012:
 * copied to quasar.precog.util.BitSet to allow some small additions to the API.
 */

import java.util.Collection;
import java.util.Set;

/**
 * <p> This class represents either a table of bits or a set of non-negative
 *     numbers.</p>
 *
 * <p> This class is integrated with the collection framework (as
 *     a set of {@link Index indices} and obeys the collection semantic
 *     for methods such as {@link #size} (cardinality) or {@link #equals}
 *     (same set of indices).</p>
 *
 * @author  <a href="mailto:jean-marie@dautelle.com">Jean-Marie Dautelle</a>
 * @version 5.3, February 24, 2008
 */
public class BitSet {
    /**
     * Holds the bits (64 bits per long).
     */
    private long[] bits;

    public long[] getBits() {
        return bits;
    }

    public void setBits(long[] arr) {
        bits = arr;
        _length = arr.length;
    }

    /**
     * Holds the length in words (long) of this bit set.
     * Any word at or above the current length should be ignored (assumed
     * to be zero).
     */
    private int _length;

    public int length() { return _length; }

    public int getBitsLength() {
        return _length;
    }

    /**
     * Creates a bit set of small initial capacity. All bits are initially
     * {@code false}.
     */
    public BitSet() {
        this(64);
    }

    /**
     * Creates a bit set of specified initial capacity (in bits).
     * All bits are initially {@code false}.  This
     * constructor reserves enough space to represent the integers
     * from {@code 0} to {@code bitSize-1}.
     *
     * @param bitSize the initial capacity in bits.
     */
    public BitSet(int bitSize) {
        _length = ((bitSize - 1) >> 6) + 1;
        bits = new long[_length];
    }

    /**
     * Creates a bitset given the bits array and length.
     *
     * Used internally by copy.
     */
    private BitSet(long[] _bits, int _len) {
        bits = _bits;
        _length = _len;
    }

    /**
     * Adds the specified index to this set. This method is equivalent
     * to <code>set(index.intValue())</code>.
     *
     * @param index the integer value to be appended to this set.
     * @return {@code true} if this set did not contains the specified
     *         index; {@code false} otherwise.
     */
    public boolean add( Index index) {
        int bitIndex = ((Index) index).intValue();
        if (this.get(bitIndex)) return false; // Already there.
        set(bitIndex);
        return true;
    }

    /**
     * Performs the logical AND operation on this bit set and the
     * given bit set. This means it builds the intersection
     * of the two sets. The result is stored into this bit set.
     *
     * @param that the second bit set.
     */
    public void and(BitSet that) {
        final int n = java.lang.Math.min(this._length, that._length);
        for (int i = 0; i < n; ++i) {
            this.bits[i] &= that.bits[i];
        }
        this._length = n;
    }

    /**
     * Performs the logical AND operation on this bit set and the
     * complement of the given bit set.  This means it
     * selects every element in the first set, that isn't in the
     * second set. The result is stored into this bit set.
     *
     * @param that the second bit set
     */
    public void andNot(BitSet that) {
        int i = Math.min(this._length, that._length);
        while (--i >= 0) {
            this.bits[i] &= ~that.bits[i];
        }
    }

    /**
     * Returns the number of bits set to {@code true} (or the size of this
     * set).
     *
     * @return the number of bits being set.
     */
    public int cardinality() {
        int sum = 0;
        for (int i = 0; i < _length; i++) {
            sum += java.lang.Long.bitCount(bits[i]);
        }
        return sum;
    }

    /**
     * Sets all bits in the set to {@code false} (empty the set).
     */
    public void clear() {
        _length = 0;
    }

    /**
     * Removes the specified integer value from this set. That is
     * the corresponding bit is cleared.
     *
     * @param bitIndex a non-negative integer.
     * @throws IndexOutOfBoundsException if {@code index < 0}
     */
    public void clear(int bitIndex) {
        int longIndex = bitIndex >> 6;
        if (longIndex >= _length)
            return;
        bits[longIndex] &= ~(1L << bitIndex);
    }

    /**
     * Sets the bits from the specified {@code fromIndex} (inclusive) to the
     * specified {@code toIndex} (exclusive) to {@code false}.
     *
     * @param  fromIndex index of the first bit to be cleared.
     * @param  toIndex index after the last bit to be cleared.
     * @throws IndexOutOfBoundsException if
     *          {@code (fromIndex < 0) | (toIndex < fromIndex)}
     */
    public void clear(int fromIndex, int toIndex) {
        if ((fromIndex < 0) || (toIndex < fromIndex))
            throw new IndexOutOfBoundsException();
        int i = fromIndex >>> 6;
        if (i >= _length)
            return; // Ensures that i < _length
        int j = toIndex >>> 6;
        if (i == j) {
            bits[i] &= ((1L << fromIndex) - 1) | (-1L << toIndex);
            return;
        }
        bits[i] &= (1L << fromIndex) - 1;
        if (j < _length) {
            bits[j] &= -1L << toIndex;
        }
        for (int k = i + 1; (k < j) && (k < _length); k++) {
            bits[k] = 0;
        }
    }

    public BitSet copy() {
        long[] _bits = new long[_length];
        System.arraycopy(bits, 0, _bits, 0, _length);
        return new BitSet(_bits, _length);
    }

    /**
     * Sets the bit at the index to the opposite value.
     *
     * @param bitIndex the index of the bit.
     * @throws IndexOutOfBoundsException if {@code bitIndex < 0}
     */
    public void flip(int bitIndex) {
        int i = bitIndex >> 6;
        setLength(i + 1);
        bits[i] ^= 1L << bitIndex;
    }

    /**
     * Sets a range of bits to the opposite value.
     *
     * @param fromIndex the low index (inclusive).
     * @param toIndex the high index (exclusive).
     * @throws IndexOutOfBoundsException if
     *          {@code (fromIndex < 0) | (toIndex < fromIndex)}
     */
    public void flip(int fromIndex, int toIndex) {
        if ((fromIndex < 0) || (toIndex < fromIndex))
            throw new IndexOutOfBoundsException();
        int i = fromIndex >>> 6;
        int j = toIndex >>> 6;
        setLength(j + 1);
        if (i == j) {
            bits[i] ^= (-1L << fromIndex) & ((1L << toIndex) - 1);
            return;
        }
        bits[i] ^= -1L << fromIndex;
        bits[j] ^= (1L << toIndex) - 1;
        for (int k = i + 1; k < j; k++) {
            bits[k] ^= -1;
        }
    }

    /**
     * Returns {@code true}> if the specified integer is in
     * this bit set; {@code false } otherwise.
     *
     * @param bitIndex a non-negative integer.
     * @return the value of the bit at the specified index.
     * @throws IndexOutOfBoundsException if {@code bitIndex < 0}
     */
    public boolean get(int bitIndex) {
        int i = bitIndex >> 6;
        return (i >= _length) ? false : (bits[i] & (1L << bitIndex)) != 0;
    }

    // added for scala compatibility
    public boolean apply(int bitIndex) {
        if (bitIndex < 0) return false;
        int i = bitIndex >> 6;
        return (i >= _length) ? false : (bits[i] & (1L << bitIndex)) != 0;
    }
    public boolean contains(int bitIndex) {
        if (bitIndex < 0) return false;
        int i = bitIndex >> 6;
        return (i >= _length) ? false : (bits[i] & (1L << bitIndex)) != 0;
    }

    /**
     * Returns the index of the next {@code false} bit, from the specified bit
     * (inclusive).
     *
     * @param fromIndex the start location.
     * @return the first {@code false} bit.
     * @throws IndexOutOfBoundsException if {@code fromIndex < 0}
     */
    public int nextClearBit(int fromIndex) {
        int offset = fromIndex >> 6;
        long mask = 1L << fromIndex;
        while (offset < _length) {
            long h = bits[offset];
            do {
                if ((h & mask) == 0) {
                    return fromIndex;
                }
                mask <<= 1;
                fromIndex++;
            } while (mask != 0);
            mask = 1;
            offset++;
        }
        return fromIndex;
    }

    /**
     * Returns the index of the next {@code true} bit, from the specified bit
     * (inclusive). If there is none, {@code -1} is returned.
     * The following code will iterates through the bit set:[code]
     *    for (int i=nextSetBit(0); i >= 0; i = nextSetBit(i + 1)) {
     *         ...
     *    }[/code]
     *
     * @param fromIndex the start location.
     * @return the first {@code false} bit.
     * @throws IndexOutOfBoundsException if {@code fromIndex < 0}
     */
    public int nextSetBit(int fromIndex) {
        int offset = fromIndex >> 6;
        long mask = 1L << fromIndex;
        while (offset < _length) {
            long h = bits[offset];
            do {
                if ((h & mask) != 0)
                    return fromIndex;
                mask <<= 1;
                fromIndex++;
            } while (mask != 0);
            mask = 1;
            offset++;
        }
        return -1;
    }

    public boolean nonEmpty() {
        return nextSetBit(0) != -1;
    }

    /**
     * Performs the logical OR operation on this bit set and the one specified.
     * In other words, builds the union of the two sets.
     * The result is stored into this bit set.
     *
     * @param that the second bit set.
     */
    public void or(BitSet that) {
        if (that._length > this._length) {
            setLength(that._length);
        }
        for (int i = that._length; --i >= 0;) {
            bits[i] |= that.bits[i];
        }
    }

    /**
     * Adds the specified integer to this set (corresponding bit is set to
     * {@code true}.
     *
     * @param bitIndex a non-negative integer.
     * @throws IndexOutOfBoundsException if {@code bitIndex < 0}
     */
    public void set(int bitIndex) {
        int i = bitIndex >> 6;
        if (i >= _length) {
            setLength(i + 1);
        }
        bits[i] |= 1L << bitIndex;
    }

    /**
     * Sets the bit at the given index to the specified value.
     *
     * @param bitIndex the position to set.
     * @param value the value to set it to.
     * @throws IndexOutOfBoundsException if {@code bitIndex < 0}
     */
    public void set(int bitIndex, boolean value) {
        if (value) {
            set(bitIndex);
        } else {
            clear(bitIndex);
        }
    }

    // added for scala compatibility
    public void update(int bitIndex, boolean value) {
        if (value) {
            set(bitIndex);
        } else {
            clear(bitIndex);
        }
    }

    /**
     * Sets the bits from the specified {@code fromIndex} (inclusive) to the
     * specified {@code toIndex} (exclusive) to {@code true}.
     *
     * @param  fromIndex index of the first bit to be set.
     * @param  toIndex index after the last bit to be set.
     * @throws IndexOutOfBoundsException if
     *          {@code (fromIndex < 0) | (toIndex < fromIndex)}
     */
    public void set(int fromIndex, int toIndex) {
        if ((fromIndex < 0) || (toIndex < fromIndex))
            throw new IndexOutOfBoundsException();
        int i = fromIndex >>> 6;
        int j = toIndex >>> 6;
        setLength(j + 1);
        if (i == j) {
            bits[i] |= (-1L << fromIndex) & ((1L << toIndex) - 1);
            return;
        }
        bits[i] |= -1L << fromIndex;
        bits[j] |= (1L << toIndex) - 1;
        for (int k = i + 1; k < j; k++) {
            bits[k] = -1;
        }
    }

    /**
     * Sets the bits between from (inclusive) and to (exclusive) to the
     * specified value.
     *
     * @param fromIndex the start range (inclusive).
     * @param toIndex the end range (exclusive).
     * @param value the value to set it to.
     * @throws IndexOutOfBoundsException if {@code bitIndex < 0}
     */
    public void set(int fromIndex, int toIndex, boolean value) {
        if (value) {
            set(fromIndex, toIndex);
        } else {
            clear(fromIndex, toIndex);
        }
    }

    /**
     * Returns the cardinality of this bit set (number of bits set).
     *
     * <P>Note: Unlike {@code java.util.BitSet} this method does not
     *          returns an approximation of the number of bits of space
     *          actually in use. This method is compliant with
     *          java.util.Collection meaning for size().</p>
     *
     * @return the cardinality of this bit set.
     */
    public int size() {
        return cardinality();
    }

    /**
     * Performs the logical XOR operation on this bit set and the one specified.
     * In other words, builds the symmetric remainder of the two sets
     * (the elements that are in one set, but not in the other).
     * The result is stored into this bit set.
     *
     * @param that the second bit set.
     */
    public void xor(BitSet that) {
        if (that._length > this._length) {
            setLength(that._length);
        }
        for (int i = that._length; --i >= 0;) {
            bits[i] ^= that.bits[i];
        }
    }

    // Optimization.
    public boolean equals(Object obj) {
        if (!(obj instanceof BitSet))
            return super.equals(obj);
        BitSet that = (BitSet) obj;
        int n = java.lang.Math.min(this._length, that._length);
        for (int i = 0; i < n; ++i) {
            if (bits[i] != that.bits[i])
                return false;
        }
        for (int i = n; i < this._length; i++) {
            if (this.bits[i] != 0)
                return false;
        }
        for (int i = n; i < that._length; i++) {
            if (that.bits[i] != 0)
                return false;
        }
        return true;
    }

    // Optimization.
    public int hashCode() {
        int h = 0;
        for (int i=nextSetBit(0); i >= 0; i = nextSetBit(i + 1)) {
            h += i;
        }
        return h;
    }

    /**
     * Sets the new length of the table (all new bits are <code>false</code>).
     *
     * @param newLength the new length of the table.
     */
    private final void setLength(final int newLength) {
        if (bits.length < newLength) { // Resizes array.
            int arrayLength = bits.length;
            while (arrayLength < newLength) {
                arrayLength <<= 1;
            }
            long[] tmp = new long[arrayLength];
            System.arraycopy(bits, 0, tmp, 0, _length);
            bits = tmp;
        }
        for (int i = _length; i < newLength; i++) {
            bits[i] = 0;
        }
        _length = newLength;
    }

    private static final long serialVersionUID = 1L;
}
