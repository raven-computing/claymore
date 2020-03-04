/* 
 * Copyright (C) 2020 Raven Computing
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.raven.common.struct;

import java.io.Serializable;
import java.util.Arrays;

/**
 * A low-level implementation of an array of bits. A bit can either be 1 (true)
 * or 0 (false). A bit with a value of 1 is considered to be <i>set</i> whereas
 * a bit with a value of 0 (zero) is considered to be <i>unset</i>.
 * 
 * <p>Every bit in a BitVector is addressable directly by an index, with the
 * first bit in the vector occupying index zero. The value of a bit can be
 * queried by calling the {@link #get(int)} method. The index must not be
 * equal to or greater than the size of the BitVector when getting a bit value.
 * This restriction does not apply, however, when using an index to set a bit to
 * a specific value which can be done by using
 * the {@link #set(int, boolean)} method. This means that it is a legal
 * operation to deliberately set bits outside of the (positive) vector bounds.
 * <br>A BitVector will resize itself if necessary in order to be able to hold
 * all bits as set by a caller. If a bit is set at an index greater than the
 * size of the BitVector then all positions in between the set bit and the
 * current size will be filled with 0s (zeros) after the BitVector
 * has been resized.
 * 
 * <p>The size of a BitVector can be inspected with the {@link #size()} method.
 * It will return the total number of bits within a specific BitVector. This may
 * be different though from the actual size of the underlying data structure
 * holding the bits. Not all bit positions of an internal data block have to be
 * occupied at a given time and the BitVector may introduce a space buffer when
 * deemed appropriate in order to reduce the resizing operation frequency.<br>
 * The actual length of the internally used array can be examined
 * with the {@link #capacity()} method.
 *  
 * <p>This implementation differs in a number of ways
 * from <code>java.util.BitSet</code>. A BitVector object stores all bits in
 * an internal array of bytes instead of an array of longs. This allows easier
 * and more efficient operations when working with arbitrary binary data which
 * often is represented as a byte array. Hence in some situations additional copy
 * and conversion steps can be omitted. A BitVector is therefore perfectly suitable
 * to make every single bit in an array of bytes indexable.<br>
 * Every byte array can be wrapped into a BitVector by using the static
 * {@link BitVector#wrap(byte[])} method. Wrapping does not require any copying
 * of data and is therefore a very efficient operation. It uses the reference to
 * the byte array directly, so any modifications made to the byte array externally
 * by other code or by the BitVector itself will be globally visible.
 * 
 * <p>Unlike the standard java BitSet implementation, a BitVector has no default
 * value for the stored bits. The value of each bit must be explicitly set by
 * a method provided by this class. In contrast to the standard java BitSet, it
 * is not possible to construct a BitVector which has all bits automatically
 * initialized with zeros. If a BitVector with a certain number of bits initialized
 * with a specific value is wanted, then the static 
 * {@link BitVector#createInitialized(int, boolean)} utility method can be used.
 * 
 * <p>BitVectors with a specific preset bit pattern can be created by using one
 * of the static utility methods. For exmaple, the following code will
 * produce equal BitVectors:
 * <pre><code>
 * BitVector vec1 = BitVector.valueOf("00001101");
 * byte b = 13;
 * BitVector vec2 = BitVector.valueOf(b);
 * BitVector vec3 = BitVector.fromHexString("0d");
 * </code></pre>
 * 
 * Be aware that the interpretation of the bit pattern of a BitVector is solely up
 * to the user. A BitVector is only an ordered collection of bits. For example, when
 * interpreting a BitVector as a number then any leading zeros must be considered:
 * <pre><code>
 * BitVector vec1 = BitVector.valueOf("00001101");
 * BitVector vec2 = BitVector.valueOf("1101");
 * System.out.println(vec1.equals(vec2)); //false
 * </code></pre>
 * 
 * The BitVector class provides static utility methods to work with primitive
 * numbers, for example:
 * <pre><code>
 * byte b = -93;
 * BitVector vec = BitVector.valueOf(b);
 * System.out.println(vec); //10100011
 * //Set the most significant bit to zero
 * vec.set(0, false);
 * System.out.println(vec); //00100011
 * </code></pre>
 * 
 * <p>A BitVector can be queried via the {@link #bitsSet()} method to get the
 * number of 1s it currently contains, sometimes referred to as the cardinality.
 * The same can be done for the number of 0s the BitVector contains at a given
 * moment by calling the {@link #bitsUnset()} method. This implementation tries to
 * optimize he number of times it has to iterate through the internal data structure
 * when the total amount of either 1s or 0s is queried. This is done by caching the
 * amount of 1s in an internal member variable. If the number of 1s or 0s is
 * queried and the cache is in an invalid state then the current amount is
 * computed by a linear operation and then cached so that subsequent queries
 * can be served from the cache instead, which can be done in constant time.<br>
 * Some operations, however, will cause the internal cache to be invalidated to
 * avoid additional overhead. This will then cause subsequent queries for the
 * number of 1s or 0s to have to compute the cache again first.
 *  
 * <p>A BitVector is {@link Cloneable}, {@link Serializable}
 *  
 * <p>This implementation is NOT thread-safe.
 * 
 * @author Phil Gaiser
 * @since 3.0.0
 *
 */
public final class BitVector implements Cloneable, Serializable {
    
    private static final long serialVersionUID = 1L;
    
    /**
     * The internal array holding the state of all bits
     */
    private byte[] bits;
    
    /**
     * Index pointer indicating the next free bit position
     */
    private int next;
    
    /**
     * Internal cache of the counted 1s within the BitVector. This is initially
     * set to -1 which indicates that the bits have not been counted yet. After
     * the first call to either <code>bitsSet()</code> or <code>bitsUnset()</code>
     * this variable will hold the number of 1s counted and will be continuously
     * updated to reflect any changes to the vector. Some methods may reset this
     * variable to -1 to invalidate the cache which will cause subsequent read
     * requests to do a full loop again
     * 
     */
    private int ones = -1;
    
    /**
     * Constructs an empty <code>BitVector</code>
     */
    public BitVector(){
        this(0);
    }
    
    /**
     * Constructs an empty <code>BitVector</code> with the specified initial
     * capacity in bits
     * 
     * @param initialCapacity The initial bit capacity of the BitVector
     *                      to be constructed
     */
    public BitVector(final int initialCapacity){
        this.bits = new byte[
                ((initialCapacity%8) != 0)
                ? ((initialCapacity >> 3) + 1)
                : (initialCapacity >> 3)];
    }
    
    /**
     * Constructs a <code>BitVector</code> which has the same bits
     * set as the BitVector provided to this constructor. This essentially
     * creates a copy of the specified BitVector
     * 
     * @param vec The <code>BitVector</code> to copy
     * @see BitVector#clone()
     */
    public BitVector(final BitVector vec){
        final int length = vec.bits.length;
        this.bits = new byte[length];
        for(int i=0; i<length; ++i){
            bits[i] = vec.bits[i];
        }
        this.next = vec.next;
        this.ones = vec.ones;
    }
    
    /**
     * Private constructor to create a BitVector from the specified byte array
     * 
     * @param bits The byte array to be used by the BitVector to be constructed
     */
    private BitVector(final byte[] bits){
      this.bits = bits;
      this.next = bits.length * 8;
    }
    
    /**
     * Gets the value of the bit at the specified index.<br>
     * The index must not be greater than or equal to this BitVector's size
     * 
     * @param index The index of the bit to get
     * @return True if the bit at the specified index is 1
     *         or false if the bit at the specified index is 0 (zero)
     */
    public boolean get(final int index){
        check(index);
        return ((bits[index >> 3] & (0x80 >>> (index%8))) != 0);
    }

    /**
     * Gets all bits from the specified start index (inclusive) to
     * the specified end index (exclusive) as a BitVector.<br>
     * The end index must not be greater than or equal to this BitVector's size
     * 
     * @param start The index from which to start getting bits (inclusive)
     * @param end The index to which to get bits from (exclusive)
     * @return A <code>BitVector</code> containing the bits from this BitVector at
     *         the specified start index to the specified end index
     */
    public BitVector get(final int start, final int end){
        checkRangeIn(start, end);
        final int length = (end - start);
        final int blocks = (((length%8) != 0) ? ((length >> 3) + 1) : (length >> 3));
        final byte[] bytes = new byte[blocks];
        int pos = start;
        for(int i=0; i<length; ++i){
            final int mod = (pos%8);
            bytes[i >> 3] |= (((bits[pos >> 3] & (0x80 >>> mod)) << mod) >>> (i%8));
            ++pos;
        }
        final BitVector vec = new BitVector(bytes);
        vec.next = length;
        return vec;
    }
    
    /**
     * Sets the bit at the specified index to the specified value.<br>
     * If the index is greater than the current size of this BitVector then
     * all bits between the set bit and the original size are set to 0 (zero).
     * The BitVector will be resized if necessary to match the required length
     * 
     * @param index The index of the bit to set
     * @param bit The value of the bit to set
     */
    public void set(final int index, final boolean bit){
        if(index < 0){
            throw new IndexOutOfBoundsException(String.valueOf(index));
        }
        final int block = (index >> 3);
        ensureCapacitySet(block);
        if(index >= next){
            next = (index + 1);
        }
        final int mask = (0x80 >>> (index%8));
        final boolean current = ((bits[block] & mask) != 0);
        if(bit == current){
            return;
        }
        bits[block] ^= mask;
        if(ones != -1){
            ones += (bit ? 1 : -1);
        }
    }
    
    /**
     * Sets all bits from the specified start index (inclusive) to the specified
     * end index (exclusive) to the specified value.<br>
     * The end index is allowed to be greater than the current
     * size of this BitVector.<br>
     * The BitVector will be resized if necessary to match the required length
     * 
     * @param start The index from which to start setting bits (inclusive)
     * @param end The index to which to set bits to (exclusive)
     * @param bit The value of the bits to set
     */
    public void set(final int start, final int end, final boolean bit){
        checkRangeOut(start, end);
        if(end >= next){
            next = end;
        }
        if(ones != -1){
            final int changed = (end - start);
            final int onesBefore = countOnes(start, end);
            this.ones += (bit ? (changed - onesBefore) : -onesBefore);
        }
        if(bit){
            for(int i=start; i<end; ++i){
                bits[(i >> 3)] |= (0x80 >>> (i%8));
            }
        }else{
            for(int i=start; i<end; ++i){
                bits[(i >> 3)] &= ~(0x80 >>> (i%8));
            }
        }
    }
    
    /**
     * Sets all bits to the values of the specified BitVector beginning at
     * the specified index of this BitVector.
     * This BitVector will be resized if necessary to match the required length
     * 
     * @param index The index of the first bit of this BitVector to set
     * @param vec The <code>BitVector</code> containing the bits to use to
     *            set this BitVector's values
     */
    public void set(final int index, final BitVector vec){
        if(index < 0){
            throw new IndexOutOfBoundsException(String.valueOf(index));
        }
        if(vec == null){
            throw new IllegalArgumentException("BitVector must not be null");
        }
        final int size = vec.size();
        ensureCapacitySet((index + size) >> 3);
        final byte[] content = vec.bits;
        int ptr = index;
        for(int i=0; i<size; ++i){
            set(ptr++, ((content[i >> 3] & (0x80 >>> (i%8))) != 0));
        }
    }
    
    /**
     * Adds the specified bit to end of this BitVector
     * 
     * @param bit The bit to be added to this BitVector
     */
    public void add(final boolean bit){
        final int block = (next >> 3);
        ensureCapacityAdd(block);
        if(bit){
            bits[block] |= (0x80 >>> (next % 8));
            if(ones != -1){
                ++ones;
            }
        }
        ++next;
    }
    
    /**
     * Adds all bits of the specified BitVector to end of this BitVector
     * 
     * @param vec The <code>BitVector</code> containing the bits
     *            to be added to this BitVector
     */
    public void add(final BitVector vec){
        final int size = vec.size();
        ensureCapacitySet((next + size) >> 3);
        for(int i=0; i<size; ++i){
            this.add(vec.get(i));
        }
    }

    /**
     * Inserts the specified bit into this BitVector at the specified index.
     * Shifts all bits currently at that position and any subsequent bits to
     * the right (adds one to their indices).
     * 
     * <p>The insertion index must not exceed the size of this BitVector
     * 
     * @param index The index at which the specified bit is to be inserted
     * @param bit The bit to be inserted
     */
    public void insertAt(final int index, final boolean bit){
        if((index < 0) || (index > next)){
            throw new IndexOutOfBoundsException(String.valueOf(index));
        }
        if(index == next){
            add(bit);
            return;
        }
        final int block = (next >> 3);
        ensureCapacityAdd(block);
        final int targetBlock = (index >> 3);
        //Shift all blocks to the right of the target block
        for(int i=block; i>targetBlock; --i){
            bits[i] = (byte) ((bits[i] & 0xff) >>> 1);
            bits[i] |= (bits[i-1] << 7);
        }
        //Shift all bits to the right of the insertion index
        final int shift = (8 - (index%8));
        for(int i=1; i<shift; ++i){
            final byte mask = (byte) ((bits[targetBlock] & (1 << i)) >>> 1);
            if(mask != 0){
                bits[targetBlock] |= mask;
            }else{
                bits[targetBlock] &= ~(1 << (i-1));
            }
        }
        //Set the bit at the insertion index
        if(bit){
            this.bits[targetBlock] |= (0x80 >>> (index%8));
        }else{
            this.bits[targetBlock] &= ~(0x80 >>> (index%8));
        }
        ++next;
        if((ones != -1) && bit){
            ++ones;
        }
    }
    
    /**
     * Inserts all bits of the specified BitVector argument into this BitVector at
     * the specified index. Shifts all bits currently at that position and any
     * subsequent bits to the right (adds <i>n</i> to their indices where <i>n</i>
     * is the size of the BitVector to be inserted).
     * 
     * <p>The insertion index must not exceed the size of this BitVector
     * 
     * @param index The index at which the specified BitVector is to be inserted
     * @param vec The <code>BitVector</code> to be inserted
     */
    public void insertAt(final int index, final BitVector vec){
        if(vec == null){
            throw new IllegalArgumentException(
                    "BitVector argument must not be null");
            
        }
        final int size = vec.size();
        for(int i=size-1; i>=0; --i){
            this.insertAt(index, vec.get(i));
        }
    }
    
    /**
     * Removes the bit at the specified index.
     * <br>This operation will essentially shift all bits on the right
     * side of the specified index to the left by one position
     * (subtracts one from their index)
     * 
     * @param index The index of the bit to be removed
     */
    public void remove(final int index){
        check(index);
        final int targetBlock = (index >> 3);
        //Adjust the internal cache if applicable
        if((ones != -1) && ((bits[targetBlock] & (0x80 >>> (index%8))) != 0)){
            --ones;
        }
        final int shift = (7 - (index%8));
        //First override the target block by shifting
        //all bits to the right of the target index to the left
        for(int i=shift; i>0; --i){
            final byte mask = (byte) (bits[targetBlock] & (1 << (i-1)));
            if(mask != 0){
                bits[targetBlock] |= (mask << 1);
            }else{
                bits[targetBlock] &= ~(1 << i);
            }
        }
        //Set the LSB of the target block to zero
        bits[targetBlock] &= 0xfe;
        //Shift all remaining blocks on the right side
        //to the left by one and replace the LSB with
        //the MSB from the next block
        final int high = (((next%8) != 0) ? (next >> 3) : ((next >> 3) - 1));
        for(int i=targetBlock; i<high; ++i){
            bits[i] &= 0xfe;//override LSB with zero
            bits[i] |= ((bits[i+1] & 0xff) >>> 7);
            bits[i+1] <<= 1;
        }
        --next;
    }
    
    /**
     * Removes all bits from the specified index (inclusive) to the
     * specified index (exclusive).
     * <br>This operation will essentially shift all bits from the
     * specified end index to the left by <i>n</i> positions
     * (subtracts one from their index), where <i>n</i> is the number of
     * bits to be removed (end - start)
     * 
     * @param start The index from which to start removing bits (inclusive)
     * @param end The index to which to remove bits (exclusive)
     */
    public void remove(final int start, final int end){
        checkRangeIn(start, end);
        final int targetBlock = (start >> 3);
        final int shift = (7 - (start%8));
        for(int i=start; i<end; ++i){
            //Adjust the internal cache if applicable
            if((ones != -1) && ((bits[targetBlock] & (0x80 >>> (start%8))) != 0)){
                --ones;
            }
            //First override the target block by shifting
            //all bits to the right of the target index to the left
            for(int j=shift; j>0; --j){
                final byte mask = (byte) (bits[targetBlock] & (1 << (j-1)));
                if(mask != 0){
                    bits[targetBlock] |= (mask << 1);
                }else{
                    bits[targetBlock] &= ~(1 << j);
                }
            }
            //Set the LSB of the target block to zero
            bits[targetBlock] &= 0xfe;
            //Shift all remaining blocks on the right side
            //to the left by one and replace the LSB with
            //the MSB from the next block
            final int high = (((next%8) != 0) ? (next >> 3) : ((next >> 3) - 1));
            for(int j=targetBlock; j<high; ++j){
                bits[j] &= 0xfe;//override LSB with zero
                bits[j] |= ((bits[j+1] & 0xff) >>> 7);
                bits[j+1] <<= 1;
            }
            --next;
        }
    }
    
    /**
     * Sets all bits of this BitVector to 0 (zero).<br>
     * This operation does neither affect the size nor the capacity
     * of this BitVector
     * 
     * @see #clear(boolean)
     */
    public void clear(){
        for(int i=0; i<bits.length; ++i){
            bits[i] = 0;
        }
        if(ones != -1){
            ones = 0;
        }
    }
    
    /**
     * Sets all bits of this BitVector to the specified value.
     * <br>This operation does neither affect the size nor the capacity
     * of this BitVector
     * 
     * @param value The value to set all bits to
     * @see #clear()
     */
    public void clear(final boolean value){
        if(value){
            for(int i=0; i<next; ++i){
                bits[i >> 3] |= (0x80 >>> (i%8));
            }
            ones = next;
        }else{
            clear();
        }
    }
    
    /**
     * Flips the bit at the specified index.<br>
     * If the bit at the given index is 1 then it will be set to 0 (zero) and if
     * the bit at the given index is 0 (zero) then it will be set
     * to 1 after this operation.<br>
     * This is equivalent to performing a <i>bitXOR1</i> operation to the bit at
     * the specified index
     * 
     * @param index The index of the bit to flip
     */
    public void flip(final int index){
        check(index);
        final int block = (index >> 3);
        final int mask = (0x80 >>> (index%8));
        final boolean isSet = ((bits[block] & mask) != 0);
        if(ones != -1){
            ones += (isSet ? -1 : 1);
        }
        bits[block] ^= mask;
    }
    
    /**
     * Flips all bits from the specified start index (inclusive) to the specified
     * end index (exclusive).
     * If a bit at a given position is 1 then it will be set to 0 (zero) and if
     * a bit at a given position is 0 (zero) then it will be set
     * to 1 after this operation.<br>
     * This is equivalent to performing a <i>bitXOR1</i> operation to all bits in
     * the specified range
     * 
     * @param start The index from which to start flipping bits (inclusive)
     * @param end The index to which to flip bits to (exclusive)
     */
    public void flip(final int start, final int end){
        checkRangeIn(start, end);
        for(int i=start; i<end; ++i){
            this.flip(i);
        }
    }
    
    /**
     * Returns the index of the first bit that is set to 1 that occurs on or
     * after the specified index. If the bit on the specfied index and all
     * subsequent bits are 0 (zero) then this method returns -1
     * 
     * @param index The index from which the BitVector is searched in ascending
     *              order, i.e. from left to right. The index is inclusive
     * @return The index of the next set bit, or -1 if there is no such bit
     */
    public int nextSetBit(final int index){
        if((index < 0) || (index > (next - 1))){
            throw new IndexOutOfBoundsException(String.valueOf(index));
        }
        for(int i=index; i<next; ++i){
            if((bits[i >> 3] & (0x80 >>> (i%8))) != 0){
                return i;
            }
        }
        return -1;
    }
    
    /**
     * Returns the index of the first bit that is set to 0 (zero) that occurs
     * on or after the specified index. If the bit on the specfied index and all
     * subsequent bits are 1 then this method returns -1
     * 
     * @param index The index from which the BitVector is searched in ascending
     *              order, i.e. from left to right. The index is inclusive
     * @return The index of the next unset bit, or -1 if there is no such bit
     */
    public int nextUnsetBit(final int index){
        if((index < 0) || (index > (next - 1))){
            throw new IndexOutOfBoundsException(String.valueOf(index));
        }
        for(int i=index; i<next; ++i){
            if((bits[i >> 3] & (0x80 >>> (i%8))) == 0){
                return i;
            }
        }
        return -1;
    }
    
    /**
     * Returns the index of the first bit that is set to 1 that occurs on or
     * prior to the specified index. If the bit on the specfied index and all
     * previous bits are 0 (zero) then this method returns -1
     * 
     * @param index The index from which the BitVector is searched in descending
     *              order, i.e. from right to left. The index is inclusive
     * @return The index of the previous set bit, or -1 if there is no such bit
     */
    public int previousSetBit(final int index){
        if((index < 0) || (index > next)){
            throw new IndexOutOfBoundsException(String.valueOf(index));
        }
        for(int i=index; i>=0; --i){
            if((bits[i >> 3] & (0x80 >>> (i%8))) != 0){
                return i;
            }
        }
        return -1;
    }
    
    /**
     * Returns the index of the first bit that is set to 0 (zero) that occurs
     * on or prior to the specified index. If the bit on the specfied index and all
     * previous bits are 1 then this method returns -1
     * 
     * @param index The index from which the BitVector is searched in descending
     *              order, i.e. from right to left. The index is inclusive
     * @return The index of the previous unset bit, or -1 if there is no such bit
     */
    public int previousUnsetBit(final int index){
        if((index < 0) || (index > next)){
            throw new IndexOutOfBoundsException(String.valueOf(index));
        }
        for(int i=index; i>=0; --i){
            if((bits[i >> 3] & (0x80 >>> (i%8))) == 0){
                return i;
            }
        }
        return -1;
    }
    
    /**
     * Returns the number of bits in this BitVector. Please note that the size of
     * a BitVector is purely determined by the sum of 1s and 0s such that
     * {@link #bitsSet()} + {@link #bitsUnset()} equals the return value
     * of this method
     * 
     * @return The number of bits in this BitVector
     */
    public int size(){
        return this.next;
    }
    
    /**
     * Returns the number of bits that this BitVector can hold before having
     * to perform a resizing operation. The return value of this method
     * devided by 8 gives the total number of bytes that this BitVector uses
     * internally to store all the bits according to its absolute capacity
     * 
     * @return The capacity of this BitVector
     * @see #size()
     */
    public int capacity(){
        return (this.bits.length * 8);
    }
    
    /**
     * Indicates whether this BitVector is empty, i.e. it has no bits.<br>
     * Please note that a BitVector with all bits set to 0 (zero) is not
     * considered empty. This method returns true if and
     * only if {@link #size()} returns 0 (zero).
     * 
     * @return True if this BitVector is empty, false if it contains
     *         at least one bit
     */
    public boolean isEmpty(){
        return (next == 0);
    }
    
    /**
     * Returns the number of set bits in this BitVector, i.e. how many bits
     * are set to <b>1</b>
     * 
     * <p>This method will iterate through the entire BitVector and count the
     * amount of 1s currently stored in it. It then stores the computed number
     * internally and updates it as the structure of this vector changes, for
     * example when adding or setting individual bits. Subsequent calls to this
     * method are therefore much faster as the cached value can be returned
     * instantly instead of having to iterate through the entire vector again.<br>
     * Some specific operations however, will result in a cache invalidation,
     * e.g. when shifting to the left or right, as keeping track of the amount of
     * set and unset bits in such operations would require significant overhead.
     * <br>This choice of behaviour has been made purely for performance reasons.
     * Users of BitVectors may introduce external counters/caches to fit their
     * specific requirements
     * 
     * @return The amount of bits set to 1 in this BitVector
     * @see #bitsUnset()
     */
    public int bitsSet(){
        ensureCounted();
        return this.ones;
    }

    /**
     * Returns the number of unset bits in this BitVector, i.e. how many bits
     * are set to <b>0 (zero)</b>
     * 
     * <p>This method will iterate through the entire BitVector and count the
     * amount of 0s currently stored in it. It then stores the computed number
     * internally and updates it as the structure of this vector changes, for
     * example when adding or setting individual bits. Subsequent calls to this
     * method are therefore much faster as the cached value can be returned
     * instantly instead of having to iterate through the entire vector again.<br>
     * Some specific operations however, will result in a cache invalidation,
     * e.g. when shifting to the left or right, as keeping track of the amount of
     * set and unset bits in such operations would require significant overhead.
     * <br>This choice of behaviour has been made purely for performance reasons.
     * Users of BitVectors may introduce external counters/caches to fit their
     * specific requirements
     * 
     * @return The amount of bits set to 0 (zero) in this BitVector
     * @see #bitsSet()
     */
    public int bitsUnset(){
        ensureCounted();
        return (next - ones);
    }
    
    /**
     * Shifts the entire bit pattern of this BitVector to the left by the
     * specified amount.
     * 
     * <p>This call will cause the internal cache for the number of bits
     * set to 1 to be reset.
     * 
     * <p>Passing a negative number to this method will perform a right
     * shift by the specified absolute amount
     * 
     * @param positions The number of positions to shift the bit pattern
     *                  to the left
     * @return This <code>BitVector</code> instance
     */
    public BitVector shiftLeft(final int positions){
        if(positions < 0){
            return shiftRight(-positions);
        }
        //First, the number of currently used blocks is computed.
        //If the vector is shifted by more than 8 bits, then there
        //is at least one entire block falling out the leftmost end.
        //Consequently, for every additional 8 bits shifted there is
        //one more block falling out. Therefore the number of whole
        //blocks is computed here so that the bytes which are guaranteed
        //to be overridden can be set in a preprocessing step.
        //The remaining bits (or all bits at once if shifted
        //by less than 8 positions) can then be shifted and modified
        //according to their adjacent blocks
        final int blocks = (((next%8) != 0) ? (next >> 3) : ((next >> 3) - 1));
        final int rounds = (positions >> 3);
        final int mod = (positions%8);
        for(int i=0; i<rounds; ++i){
            for(int j=0; j<blocks; ++j){
                bits[j] = 0;
                bits[j] |= bits[j+1];
            }
            bits[blocks] = 0;
        }
        if(mod != 0){
            //Define constants used in for loop
            final int modM1 = (mod - 1);
            final int complement = (8 - mod);
            //Go over every block and shift all bits to the left.
            //Then set the bits which got shifted in according
            //to the bits in the adjacent block to the right.
            //Finally, the last (rightmost) block gets shifted
            for(int j=0; j<blocks; ++j){
                bits[j] <<= mod;
                bits[j] |= ((0xff & ((0xffffff80 >> modM1) & bits[j+1])) >>> complement);
            }
            bits[blocks] <<= mod;
        }
        //Reset the internal counter as we do not know how
        //many ones got shifted out of the vector
        if(ones != -1){
            ones = -1;
        }
        return this;
    }
    
    /**
     * Shifts the entire bit pattern of this BitVector to the right by the
     * specified amount.
     * 
     * <p>This call will cause the internal cache for the number of bits
     * set to 1 to be reset.
     * 
     * <p>Passing a negative number to this method will perform a left
     * shift by the specified absolute amount
     * 
     * @param positions The number of positions to shift the bit pattern
     *                  to the right
     * @return This <code>BitVector</code> instance
     */
    public BitVector shiftRight(final int positions){
        if(positions < 0){
            return shiftLeft(-positions);
        }
        //First, the number of currently used blocks is computed.
        //If the vector is shifted by more than 8 bits, then there
        //is at least one entire block falling out the rightmost end.
        //Consequently, for every additional 8 bits shifted there is
        //one more block falling out the right end. Therefore the
        //number of whole blocks is computed here so that the bytes
        //which are guaranteed to be overridden can be set
        //in a preprocessing step. The remaining bits (or all bits at
        //once if shifted by less than 8 positions) can then be shifted
        //and modified according to their adjacent blocks
        final int blocks = (((next%8) != 0) ? (next >> 3) : ((next >> 3) - 1));
        final int rounds = (positions >> 3);
        final int mod = (positions%8);
        for(int i=0; i<rounds; ++i){
            for(int j=blocks; j>0; --j){
                bits[j] = 0;
                bits[j] |= bits[j-1];
            }
            bits[0] = 0;
        }
        if(mod != 0){
            //Define constants used in for loop.
            //The mask picks the bits from the left adjacent block which
            //need to get transferred one block to the right
            final int mask = ~(0xff << mod);
            final int complement = (8 - mod);
            //Go over every block and shift all bits to the right.
            //Then set the bits which got shifted in according
            //to the bits in the adjacent block to the left.
            //Finally, the last (leftmost) block gets shifted.
            //Simple rightshift operations of single byte blocks must
            //be stated in the verbose way because of sign extension
            //when the byte is promoted to an int by the JVM
            for(int j=blocks; j>0; --j){
                bits[j] = (byte) ((0xff & bits[j]) >>> mod);
                bits[j] |= ((mask & bits[j-1]) << complement);
            }
            bits[0] = (byte) ((0xff & bits[0]) >>> mod);
        }
        //Reset the internal counter as we do not know how
        //many ones got shifted out of the vector
        if(ones != -1){
            ones = -1;
        }
        return this;
    }
    
    /**
     * Rotates the entire bit pattern of this BitVector to the left by the
     * specified amount. Bits shifted out of the left hand side reenter on
     * the right.
     * 
     * <p>Shifting a bit pattern by an integer multiple of its own size
     * will not change the bit pattern as all the bits end up at
     * their original position.
     * 
     * <p>Passing a negative number to this method will perform a right
     * rotation by the specified absolute amount
     * 
     * @param positions The number of positions to rotate the bit pattern
     *                  to the left
     * @return This <code>BitVector</code> instance
     */
    public BitVector rotateLeft(final int positions){
        if(positions < 0){
            return rotateRight(-positions);
        }
        final int mod = (positions % next);
        if(mod == 0){
            return this;
        }
        final BitVector shifted = get(0, mod);
        this.shiftLeft(mod).set((next - mod), shifted);
        return this;
    }
    
    /**
     * Rotates the entire bit pattern of this BitVector to the right by the
     * specified amount. Bits shifted out of the right hand side reenter on
     * the left.
     * 
     * <p>Shifting a bit pattern by an integer multiple of its own size
     * will not change the bit pattern as all the bits end up at
     * their original position.
     * 
     * <p>Passing a negative number to this method will perform a left
     * rotation by the specified absolute amount
     * 
     * @param positions The number of positions to rotate the bit pattern
     *                  to the right
     * @return This <code>BitVector</code> instance
     */
    public BitVector rotateRight(final int positions){
        if(positions < 0){
            return rotateLeft(-positions);
        }
        final int mod = (positions % next);
        if(mod == 0){
            return this;
        }
        final BitVector shifted = get((next - mod), next);
        this.shiftRight(mod).set(0, shifted);
        return this;
    }
    
    /**
     * Performs a logical AND operation to the bit at the specified
     * index with the bit provided to this method. The bit at the
     * specified index of this BitVector is set to the result of
     * that operation
     * 
     * @param index The index of the bit to use as the first operand
     *              for the logical AND operation
     * @param bit The bit to use as the second operand for the
     *            logical AND operation
     * @return This <code>BitVector</code> instance
     */
    public BitVector and(final int index, final boolean bit){
        set(index, (get(index) & bit));
        return this;
    }
    
    /**
     * Performs a logical AND operation to all bits of this BitVector
     * with all bits of the BitVector provided to this method. All bits of
     * this BitVector are set to the result of the corresponding
     * bitwise operation. The content of the BitVector provided as an
     * argument to this method is not changed.
     * 
     * <p>Both BitVectors must have the same size
     * 
     * @param vec The <code>BitVector</code> to use as the second
     *            operand for the logical AND operations
     * @return This <code>BitVector</code> instance
     * @throws IllegalArgumentException If the BitVector passed to this
     *                                  method has a different size
     *                                  than this BitVector
     */
    public BitVector and(final BitVector vec){
        if(vec.next != this.next){
            throw new IllegalArgumentException("BitVectors must be the same length");
        }
        //Compute the number of byte blocks to process.
        //If the next free position is on a new block, we do not
        //take that one into account in the loop
        final int blocks = (((next%8) != 0) ? ((next >> 3) + 1) : (next >> 3));
        for(int i=0; i<blocks; ++i){
            bits[i] &= vec.bits[i];
        }
        //Reset the internal counter as we do not
        //know which bits changed
        if(ones != -1){
            ones = -1;
        }
        return this;
    }
    
    /**
     * Performs a logical OR operation to the bit at the specified
     * index with the bit provided to this method. The bit at the
     * specified index of this BitVector is set to the result of
     * that operation
     * 
     * @param index The index of the bit to use as the first operand
     *              for the logical OR operation
     * @param bit The bit to use as the second operand for the
     *            logical OR operation
     * @return This <code>BitVector</code> instance
     */
    public BitVector or(final int index, final boolean bit){
        set(index, (get(index) | bit));
        return this;
    }
    
    /**
     * Performs a logical OR operation to all bits of this BitVector
     * with all bits of the BitVector provided to this method. All bits of
     * this BitVector are set to the result of the corresponding
     * bitwise operation. The content of the BitVector provided as an
     * argument to this method is not changed.
     * 
     * <p>Both BitVectors must have the same size
     * 
     * @param vec The <code>BitVector</code> to use as the second
     *            operand for the logical OR operations
     * @return This <code>BitVector</code> instance
     * @throws IllegalArgumentException If the BitVector passed to this
     *                                  method has a different size
     *                                  than this BitVector
     */
    public BitVector or(final BitVector vec){
        if(vec.next != this.next){
            throw new IllegalArgumentException("BitVectors must be the same length");
        }
        //Compute the number of byte blocks to process.
        //If the next free position is on a new block, we do not
        //take that one into account in the loop
        final int blocks = (((next%8) != 0) ? ((next >> 3) + 1) : (next >> 3));
        for(int i=0; i<blocks; ++i){
            bits[i] |= vec.bits[i];
        }
        //Reset the internal counter as we do not
        //know which bits changed
        if(ones != -1){
            ones = -1;
        }
        return this;
    }
    
    /**
     * Performs a logical XOR operation to the bit at the specified
     * index with the bit provided to this method. The bit at the
     * specified index of this BitVector is set to the result of
     * that operation
     * 
     * @param index The index of the bit to use as the first operand
     *              for the logical XOR operation
     * @param bit The bit to use as the second operand for the
     *            logical XOR operation
     * @return This <code>BitVector</code> instance
     */
    public BitVector xor(final int index, final boolean bit){
        set(index, (get(index) ^ bit));
        return this;
    }
    
    /**
     * Performs a logical XOR operation to all bits of this BitVector
     * with all bits of the BitVector provided to this method. All bits of
     * this BitVector are set to the result of the corresponding
     * bitwise operation. The content of the BitVector provided as an
     * argument to this method is not changed.
     * 
     * <p>Both BitVectors must have the same size
     * 
     * @param vec The <code>BitVector</code> to use as the second
     *            operand for the logical XOR operations
     * @return This <code>BitVector</code> instance
     * @throws IllegalArgumentException If the BitVector passed to this
     *                                  method has a different size
     *                                  than this BitVector
     */
    public BitVector xor(final BitVector vec){
        if(vec.next != this.next){
            throw new IllegalArgumentException("BitVectors must be the same length");
        }
        //Compute the number of byte blocks to process.
        //If the next free position is on a new block, we do not
        //take that one into account in the loop
        final int blocks = (((next%8) != 0) ? ((next >> 3) + 1) : (next >> 3));
        for(int i=0; i<blocks; ++i){
            bits[i] ^= vec.bits[i];
        }
        //Reset the internal counter as we do not
        //know which bits changed
        if(ones != -1){
            ones = -1;
        }
        return this;
    }
    
    /**
     * Returns the first 8 bits of this BitVector as a primitive byte value.
     * The bits are read in big-endian order, i.e. the bit at index zero is
     * taken as the most significant bit of the returned byte.<br>
     * If this BitVector's size is less than 8, then the missing bits for
     * creating a valid byte are padded with zeros
     * 
     * @return A primitive byte value composed of the
     *         first 8 bits of this BitVector
     */
    public byte asByte(){
        ensureCapacitySet(0);
        return bits[0];
    }

    /**
     * Returns the first 16 bits of this BitVector as a primitive short value.
     * The bits are read in big-endian order, i.e. the bit at index zero is
     * taken as the most significant bit of the returned short.<br>
     * If this BitVector's size is less than 16, then the missing bits for
     * creating a valid short are padded with zeros
     * 
     * @return A primitive short value composed of the
     *         first 16 bits of this BitVector
     */
    public short asShort(){
        ensureCapacitySet(1);
        return (short) (((bits[0] & 0xff) << 8) 
                       | (bits[1] & 0xff));
    }
    
    /**
     * Returns the first 32 bits of this BitVector as a primitive int value.
     * The bits are read in big-endian order, i.e. the bit at index zero is
     * taken as the most significant bit of the returned int.<br>
     * If this BitVector's size is less than 32, then the missing bits for
     * creating a valid int are padded with zeros
     * 
     * @return A primitive int value composed of the
     *         first 32 bits of this BitVector
     */
    public int asInt(){
        ensureCapacitySet(3);
        return (((bits[0] & 0xff) << 24) 
              | ((bits[1] & 0xff) << 16) 
              | ((bits[2] & 0xff) << 8) 
              |  (bits[3] & 0xff));
    }
    
    /**
     * Returns the first 64 bits of this BitVector as a primitive long value.
     * The bits are read in big-endian order, i.e. the bit at index zero is
     * taken as the most significant bit of the returned long.<br>
     * If this BitVector's size is less than 64, then the missing bits for
     * creating a valid long are padded with zeros
     * 
     * @return A primitive long value composed of the
     *         first 64 bits of this BitVector
     */
    public long asLong(){
        ensureCapacitySet(7);
        return (((bits[0] & 0xffL) << 56)
              | ((bits[1] & 0xffL) << 48)
              | ((bits[2] & 0xffL) << 40)
              | ((bits[3] & 0xffL) << 32)
              | ((bits[4] & 0xffL) << 24) 
              | ((bits[5] & 0xffL) << 16) 
              | ((bits[6] & 0xffL) << 8) 
              |  (bits[7] & 0xffL));
    }
    
    /**
     * Returns the first 32 bits of this BitVector as a primitive float value.
     * The bits are read in big-endian order, i.e. the bit at index zero is
     * taken as the sign bit of the returned float.<br>
     * If this BitVector's size is less than 32, then the missing bits for
     * creating a valid float are padded with zeros
     * 
     * @return A primitive float value composed of the
     *         first 32 bits of this BitVector
     */
    public float asfloat(){
        return Float.intBitsToFloat(asInt());
    }
    
    /**
     * Returns the first 64 bits of this BitVector as a primitive double value.
     * The bits are read in big-endian order, i.e. the bit at index zero is
     * taken as the sign bit of the returned double.<br>
     * If this BitVector's size is less than 64, then the missing bits for
     * creating a valid double are padded with zeros
     * 
     * @return A primitive double value composed of the
     *         first 64 bits of this BitVector
     */
    public double asDouble(){
        return Double.longBitsToDouble(asLong());
    }
    
    /**
     * Returns a reference to the internal byte array used to store
     * the state of all bits in this vector. Modifications to this BitVector will
     * be reflected by the returned byte array and vice versa
     * 
     * @return A reference to the internally used bit storage
     * @see #toArray()
     */
    public byte[] asArray(){
        return this.bits;
    }
    
    /**
     * Returns a copy of the internally used byte array which carries the state
     * of all bits in this vector. Please note that the returned array does not
     * include any growth buffer that might has been accumulated by this BitVector
     * at the time this method is called.
     * The returned array carries only the raw data
     * 
     * @return A copy of the internally used bit storage
     * @see #asArray()
     */
    public byte[] toArray(){
        final int length = (((next%8) != 0) ? ((next >> 3) + 1) : (next >> 3));
        final byte[] bytes = new byte[length];
        for(int i=0; i<length; ++i){
            bytes[i] = this.bits[i];
        }
        return bytes;
    }
    
    /**
     * Creates and returns a copy of this BitVector. The returned BitVector
     * is a deep copy, i.e. the array holding the internal state is
     * copied in its entirety.<br>
     * This method has the same behaviour as the copy constructor of
     * the BitVector class.
     * 
     * @return A deep copy of this BitVector
     * @see #BitVector(BitVector)
     */
    @Override
    public BitVector clone(){
        return new BitVector(this);
    }
    
    /**
     * Returns a string representation of this BitVector. The returned
     * String will only be composed of the characters '1' and '0'
     * 
     * @return A string representation of this BitVector
     * @see #toFormattedString()
     * @see #toHexString()
     */
    @Override
    public String toString(){
        if(next == 0){
            return "[]";
        }
        final StringBuilder sb = new StringBuilder();
        for(int i=0; i<next; ++i){
            sb.append(get(i) ? "1" : "0");
        }
        return sb.toString();
    }
    
    /**
     * Returns a formatted string representation of this BitVector.
     * This method returns a more human readable string than <code>toString()</code>
     * by adding a blank space after every 8 bits. Therefore it is easier to visually
     * distinguish each byte block. The returned String will only be composed of
     * the characters '1', '0' and possibly padding whitespaces. 
     * 
     * @return A formatted string representation of this BitVector
     * @see #toString()
     * @see #toHexString()
     */
    public String toFormattedString(){
        if(next == 0){
            return "[]";
        }
        final StringBuilder sb = new StringBuilder();
        for(int i=0; i<next; ++i){
            if((i != 0) && ((i%8) == 0)){
                sb.append(" ");
            }
            sb.append(get(i) ? "1" : "0");
        }
        return sb.toString();
    }
    
    /**
     * Returns a string representation of this BitVector in hexadecimal notation.
     * The returned String will only be composed of the
     * characters 1-9 and a-f (lowercase).<br>
     * This method will take all set nibbles into account, regardless whether
     * all bits are used. For example, the vector "0110" will return
     * the string "6" whereas the vector "01100" will return "60", as the latter
     * uses the leftmost bit of the second nibble as well.
     * <br>A "0x" prefix is never added
     * 
     * @return A hexadecimal string representation of this BitVector
     * @see #toString()
     * @see #toFormattedString()
     */
    public String toHexString(){
        if(next == 0){
            return "[]";
        }
        final StringBuilder sb = new StringBuilder();
        final char[] hex = { '0', '1', '2', '3', '4', '5', '6', '7',
                             '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };
        
        final int length = (((next%8) != 0) ? ((next >> 3) + 1) : (next >> 3));
        final boolean printLastNibble = (((next-1)%8) >= 4);
        final int max = (length - 1);
        for(int i=0; i<length; ++i){
            sb.append(hex[(bits[i] & 0xf0) >>> 4]);
            if((i != max) || printLastNibble){
                sb.append(hex[bits[i] & 0x0f]);
            }
        }
        return sb.toString();
    }
    
    @Override
    public int hashCode(){
        return Arrays.hashCode(this.bits);
    }
    
    @Override
    public boolean equals(Object obj){
        if(!(obj instanceof BitVector)){
            return false;
        }
        final BitVector vec = (BitVector) obj;
        //Optimize since both vectors must have the same length
        if(this.next != vec.next){
            return false;
        }
        final byte[] bits2 = vec.bits;
        //Optimization: check if they have the same reference
        if (bits == bits2){
            return true;
        }
        final int length = (((next%8) != 0) ? ((next >> 3) + 1) : (next >> 3));
        for(int i=0; i<length; ++i){
            if(bits[i] != bits2[i]){
                return false;
            }
        }
        return true;
    }
    
    /**
     * Wraps the given byte array into a BitVector. This method does not create
     * a copy of the specified byte array but rather uses the reference to it directly.
     * Therefore any modifications made to the byte array either externally by other
     * code or internally by the BitVector itself will be immediately visible by
     * every component in possession of a reference to that array.
     * 
     * <p>Wrapping a byte array into a BitVector is an easy and efficient way to make
     * every individual bit addressable by an index directly. The content of the array
     * passed to this method is not copied. However, when the array needs to be resized
     * due to an <i>add</i> or <i>set</i> operation, the internal reference to the given
     * array will be replaced by the resized instance, effectively breaking the wrapping
     * effect. If the wrapped array is supposed to be used with a variable length, then
     * resizing and subsequent rewrapping must be handled by external code.
     * 
     * @param bits The byte array to be wrapped into a newly allocated BitVector
     * @return A <code>BitVector</code> holding the specified
     *         byte array as its content
     * @see BitVector#valueOf(byte[])
     */
    public static BitVector wrap(final byte[] bits){
        return new BitVector(bits);
    }
    
    /**
     * Creates a BitVector from specified String object. The String argument should
     * only consist of the characters '0' and '1'. Every character which is not either
     * a '0' or a '1' will be interpreted as a '1'.
     * 
     * @param bits The String containing the bits for the BitVector to be created
     * @return A <code>BitVector</code> containing the bits from
     *         the specified String object
     * @see BitVector#fromHexString(String)
     */
    public static BitVector valueOf(final String bits){
        if(bits == null){
            throw new IllegalArgumentException("String argument must not be null");
        }
        final int length = bits.length();
        if(length == 0){
            return new BitVector(0);
        }
        final BitVector vec = new BitVector(length);
        for(int i=0; i<length; ++i){
            vec.add(((bits.charAt(i) == '0') ? Bit._0 : Bit._1));
        }
        return vec;
    }
    
    /**
     * Creates a BitVector holding the bytes from the specified array. The content of the
     * byte array passed to this method will be copied into a newly allocated array
     * 
     * @param bytes The array of bytes to be copied and put into a newly allocated BitVector
     * @return A <code>BitVector</code> holding the byte values of the specified array
     * @see BitVector#wrap(byte[])
     */
    public static BitVector valueOf(final byte[] bytes){
        if(bytes == null){
            throw new IllegalArgumentException("Byte array argument must not be null");
        }
        final int length = bytes.length;
        final byte[] copy = new byte[length];
        for(int i=0; i<length; ++i){
            copy[i] = bytes[i];
        }
        return new BitVector(copy);
    }
    
    /**
     * Creates a BitVector holding the bits from the specified array. The content of the
     * boolean array passed to this method determines the bit pattern
     * of the newly created BitVector
     * 
     * @param bits The boolean array holding the bit values of the BitVector to be created
     * @return A <code>BitVector</code> holding the bit pattern of the specifie boolean array
     */
    public static BitVector valueOf(final boolean[] bits){
        if(bits == null){
            throw new IllegalArgumentException("Boolean array argument must not be null");
        }
        final int length = bits.length;
        final BitVector vec = new BitVector(length);
        for(int i=length-1; i>=0; --i){
            vec.set(i, bits[i]);
        }
        return vec;
    }
    
    /**
     * Creates a BitVector holding the 8 bits from the specified byte value.
     * The returned BitVector has a length of 8. The most significant bit of
     * the byte value is located at index 0 (zero)
     * 
     * @param b The byte value to create a BitVector from
     * @return A <code>BitVector</code> holding the bit pattern
     *         of the specified byte value
     */
    public static BitVector valueOf(final byte b){
        return new BitVector(new byte[]{b});
    }
    
    /**
     * Creates a BitVector holding the 16 bits from the specified short value.
     * The returned BitVector has a length of 16. The most significant bit of
     * the short value is located at index 0 (zero)
     * 
     * @param s The short value to create a BitVector from
     * @return A <code>BitVector</code> holding the bit pattern
     *         of the specified short value
     */
    public static BitVector valueOf(final short s){
        return new BitVector(new byte[]{
                (byte) ((s & 0xff00) >> 8),
                (byte) ((s & 0xff))});
    }
    
    /**
     * Creates a BitVector holding the 32 bits from the specified int value.
     * The returned BitVector has a length of 32. The most significant bit of
     * the int value is located at index 0 (zero)
     * 
     * @param i The int value to create a BitVector from
     * @return A <code>BitVector</code> holding the bit pattern
     *         of the specified int value
     */
    public static BitVector valueOf(final int i){
        return new BitVector(new byte[]{
                (byte) ((i & 0xff000000) >> 24),
                (byte) ((i & 0xff0000) >> 16),
                (byte) ((i & 0xff00) >> 8),
                (byte) ((i & 0xff))});
    }
    
    /**
     * Creates a BitVector holding the 64 bits from the specified long value.
     * The returned BitVector has a length of 64. The most significant bit of
     * the long value is located at index 0 (zero)
     * 
     * @param l The long value to create a BitVector from
     * @return A <code>BitVector</code> holding the bit pattern
     *         of the specified long value
     */
    public static BitVector valueOf(final long l){
        return new BitVector(new byte[]{
                (byte) ((l & 0xff00000000000000L) >> 56),
                (byte) ((l & 0xff000000000000L) >> 48),
                (byte) ((l & 0xff0000000000L) >> 40),
                (byte) ((l & 0xff00000000L) >> 32),
                (byte) ((l & 0xff000000L) >> 24),
                (byte) ((l & 0xff0000L) >> 16),
                (byte) ((l & 0xff00L) >> 8),
                (byte) ((l & 0xffL))});
    }
    
    /**
     * Creates a BitVector holding the 32 bits from the specified float value.
     * The returned BitVector has a length of 32
     * 
     * @param f The float value to create a BitVector from
     * @return A <code>BitVector</code> holding the bit pattern
     *         of the specified float value
     */
    public static BitVector valueOf(final float f){
        return valueOf(Float.floatToIntBits(f));
    }
    
    /**
     * Creates a BitVector holding the 64 bits from the specified double value.
     * The returned BitVector has a length of 64
     * 
     * @param d The double value to create a BitVector from
     * @return A <code>BitVector</code> holding the bit pattern
     *         of the specified double value
     */
    public static BitVector valueOf(final double d){
        return valueOf(Double.doubleToLongBits(d));
    }
    
    /**
     * Constructs a <code>BitVector</code> with the specified size whose bits
     * are all initialized with the specified value
     * 
     * @param size The initial size of the <code>BitVector</code> to be created
     * @param bit The initial value of all bits in the <code>BitVector</code>
     *            to be created
     * @return A <code>BitVector</code> which has the specified size and whose
     *         bits are all set to the specified value
     */
    public static BitVector createInitialized(final int size, final boolean bit){
        final BitVector vec = new BitVector(size);
        vec.next = size;
        if(bit){
            for(int i=0; i<size; ++i){
                vec.bits[i >> 3] |= (0x80 >>> (i%8));
            }
            //Optimization as the state of all bits is known
            vec.ones = size;
        }else{
            //Optimize here as well
            vec.ones = 0;
        }
        return vec;
    }
    
    /**
     * Returns a BitVector from the specified String object in hexadecimal notation.
     * The given String should only be composed of the characters 1-9
     * and a-f (case insensitive). Every character which is not in the set of
     * hexadecimal characters will be interpreted as the hex digit 'f'.<br>
     * If the string passed to this method is empty, a BitVector
     * of length 0 (zero) is returned
     * 
     * @param hexValues The String containing the hexadecimal digits which represent
     *                  the content of the BitVector to be created
     * @return A <code>BitVector</code> from the specified String
     *         in hexadecimal notation
     */
    public static BitVector fromHexString(final String hexValues){
        if(hexValues == null){
            throw new IllegalArgumentException("String argument must not be null");
        }
        if(hexValues.isEmpty()){
            return new BitVector(0);
        }

        if(hexValues.length() == 1){
            final BitVector vec = new BitVector(new byte[]{
                    (byte) (Character.digit(hexValues.charAt(0), 16) << 4)});
            
            vec.next = 4;
            return vec;
        }
        final int mod = (hexValues.length() % 2);
        final int length = (hexValues.length() + mod);
        final byte[] bits = new byte[length / 2];
        final int max = ((mod == 0) ? length : (length - 1));
        for(int i=0; i<max; ++i){
            bits[i / 2] |= (byte) ((i%2 == 0)
                    ? (Character.digit(hexValues.charAt(i), 16) << 4)
                    : (Character.digit(hexValues.charAt(i), 16)));
            
        }
        final BitVector vec = new BitVector(bits);
        vec.next = (hexValues.length() * 4);
        return vec;
    }
    
    /**
     * Creates a BitVector from specified String object. The String argument should
     * only consist of the characters '0' and '1'. Every character which is not either
     * a '0' or a '1' will be interpreted as a '1'.<br>
     * This method is equivalent to {@link BitVector#valueOf(String)}
     * 
     * @param binaryValues The String containing the bits
     *                     for the BitVector to be created
     * @return A <code>BitVector</code> containing the bits from
     *         the specified String object
     * @see BitVector#fromHexString(String)
     */
    public static BitVector fromBinaryString(final String binaryValues){
        return valueOf(binaryValues);
    }
    
    /**
     * Counts the amount of 1s in the BitVector within the specified range
     * 
     * @param from The start index of the range (inclusive)
     * @param to The end index of the range (exclusive)
     * @return The number of 1s within the specified range of the BitVector
     */
    private int countOnes(final int from, final int to){
        int total = 0;
        for(int i=from; i<to; ++i){
            if(((0xff & bits[i >> 3]) & (0x80 >>> (i%8))) != 0){
                ++total;
            }
        }
        return total;
    }
    
    /**
     * Ensures the internal array has at least the length to serve
     * the specified block index, resizing the entire array if necessary.
     * Resizing will double the length of the internal array
     * 
     * @param blockIndex The index of the byte block that the
     *                   internal array should hold as a minimum
     */
    private void ensureCapacityAdd(final int blockIndex){
        if((blockIndex + 1) > bits.length){
            copyBytes((bits.length == 0) ? 1 : (bits.length * 2));
        }
    }
    
    /**
     * Ensures the internal array has at least the length to serve
     * the specified block index, resizing the entire array if necessary.
     * Resizing will extend the length of the internal array to
     * match the specified block index
     * 
     * @param blockIndex The index of the byte block that the
     *                   internal array should hold as a minimum
     */
    private void ensureCapacitySet(final int blockIndex){
        if((blockIndex + 1) > bits.length){
            copyBytes(blockIndex + 1);
        }
    }
    
    /**
     * Copies all bytes of the internal array to a newly allocated
     * array of the specified block length
     * 
     * @param newBlockLength The new length of the internal array
     */
    private void copyBytes(final int newBlockLength){
        final byte[] tmp = new byte[newBlockLength];
        for(int i=0; i<bits.length; ++i){
            tmp[i] = bits[i];
        }
        this.bits = tmp;
    }

    /**
     * Ensures the internal counter variable is set
     * to the currently valid amount
     */
    private void ensureCounted(){
        if(ones == -1){
            this.ones = countOnes(0, next);
        }
    }
    
    /**
     * Checks that the specified bit index is not out of bounds
     * 
     * @param index The bit index to check 
     */
    private void check(final int index){
        if((index < 0) || (index >= next)){
            throw new IndexOutOfBoundsException(String.valueOf(index));
        }
    }
    
    /**
     * Checks that the specified indices form a valid range and that the end
     * index is not out of the current length of the internal array
     * 
     * @param start The start index to check
     * @param end The end index to check
     */
    private void checkRangeIn(final int start, final int end){
        if(start < 0){
            throw new IndexOutOfBoundsException("Invalid start index: "
                                               + String.valueOf(start));
        }
        if(end > next){
            throw new IndexOutOfBoundsException("Invalid end index: "
                                               + String.valueOf(end));
        }
        if(start >= end){
            throw new IllegalArgumentException("Invalid range: start="
                                              + start + " end=" + end);
        }
    }
    
    /**
     * Checks that the specified indices form a valid range. This method
     * allows the end index to be greater than the current length of the
     * internal array and will perform a resizing operation if necessary
     * 
     * @param start The start index to check
     * @param end The end index to check
     */
    private void checkRangeOut(final int start, final int end){
        if(start < 0){
            throw new IndexOutOfBoundsException("Invalid start index: "
                                               + String.valueOf(start));
        }
        ensureCapacitySet(end >> 3);
        if(start >= end){
            throw new IllegalArgumentException("Invalid range: start="
                                              + start + " end=" + end);
        }
    }
}
