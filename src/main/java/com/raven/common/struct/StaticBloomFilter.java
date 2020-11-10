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

import java.text.DecimalFormat;

import com.raven.common.io.Serializer;

/**
 * An implementation of a static Bloom filter. A StaticBloomFilter instance
 * can be created by using one of the provided constructors. As a minimum
 * set of arguments, a {@link Serializer} must be specified which will be
 * used to serialize any elements passed to the core data structure methods
 * of this class. Serialization is required for computing hash values of
 * elements. If no <code>Serializer</code> object is specified during
 * the contruction of a StaticBloomFilter, then elements must implement
 * the <code>java.io.Serializable</code> interface. If this condition is
 * not met, then as a last fallback, serialization is attempted for
 * the String returned by the <code>toString()</code> method of
 * the respective element. To achieve the best performance and stability it
 * is recommended to always specify a <code>Serializer</code> when
 * constructing a StaticBloomFilter instance.
 * 
 * <p>As demanded by the {@link ProbabilisticSet} interface,
 * a StaticBloomFilter implements two basic set operations. Elements can be
 * added to a filter by means of the {@link #add(Object)} method. The presence
 * of an element can be checked with the {@link #contains(Object)} method.
 * Adding an element to a StaticBloomFilter causes the <code>contains()</code>
 * method to affirm the presence of that element for all subsequent calls.
 * However, performing membership queries for arbitrary elements is subject to a
 * rate of error. More specific, a StaticBloomFilter might erroneously affirm
 * the presence of an element which is not in fact contained in it. The
 * likelihood of such a false positive is always non-zero. It can be controlled
 * by specifying a maximum allowed rate when constructing a
 * new StaticBloomFilter.<br>
 * On the other hand, a StaticBloomFilter will never produce false negatives
 * for all elements added to it, that is, it will never deny the presence of
 * an element in its set.
 * 
 * <p>A StaticBloomFilter cannot grow dynamically as more elements are
 * added to it. Its capacity is bound by the initially given value. Adding more
 * elements to a StaticBloomFilter than the specified capacity will result
 * in a rapidly increasing false positive rate.
 * 
 * <p>A StaticBloomFilter cannot be used with null elements. Passing null
 * to either the <code>add()</code> or <code>contains()</code> method will
 * result in a <code>NullPointerException</code> being thrown.
 * 
 * <p>This implementation is NOT thread-safe.
 * 
 * @author Phil Gaiser
 * @see ScalableBloomFilter
 * @since 3.0.0
 *
 * @param <E> The type of elements to be used by the Bloom filter
 */
public class StaticBloomFilter<E> extends AbstractBloomFilter<E> {

    private final BitVector filter;
    private final int capacity;
    private final int slices;
    private final int sliceSize;

    /**
     * Constructs a new <code>StaticBloomFilter</code> with a capacity
     * of 10000 elements and a maximum allowed false positive
     * probability of 1%.<br>
     * Adding more elements to the constructed filter than the specified
     * capacity will result in the actual false positive rate to
     * exceed the maximum allowed value
     * 
     * @param serializer The <code>Serializer</code> instance to use to
     *                   serialize the elements used by the constructed
     *                   StaticBloomFilter
     */
    public StaticBloomFilter(final Serializer<E> serializer){
        this(serializer, 10000, 0.01);
    }

    /**
     * Constructs a new <code>StaticBloomFilter</code> with the specified
     * capacity and a maximum allowed false positive probability of 1%.<br>
     * Adding more elements to the constructed filter than the specified
     * capacity will result in the actual false positive rate to
     * exceed the maximum allowed value
     * 
     * @param serializer The <code>Serializer</code> instance to use to
     *                   serialize the elements used by the constructed
     *                   StaticBloomFilter
     * @param capacity The number of elements the constructed
     *                 StaticBloomFilter should be able to hold
     */
    public StaticBloomFilter(final Serializer<E> serializer,
            final int capacity){
        
        this(serializer, capacity, 0.01);
    }

    /**
     * Constructs a new <code>StaticBloomFilter</code> with the specified
     * capacity and maximum allowed false positive probability.<br>
     * Adding more elements to the constructed filter than the specified
     * capacity will result in the actual false positive rate to
     * exceed the maximum allowed value
     * 
     * @param serializer The <code>Serializer</code> instance to use to
     *                   serialize the elements used by the constructed
     *                   StaticBloomFilter
     * @param capacity The number of elements the constructed
     *                 StaticBloomFilter should be able to hold
     * @param maxError The maximum allowed false positive probability the
     *                  constructed StaticBloomFilter should adhere to
     */
    public StaticBloomFilter(final Serializer<E> serializer,
            final int capacity, final double maxError){

        super(serializer);
        this.capacity = capacity;
        this.slices = log2(1.0 / maxError);
        this.sliceSize = (int) Math.ceil((capacity * Math.abs(
                Math.log(maxError))) / (slices * 0.480453014));//const: ln(2)^2

        final int vectorSize = (slices * sliceSize);
        this.filter = BitVector.createInitialized(vectorSize, false);
    }

    /**
     * Adds the specified element to this StaticBloomFilter. Adding an element
     * which is already in the filter will have no effect
     * 
     * @param element The element to be added to this StaticBloomFilter
     * @throws NullPointerException If the specified element is null
     * @throws IllegalArgumentException If the specified element
     *                                  is invalid, e.g. an empty String
     * @throws SerializationException If the specified element cannot be serialized
     */
    @Override
    public void add(E element){
        put(hash(element));
    }

    /**
     * Indicates whether this StaticBloomFilter possibly contains
     * the specified element. If this method returns true, then the specified
     * element might be in this filter in due consideration of the maximum allowed
     * error probability this StaticBloomFilter was configured with. If this
     * method returns false, then the specified element is certainly
     * not in this filter.<br>
     * In other words, a true return value might be wrong and the specified
     * element is not actually in this StaticBloomFilter. If the return
     * value is false then the specified element is guaranteed to not be
     * in this filter
     * 
     * @param element The element whose presence in this StaticBloomFilter
     *                is to be tested
     * @return True if the specified element might have been added to
     *         this StaticBloomFilter, false if it is definitely not
     *         in this StaticBloomFilter
     * @throws NullPointerException If the specified element is null
     * @throws IllegalArgumentException If the specified element
     *                                  is invalid, e.g. an empty String
     * @throws SerializationException If the specified element cannot be serialized
     */
    @Override
    public boolean contains(E element){
        return contains(hash(element));
    }

    /**
     * Indicates whether this StaticBloomFilter is empty. An empty filter
     * contains no elements
     * 
     * @return True if this filter contains no elements, false if it is not
     *         empty and contains at least one element
     */
    @Override
    public boolean isEmpty(){
        return (filter.bitsSet() == 0);
    }

    /**
     * Returns an estimation of the size of this StaticBloomFilter.
     * The number of elements is approximated by putting the configured size of
     * the slices in relation to the fill ratio of the filter.<br>
     * The actual size, that is, the number of distinct elements which have been
     * added to the filter might be somewhat lower or higher than
     * the value returned by this method
     * 
     * @return The estimated number of elements in this filter
     */
    @Override
    public int approximateSize(){
        return (int) (-sliceSize * Math.log(1 - fillRatio()));
    }

    /**
     * Returns the size in bytes this StaticBloomFilter has allocated in memory
     * in order to store all elements of its current capacity
     * 
     * @return The number of bytes that this StaticBloomFilter has allocated
     *         in memory to store its current capacity
     */
    @Override
    public long sizeInBytes(){
        //Capacity of the BitVector
        // + 8 bytes for internally used vars in the BitVector
        // + 12 bytes for internally used vars in this instance
        return this.filter.asArray().length + 20;
    }

    /**
     * Removes all of the elements from this StaticBloomFilter. The filter
     * will be empty after this method returns. The capacity of this filter
     * is not changed by this operation
     */
    @Override
    public void clear(){
        this.filter.clear();
    }

    /**
     * Returns a string representation of this StaticBloomFilter.<br>
     * This method can be used to gather informative
     * information about this filter
     *
     * @return A string representation of this StaticBloomFilter
     */
    @Override
    public String toString(){
        final StringBuilder sb = new StringBuilder();
        final String nl = System.lineSeparator();
        final DecimalFormat d = new DecimalFormat("0.000");
        final int  totalSize = filter.size();
        final int bitsSet = filter.bitsSet();
        final long sizeBytes = sizeInBytes();
        sb.append("Filter: ");
        sb.append(filter.bitsSet());
        sb.append("/");
        sb.append(totalSize);
        sb.append(" (");
        sb.append(d.format(((float)bitsSet / (float)totalSize) * 100));
        sb.append("% full)");
        sb.append(nl);
        sb.append("Total size: ");
        sb.append(sizeBytes);
        sb.append(" bytes (");
        sb.append(sizeBytes / 1024);
        sb.append(" KB)");
        return sb.toString();
    }

    /**
     * Gets the capacity of this static Bloom filter. The capacity
     * for a static bloom filter cannot change and denotes the maximum
     * number of elements that should be added to the filter
     * 
     * @return The capacity of this StaticBloomFilter
     */
    public int getCapacity(){
        return this.capacity;
    }

    /**
     * Adds the specified hash to the filter
     * 
     * @param hash The hash value to be added to the filter
     */
    private void put(final long hash){
        int h1 = (int) hash;
        final int h2 = (int) (hash >>> 32);
        int offset = 0;
        for(int i=0; i<slices; ++i){
            filter.set(((h1 & 0x7fffffff) % sliceSize) + offset, true);
            h1 += h2;
            offset += sliceSize;
        }
    }

    /**
     * Indicates whether the specified hash might have
     * been added to this Stage
     * 
     * @param hash The hash value to check
     * @return True if the specified hash value might have been added to
     *         this Stage, false if it is definitely not in this Stage
     */
    private boolean contains(final long hash){
        int h1 = (int) hash;
        final int h2 = (int) (hash >>> 32);
        int offset = 0;
        for(int i=0; i<slices; ++i){
            if(!filter.get(((h1 & 0x7fffffff) % sliceSize) + offset)){
                return false;
            }
            h1 += h2;
            offset += sliceSize;
        }
        return true;
    }

    /**
     * Returns the fill ratio of this filter
     * 
     * @return The fill ratio of this filter
     */
    private double fillRatio(){
        return ((double)filter.bitsSet() / (double)filter.size());
    }
}
