/* 
 * Copyright (C) 2021 Raven Computing
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
 * An implementation of a scalable Bloom filter. An instance of this class
 * is constructed with an initial capacity and scales itself when needed as
 * more elements are added to it. As opposed to a static variant,
 * a ScalableBloomFilter will grow if necessary in order to adhere to
 * the maximum allowed error rate as specified during its construction.
 * 
 * <p>A ScalableBloomFilter instance can be created by using one of
 * the provided constructors. As a minimum set of arguments,
 * a {@link Serializer} must be specified which will be used to serialize
 * any elements passed to the core data structure methods of this class.
 * Serialization is required for computing hash values of elements. If no
 * <code>Serializer</code> object is specified during the contruction of
 * a ScalableBloomFilter, then elements must implement
 * the <code>java.io.Serializable</code> interface. If this condition is
 * not met, then as a last fallback, serialization is attempted for
 * the String returned by the <code>toString()</code> method of
 * the respective element. To achieve the best performance and stability it
 * is recommended to always specify a <code>Serializer</code> when
 * constructing a ScalableBloomFilter instance.
 * 
 * <p>As demanded by the {@link ProbabilisticSet} interface,
 * a ScalableBloomFilter implements two basic set operations. Elements can be
 * added to a filter by means of the {@link #add(Object)} method. The presence
 * of an element can be checked with the {@link #contains(Object)} method.
 * Adding an element to a ScalableBloomFilter causes the <code>contains()</code>
 * method to affirm the presence of that element for all subsequent calls.
 * However, performing membership queries for arbitrary elements is subject to a
 * rate of error. More specific, a ScalableBloomFilter might erroneously affirm
 * the presence of an element which is not in fact contained in it. The
 * likelihood of such a false positive is always non-zero. It can be controlled
 * by specifying a maximum allowed rate when constructing a
 * new ScalableBloomFilter.<br>
 * On the other hand, a ScalableBloomFilter will never produce false negatives
 * for all elements added to it, that is, it will never deny the presence of
 * an element in its set.
 * 
 * <p>A ScalableBloomFilter cannot be used with null elements. Passing null
 * to either the <code>add()</code> or <code>contains()</code> method will
 * result in a <code>NullPointerException</code> being thrown.
 * 
 * <p>The behaviour of a ScalableBloomFilter can be further adjusted in an
 * advanced way by specifying a scale factor and tightening ratio to use. Both
 * can be configured by using the corresponding constructor.
 * 
 * <p>This implementation is NOT thread-safe.
 * 
 * @author Phil Gaiser
 * @see StaticBloomFilter
 * @since 3.0.0
 * 
 * @param <E> The type of elements to be used by the scalable Bloom filter
 *
 */
public class ScalableBloomFilter<E> extends AbstractBloomFilter<E> {

    /**
     * The default scale factor of a Bloom filter. Provides suitable
     * scaling for most common scenarios
     */
    public static final int DEFAULT_SCALE_FACTOR = 2;

    /**
     * A fast scale factor of a Bloom filter. Causes more aggressive
     * scaling and is therefore suitable for situations where the amount
     * of elements successively added to a filter is several orders of
     * magnitude higher than the initial capacity. This scale factor causes
     * the Bloom filter to consume more memory more quickly but also reduces
     * the overhead introduced by the scaling operation
     */
    public static final int FAST_SCALE_FACTOR = 4;

    /**
     * The default tightening ratio of a Bloom filter
     */
    public static final double DEFAULT_TIGHTENING_RATIO = 0.9;

    private Stage[] stages;
    private final int scaleFactor;
    private final double tighteningRatio;
    private final double maxError0;

    /**
     * Constructs a new <code>ScalableBloomFilter</code> with an initial
     * capacity of 10000 elements and a maximum allowed false positive
     * probability of 1%.<br>
     * The Bloom filter will have a default scale factor and tightening ratio
     * 
     * @param serializer The <code>Serializer</code> instance to use to
     *                   serialize the elements used by the constructed
     *                   ScalableBloomFilter
     */
    public ScalableBloomFilter(final Serializer<E> serializer){
        
        this(serializer, 10000, 0.01,
                DEFAULT_SCALE_FACTOR, DEFAULT_TIGHTENING_RATIO);
    }

    /**
     * Constructs a new <code>ScalableBloomFilter</code> with the specified
     * initial capacity and a maximum allowed false positive
     * probability of 1%.<br>
     * The Bloom filter will have a default scale factor and tightening ratio
     * 
     * @param serializer The <code>Serializer</code> instance to use to
     *                   serialize the elements used by the constructed
     *                   ScalableBloomFilter
     * @param initialCapacity The number of elements the constructed
     *                        ScalableBloomFilter should be able to hold
     */
    public ScalableBloomFilter(final Serializer<E> serializer,
            final int initialCapacity){
        
        this(serializer, initialCapacity, 0.01,
                DEFAULT_SCALE_FACTOR, DEFAULT_TIGHTENING_RATIO);
    }

    /**
     * Constructs a new <code>ScalableBloomFilter</code> with the specified
     * initial capacity and maximum allowed false positive probability.<br>
     * The Bloom filter will have a default scale factor and tightening ratio
     * 
     * @param serializer The <code>Serializer</code> instance to use to
     *                   serialize the elements used by the constructed
     *                   ScalableBloomFilter
     * @param initialCapacity The number of elements the constructed
     *                        ScalableBloomFilter should be able to hold
     * @param maxError The maximum allowed false positive probability the
     *                  constructed ScalableBloomFilter should adhere to
     */
    public ScalableBloomFilter(final Serializer<E> serializer,
            final int initialCapacity, final double maxError){
        
        this(serializer, initialCapacity, maxError,
                DEFAULT_SCALE_FACTOR, DEFAULT_TIGHTENING_RATIO);
    }

    /**
     * Constructs a new <code>ScalableBloomFilter</code> with the specified initial
     * capacity, maximum allowed false positive probability and scale factor.<br>
     * The Bloom filter will have a default tightening ratio
     * 
     * @param serializer The <code>Serializer</code> instance to use to serialize
     *                   the elements used by the constructed ScalableBloomFilter
     * @param initialCapacity The number of elements the
     *                        constructed ScalableBloomFilter should be able to hold
     * @param maxError The maximum allowed false positive probability the
     *                  constructed ScalableBloomFilter should adhere to
     * @param scaleFactor The scale factor of the constructed ScalableBloomFilter.
     *                    Usually either {@link #DEFAULT_SCALE_FACTOR}
     *                    or {@link #FAST_SCALE_FACTOR}
     */
    public ScalableBloomFilter(final Serializer<E> serializer,
            final int initialCapacity, final double maxError, final int scaleFactor){
        
        this(serializer, initialCapacity, maxError,
                scaleFactor, DEFAULT_TIGHTENING_RATIO);
    }

    /**
     * Constructs a new <code>ScalableBloomFilter</code> with the specified initial
     * capacity, maximum allowed false positive probability, scale factor
     * and tightening ratio
     * 
     * @param serializer The <code>Serializer</code> instance to use to serialize
     *                   the elements used by the constructed ScalableBloomFilter
     * @param initialCapacity The number of elements the
     *                        constructed ScalableBloomFilter should be able to hold
     * @param maxError The maximum allowed false positive probability the
     *                  constructed ScalableBloomFilter should adhere to
     * @param scaleFactor The scale factor of the constructed ScalableBloomFilter.
     *                    Usually either 2 or 4
     * @param tighteningRatio The tightening ration of the constructed
     *                        ScalableBloomFilter. Usually between 0.8 and 0.9
     */
    public ScalableBloomFilter(final Serializer<E> serializer,
            final int initialCapacity, final double maxError,
            final int scaleFactor, final double tighteningRatio){
        
        super(serializer);
        if((maxError <= 0) || (maxError >= 1.0)){
            throw new IllegalArgumentException("Maximum allowed error rate must be "
                    + "positive and not greater than 1.0");
        }
        if(scaleFactor <= 1){
            throw new IllegalArgumentException("Scale factor must be greater than 1");
        }
        if(tighteningRatio <= 0){
            throw new IllegalArgumentException("Tightening ratio must be positive");
        }
        this.maxError0 = maxError;
        this.scaleFactor = scaleFactor;
        this.tighteningRatio = tighteningRatio;
        this.stages = new Stage[1];
        this.stages[0] = new Stage(initialCapacity, maxError);
    }

    /**
     * Adds the specified element to this ScalableBloomFilter. Adding an element
     * which is already in the filter will have no effect.<br>
     * The filter will resize itself if necessary
     * 
     * @param element The element to be added to this ScalableBloomFilter
     * @throws NullPointerException If the specified element is null
     * @throws IllegalArgumentException If the specified element
     *                                  is invalid, e.g. an empty String
     * @throws SerializationException If the specified element cannot be serialized
     */
    @Override
    public void add(E element){
        //Compute a hash for the given element and get
        //the currently used stage for write operations
        final long hash = hash(element);
        final Stage stage = stages[stages.length - 1];
        //If the actively used stage has reached its maximum
        //fill ratio, then create a new stage and add the
        //element to it. Otherwise simply use the current stage
        if(stage.isFull()){
            resize().put(hash);
        }else{
            stage.put(hash);
        }
    }

    /**
     * Indicates whether this ScalableBloomFilter possibly contains
     * the specified element. If this method returns true, then the specified
     * element might be in this filter in due consideration of the maximum allowed
     * error probability this ScalableBloomFilter was configured with. If this
     * method returns false, then the specified element is certainly
     * not in this filter.<br>
     * In other words, a true return value might be wrong and the specified
     * element is not actually in this ScalableBloomFilter. If the return
     * value is false then the specified element is guaranteed to not be
     * in this filter
     * 
     * @param element The element whose presence in this ScalableBloomFilter
     *                is to be tested
     * @return True if the specified element might have been added to
     *         this ScalableBloomFilter, false if it is definitely not
     *         in this ScalableBloomFilter
     * @throws NullPointerException If the specified element is null
     * @throws IllegalArgumentException If the specified element
     *                                  is invalid, e.g. an empty String
     * @throws SerializationException If the specified element cannot be serialized
     */
    @Override
    public boolean contains(E element){
      //Compute a hash for the given element and check
      //all available stages in reversed order
      final long hash = hash(element);
      for(int i=stages.length-1; i>=0; --i){
          //If one stage affirms the presence of the element,
          //then we can skip the rest of the
          //stages and return right away
          if(stages[i].contains(hash)){
              return true;
          }
      }
      //No stage had the specified element in it,
      //so it is definitely not in this filter
      return false;
    }

    /**
     * Indicates whether this ScalableBloomFilter is empty. An empty filter
     * contains no elements
     * 
     * @return True if this filter contains no elements, false if it is not
     *         empty and contains at least one element
     */
    @Override
    public boolean isEmpty(){
        return ((stages.length == 1) && stages[0].isEmpty());
    }

    /**
     * Returns an estimation of the size of this ScalableBloomFilter.
     * The number of elements is approximated by putting the configured size of
     * the slices of each stage in relation to the respective fill ratio.<br>
     * The actual size, that is, the number of distinct elements which have been
     * added to the filter might be somewhat lower or higher than
     * the value returned by this method
     * 
     * @return The estimated number of elements in this filter
     */
    @Override
    public int approximateSize(){
        double size = 0.0;
        for(int i=0; i<stages.length; ++i){
            size += stages[i].approximateSize();
        }
        return (int)size;
    }

    /**
     * Returns the size in bytes this ScalableBloomFilter has allocated in memory
     * in order to store all elements of its current capacity. The amount of required
     * memory changes if the filter resizes itself to store all added elements
     * 
     * @return The number of bytes that this ScalableBloomFilter has allocated in
     *         memory to store its current capacity
     */
    @Override
    public long sizeInBytes(){
        long size = 0;
        for(int i=0; i<stages.length; ++i){
            size += stages[i].sizeInBytes();
        }
        return size;
    }

    /**
     * Removes all of the elements from this ScalableBloomFilter. The filter
     * will be empty after this method returns. The current capacity of this filter
     * is not changed by this operation
     */
    @Override
    public void clear(){
        final Stage currentStage = stages[stages.length - 1];
        currentStage.filter.clear();
        this.stages = new Stage[]{currentStage};
    }

    /**
     * Returns a string representation of this ScalableBloomFilter.<br>
     * This method can be used to gather informative
     * information about this filter
     *
     * @return A string representation of this ScalableBloomFilter
     */
    @Override
    public String toString(){
        final StringBuilder sb = new StringBuilder();
        final String nl = System.lineSeparator();
        final DecimalFormat d = new DecimalFormat("0.000");
        sb.append("scaleFactor=");
        sb.append(scaleFactor);
        sb.append(nl);
        sb.append("tighteningRatio=");
        sb.append(tighteningRatio);
        sb.append(nl);
        sb.append("maxError=");
        sb.append(maxError0);
        sb.append(nl);
        sb.append("Stages fill ratio (in bits):");
        sb.append(nl);
        for(int i=0; i<stages.length; ++i){
            final Stage s = stages[i];
            sb.append("S");
            sb.append(i);
            sb.append(": ");
            sb.append(s.filter.bitsSet());
            sb.append("/");
            sb.append(s.filter.size());
            sb.append(" (");
            sb.append(d.format(
                    ((float)s.filter.bitsSet() /
                     (float)s.filter.size())
                            * 100));
            
            sb.append("% full)");
            sb.append(nl);
        }
        final long sizeBytes = sizeInBytes();
        sb.append("Total size: ");
        sb.append(sizeBytes);
        sb.append(" bytes (");
        sb.append(sizeBytes / 1024);
        sb.append(" KB)");
        return sb.toString();
    }

    /**
     * Resizes the internal array of stages, creates a new Stage
     * object and appends it to the resized array for further usage
     * 
     * @return The newly allocated Stage
     */
    private Stage resize(){
        final Stage stage = stages[stages.length - 1];
        final Stage newStage = new Stage(this,
                (stage.capacity * scaleFactor),
                (stage.errorRate * tighteningRatio));

        append(newStage);
        return newStage;
    }

    /**
     * Resizes the internal array to hold the specified Stage and appends
     * it to the array
     * 
     * @param stage The new stage to append to the internal array of stages
     */
    private void append(final Stage stage){
        final int length = stages.length;
        final Stage[] tmp = new Stage[length + 1];
        for(int i=0; i<length; ++i){
            tmp[i] = stages[i];
        }
        this.stages = tmp;
        stages[length] = stage;
    }

    /**
     * Models a size immutable stage of a scalable Bloom filter allowing
     * it to grow dynamically as a whole.
     *
     */
    private static class Stage {

        private final BitVector filter;
        private final double errorRate;
        private final int slices;
        private final int sliceSize;
        private final int capacity;

        private Stage(final int capacity, final double errorRate){
            this.capacity = capacity;
            this.errorRate = errorRate;
            this.slices = log2(1.0 / errorRate);
            this.sliceSize = (int) Math.ceil((capacity * Math.abs(
                    Math.log(errorRate))) / (slices * 0.480453014));//const: ln(2)^2

            final int vectorSize = (slices * sliceSize);
            this.filter = BitVector.createInitialized(vectorSize, false);
        }

        private Stage(final ScalableBloomFilter<?> parent, final int capacity,
                final double errorRate){

            this.capacity = capacity;
            this.errorRate = errorRate;
            final int scaleMode = parent.scaleFactor;
            final int m0 = parent.stages[0].sliceSize;
            final int k0 = parent.stages[0].slices;
            final int i = parent.stages.length + 1;
            final double tighteningRatio = 0.9;
            this.slices = (int) Math.round(
                    Math.ceil(k0 + (i * log2(1.0 / tighteningRatio))));
            
            this.sliceSize = (int) Math.ceil((m0 * (Math.pow(scaleMode, i - 1))));
            final int vectorSize = (slices * sliceSize);
            this.filter = BitVector.createInitialized(vectorSize, false);
        }

        /**
         * Returns a string representation of
         * this <code>ScalableBloomFilter.Stage</code>.<br>
         * This method includes the state of the internal bit vector
         * of the Stage object and is therefore only intended to be used
         * for debugging purposes
         *
         * @return A string representation
         *         of this <code>ScalableBloomFilter.Stage</code>
         */
        @Override
        public String toString(){
            final StringBuilder sb = new StringBuilder();
            final String nl = System.lineSeparator();
            sb.append(filter.toString());
            sb.append(nl);
            return sb.toString();
        }

        /**
         * Adds the specified hash to this Stage
         * 
         * @param hash The hash value to be added to this Stage
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
         * Returns the fill ratio of this Stage
         * 
         * @return The fill ratio of this Stage
         */
        private double fillRatio(){
            return ((double)filter.bitsSet() / (double)filter.size());
        }

        /**
         * Indicates whether this Stage has reached its maximum allowed capacity,
         * i.e. its fill ration
         * 
         * @return True if this Stage's fill ration is over the
         *         maximum allowed value, false otherwise
         */
        private boolean isFull(){
            return (filter.bitsSet() > (filter.size() / 2));
        }

        /**
         * Indicates whether this Stage is empty, i.e. all bits are
         * set to zero
         * 
         * @return True if this Stage has no inserted elements and
         *         is empty, false otherwise
         */
        private boolean isEmpty(){
            return (filter.bitsSet() == 0);
        }

        /**
         * Returns the size in bytes this Stage has allocated in memory
         * 
         * @return The number of bytes that this Stage has allocated in memory
         */
        private long sizeInBytes(){
            //Capacity of the BitVector
            // + 8 bytes for internally used vars in the BitVector
            // + 20 bytes for internally used vars in this Stage
            return filter.asArray().length + 28;
        }

        /**
         * Returns the estimated number of elements in this Stage
         * 
         * @return The estimated number of elements this Stage currently holds
         */
        private double approximateSize(){
            return (-sliceSize * Math.log(1 - fillRatio()));
        }
    }
}
