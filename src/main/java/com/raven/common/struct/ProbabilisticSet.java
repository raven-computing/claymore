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

/**
 * An unordered collection of probabilistically distinct elements. The mechanism
 * by which all elements in a probabilistic set are unequivocally distinguishable
 * is subject to a rate of error. That rate is innately probabilistic, i.e. in
 * general it is not predictable by an external user for what elements an error
 * is going to occur.
 * 
 * <p>This interface defines two basic operations for probabilistic sets.<br>
 * On the one hand elements can be added to a probabilistic set which makes them
 * a member of said set and on the other hand the presence of particular elements
 * can be queried. An element can be added to the set by means of
 * the {@link #add(Object)} method. It is a deterministic operation in terms of
 * predictability of the set state. This means that adding an element to a
 * probabilistic set is not subject to any uncertainty. Consequently, adding an
 * element to a probabilistic set causes all subsequent calls to
 * the {@link #contains(Object)} method to affirm the presence of that element.
 * However, performing membership queries for arbitrary elements is subject to a
 * rate of error. More specific, a probabilistic set might erroneously affirm
 * the presence of an element which is not in fact a member if that set. The
 * likelihood of such a false positive is always non-zero. On the other hand, a
 * probabilistic set will never produce false negatives for all elements added
 * to it, that is, it will never deny the presence of an element in its set.
 * 
 * <p>The possibility of errors when performing membership queries generally
 * comes with a decreased memory consumption tradeoff. Probabilistic sets do
 * not store the entire elements in memory during runtime which makes them a
 * very efficient data structure compared to classic sets. The exact magnitude
 * of memory reduction depends on the concrete implementation.
 * 
 * <p>Any concrete implementation of this interface must exhibit the
 * following properties:<br>
 * The maximum allowed error rate must be configurable upon construction.<br>
 * The initial capacity of elements for which the specified maximum allowed
 * error rate is adhered to must be configurable upon construction.<br>
 * 
 * <p>A probabilistic set implementation may have restrictions on the type of
 * elements that it can contain. It is up to the concrete implementation whether
 * it prohibits the use of null elements and the restrictions it imposes on the
 * types of the elements. Attempting to add an ineligible element to a
 * probabilistic set may cause the implementation to throw an unchecked exception.
 * Attempting to query the presence of an ineligible element may also cause the
 * implementation to throw an exception, or it may simply return false.
 * Concrete classes implementing this interface should explicitly state their
 * restrictions regarding eligible elements and the behaviour in the case of an
 * ineligible element is passed to one of the methods specified by this interface.
 * 
 * @author Phil Gaiser
 * @see StaticBloomFilter
 * @see ScalableBloomFilter
 * @since 3.0.0
 *
 * @param <E> The type of elements to be used by the probabilistic set
 */
public interface ProbabilisticSet<E> {

    /**
     * Adds the specified element to this probabilistic set. Adding an element
     * which is already in the set will have no effect.<br>
     * If the specified element can be accepted by this probabilistic set,
     * then it must be contained in it after this method returns regardless of any
     * configured error probabilities.
     * 
     * <p>Probabilistic sets, just like normal sets, are not required to accept
     * all elements. A probabilistic set may refuse to add any particular element,
     * including null, and throw an exception. Concrete implementations of
     * the ProbabilisticSet interface should document any restrictions on
     * the elements that they can contain
     * 
     * @param element The element to be added to this probabilistic set
     * @throws ClassCastException If the class of the specified element
     *                            prevents it from being added to
     *                            this probabilistic set
     * @throws NullPointerException If the specified element is null and this
     *                              probabilistic set does not permit null elements
     * @throws SerializationException If the specified element cannot be serialized
     * @throws IllegalArgumentException If some property of the specified element
     *                                  prevents it from being added
     *                                  to this probabilistic set
     */
    public void add(E element);

    /**
     * Examines whether this probabilistic set possibly contains
     * the specified element. If this method returns true, then the specified
     * element might be in this set in due consideration of the maximum allowed
     * error probability this probabilistic set was configured with. If this
     * method returns false, then the specified element is certainly
     * not in this set.
     * 
     * <p>As a general contract, the probability of this method returning
     * true on an element which is not contained in this set must, on average,
     * not be greater than the maximum allowed error rate this probabilistic set
     * was configured to adhere to
     * 
     * @param element The element whose presence in this probabilistic set
     *                is to be tested
     * @return True if this probabilistic set possibly contains
     *         the specified element. Returns false if the specified element
     *         is definitely not in this probabilistic probabilistic set
     * @throws ClassCastException If the type of the specified element
     *         is incompatible with this probabilistic set
     * @throws NullPointerException If the specified element is null and this
     *         probabilistic set does not permit null elements
     * @throws SerializationException If the specified element cannot be serialized
     * @throws IllegalArgumentException If some property of the specified element
     *                                  prevents it from being used in membership
     *                                  queries in this probabilistic set
     */
    public boolean contains(E element);

    /**
     * Indicates whether this probabilistic set is empty. An empty set
     * contains no elements
     * 
     * @return True if this set contains no elements, false if it is not
     *         empty and contains at least one element
     */
    public boolean isEmpty();

    /**
     * Returns an estimation of the size of this probabilistic set. The precision of
     * this method is generally implementation dependent. Concrete probabilistic sets
     * are free to return the actual size whenever possible. However, as
     * probabilistic sets do not store the actual objects they contain in memory,
     * the number returned by this method might only be an approximation
     * of the actual size
     * 
     * @return The estimated number of elements in this probabilistic set
     */
    public int approximateSize();

    /**
     * Returns the size in bytes this probabilistic set has allocated in memory
     * in order to store all elements of its current capacity. The amount of
     * required memory might change if more elements are added to the set and
     * an enlargement of the underlying data structure is needed
     * 
     * @return The number of bytes that this probabilistic set has allocated
     *         in memory to store its current capacity
     */
    public long sizeInBytes();

    /**
     * Removes all of the elements from this probabilistic set. The set
     * will be empty after this method returns
     */
    public void clear();

}
