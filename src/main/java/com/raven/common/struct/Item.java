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

/**
 * A class to represent modifiable key-value pairs. The key of an Item
 * is always a String. The value is of a generic type. Both keys and values
 * of items can be modified at runtime. Two items are considered to be
 * equal if both their keys and values are equal.
 * Items are <code>Comparable</code>. The comparison is based on the
 * lexicographic order of the String keys.
 * 
 * @author Phil Gaiser
 * @see WritableItem
 * @see ObservableItem
 * @see FinalItem
 * @since 3.0.0
 *
 * @param <V> The type of values to be used by the Item
 */
public interface Item<V> extends Comparable<Item<V>> {

    /**
     * Gets the key of this item
     * 
     * @return The key of this item
     */
    public String getKey();

    /**
     * Sets the key of this item to the specified String
     * 
     * @param key The new key of this item
     */
    public void setKey(String key);

    /**
     * Indicates whether this item has a key
     * 
     * @return True if this item's key is not null and not an empty String.
     *         False if this item's key is null or the empty String
     */
    public boolean hasKey();

    /**
     * Gets the value of this item
     * 
     * @return The value of this item
     */
    public V getValue();

    /**
     * Sets the value of this item to the specified value
     * 
     * @param value The new value of this item
     */
    public void setValue(V value);

    /**
     * Indicates whether this item has a value
     * 
     * @return True if this item's value is not null.
     *         False if this item's value is null
     */
    public boolean hasValue();

    /**
     * Indicates whether this item is empty. An item is empty
     * if it neither has a key nor a value
     * 
     * @return True if this item is empty, false if it is not empty
     */
    public boolean isEmpty();

}
