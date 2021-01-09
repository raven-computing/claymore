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
 * A class to represent modifiable key-value pairs whose keys are always Strings.
 * The value is of a generic type. Both keys and values of WritableItems can be
 * modified at runtime. Two items are considered to be equal if both their
 * keys and values are equal. 
 * 
 * @author Phil Gaiser
 * @since 3.0.0
 *
 * @param <V> The type of values to be used by the WritableItem
 */
public class WritableItem<V> implements Item<V> {

    private String key;
    private V value;

    /**
     * Constructs a new empty <code>WritableItem</code> with no key and no value
     */
    public WritableItem(){ }

    /**
     * Constructs a new <code>WritableItem</code> with the specified key
     * and a value of null
     * 
     * @param key The key of the item
     */
    public WritableItem(final String key){
        this.key = key;
    }

    /**
     * Constructs a new <code>WritableItem</code> with the specified key and value
     * 
     * @param key The key of the item
     * @param value The value of the item
     */
    public WritableItem(final String key, final V value){
        this.key = key;
        this.value = value;
    }

    /**
     * Gets the key of this item
     * 
     * @return The key of this item
     */
    @Override
    public String getKey(){
        return this.key;
    }

    /**
     * Sets the key of this item to the specified String
     * 
     * @param key The new key of this item
     */
    @Override
    public void setKey(final String key){
        this.key = key;
    }

    /**
     * Indicates whether this item has a key
     * 
     * @return True if this item's key is not null and not an empty String.
     *         False if this item's key is null or the empty String
     */
    @Override
    public boolean hasKey(){
        return ((this.key != null) && !(this.key.isEmpty()));
    }

    /**
     * Gets the value of this item
     * 
     * @return The value of this item
     */
    @Override
    public V getValue(){
        return this.value;
    }

    /**
     * Sets the value of this item to the specified value
     * 
     * @param value The new value of this item
     */
    @Override
    public void setValue(final V value){
        this.value = value;
    }

    /**
     * Indicates whether this item has a value
     * 
     * @return True if this item's value is not null.
     *         False if this item's value is null
     */
    @Override
    public boolean hasValue(){
        return (this.value != null);
    }

    /**
     * Indicates whether this item is empty. An item is empty
     * if it neither has a key nor a value
     * 
     * @return True if this item is empty, false if it is not empty
     */
    @Override
    public boolean isEmpty(){
        return !hasKey() && !hasValue();
    }

    /**
     * Returns a String representation of this item
     * 
     * @return A String holding both the key and the value of this item
     */
    @Override
    public String toString(){
        return key + "=" + value;
    }

    @Override
    public int hashCode(){
        int hash = 0;
        if(key != null){
            hash = key.hashCode(); 
        }
        hash *= 37;
        if(value != null){
            hash += value.hashCode();
        }
        return hash;
    }

    /**
     * Indicates whether this item is equal to the specified item.
     * Two items are considered equal if and only if both the names
     * and values are equal.<br>
     * If the specified item is not an <code>Item</code> object,
     * not a <code>FinalItem</code> object or is
     * null, then this method returns false
     *
     * @param o The <code>Item</code> to test for equality with this item
     * @return True if the given object is equal to this item, false otherwise
     */
    @Override
    public boolean equals(Object o){
        if(this == o){
            return true;
        }
        if(o instanceof Item){
            Item<?> item = (Item<?>) o;
            final boolean k1 = (key != null);
            final boolean k2 = (item.getKey() != null);
            if(k1 ^ k2){
                return false;
            }
            if(k1 && k2){
                if(!key.equals(item.getKey())){
                    return false;
                }
            }
            final boolean v1 = (value != null);
            final boolean v2 = (item.getValue() != null);
            if(v1 ^ v2){
                return false;
            }
            if(v1 && v2){
                if(!value.equals(item.getValue())){
                    return false;
                }
            }
            return true;
        }else if(o instanceof FinalItem){
            FinalItem<?> item = (FinalItem<?>) o;
            final boolean k1 = (key != null);
            final boolean k2 = (item.getKey() != null);
            if(k1 ^ k2){
                return false;
            }
            if(k1 && k2){
                if(!key.equals(item.getKey())){
                    return false;
                }
            }
            final boolean v1 = (value != null);
            final boolean v2 = (item.getValue() != null);
            if(v1 ^ v2){
                return false;
            }
            if(v1 && v2){
                if(!value.equals(item.getValue())){
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    @Override
    public int compareTo(Item<V> o){
        final String k = ((o.getKey() != null) ? o.getKey() : "");
        return ((key != null) ? this.key.compareTo(k) : "".compareTo(k));
    }
}
