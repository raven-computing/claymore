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

import com.raven.common.struct.ObservableItem.Change.Event;

/**
 * A class to represent modifiable observable key-value pairs whose keys
 * are always Strings. The value is of a generic type. Both keys and values
 * of ObservableItems can be modified at runtime. A listener can be set
 * on an ObservableItem to be notified of such a change. Two items are
 * considered to be equal if both their keys and values are equal. 
 * 
 * @author Phil Gaiser
 * @since 3.0.0
 *
 * @param <V> The type of values to be used by the ObservableItem
 */
public class ObservableItem<V> implements Item<V> {

    /**
     * Listener interface for ObservableItems.
     *
     * @param <V> The type of values to be used by the ObservableItem
     *            observed by the Listener
     */
    public interface Listener<V> {

        /**
         * Called when either the key or value of an ObservableItem has changed
         * 
         * @param change The <code>ObservableItem.Change</code> object
         *               of the change event
         */
        public void onChange(Change<V> change);
    }

    private String key;
    private V value;
    private Listener<V> listener;

    /**
     * Constructs a new <code>ObservableItem</code> with a null key, null value
     * and the specified listener for observing changes
     * 
     * @param listener The <code>ObservableItem.Listener</code> to notify when
     *                 changes to either the key or the value occur
     */
    public ObservableItem(final Listener<V> listener){
        this.listener = listener;
    }

    /**
     * Constructs a new <code>ObservableItem</code> with the specified key
     * and a value of null. No listener is set
     * 
     * @param key The key of the item
     */
    public ObservableItem(final String key){
        this.key = key;
    }

    /**
     * Constructs a new <code>ObservableItem</code> with the specified key and value.
     * No listener is set
     * 
     * @param key The key of the item
     * @param value The value of the item
     */
    public ObservableItem(final String key, final V value){
        this.key = key;
        this.value = value;
    }

    /**
     * Constructs a new <code>ObservableItem</code> with the specified key and value
     * and the specified listener for observing changes
     * 
     * @param key The key of the item
     * @param value The value of the item
     * @param listener The <code>ObservableItem.Listener</code> to notify when
     *                 changes to either the key or the value occur
     */
    public ObservableItem(final String key, final V value, final Listener<V> listener){
        this.key = key;
        this.value = value;
        this.listener = listener;
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
        final String oldKey = this.key;
        this.key = key;
        if(listener != null){
            this.listener.onChange(new Change<V>(Event.KEY, this, oldKey, value));
        }
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
        final V oldValue = this.value;
        this.value = value;
        if(listener != null){
            this.listener.onChange(new Change<V>(Event.VALUE, this, key, oldValue));
        }
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
     * Sets the listener of this ObservableItem. Any previously set
     * listener will be replaced by the specified instance
     * 
     * @param listener The <code>ObservableItem.Listener</code> to set
     *                 for this ObservableItem
     */
    public void setListener(final Listener<V> listener){
        this.listener = listener;
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

    /**
     * Represents a change event for an <code>ObservableItem</code>.
     *
     * @param <V> The type of values used by the
     *            underlying <code>ObservableItem</code>
     */
    public static class Change<V> {

        /**
         * The type of change event.
         * Either the key changed or the value.
         *
         */
        public enum Event {

            /**
             * A change event to indicate that the key
             * of an <code>ObservableItem</code> has changed
             */
            KEY,

            /**
             * A change event to indicate that the value
             * of an <code>ObservableItem</code> has changed
             */
            VALUE
        }

        private Event event;
        private Item<V> item;
        private String oldKey;
        private V oldValue;

        Change(final Event event, final Item<V> item,
                final String oldKey,final V oldValue){

            this.event = event;
            this.item = item;
            this.oldKey = oldKey;
            this.oldValue = oldValue;
        }

        /**
         * Gets the change event type of this Change
         * 
         * @return The change event type of this Change
         */
        public Event getEvent(){
            return this.event;
        }

        /**
         * Gets the ObservableItem that changed
         * 
         * @return The <code>ObservableItem</code> for which
         *         a change has occurred
         */
        public Item<V> getItem(){
            return this.item;
        }

        /**
         * Gets the old key of the ObservableItem
         * 
         * @return The old key of the ObservableItem for which
         *         a change has occurred
         */
        public String getOldKey(){
            return this.oldKey;
        }

        /**
         * Gets the new key of the ObservableItem
         * 
         * @return The new key of the ObservableItem for which
         *         a change has occurred
         */
        public String getNewKey(){
            return this.item.getKey();
        }

        /**
         * Gets the old value of the ObservableItem
         * 
         * @return The old value of the ObservableItem for which
         *         a change has occurred
         */
        public V getOldValue(){
            return this.oldValue;
        }

        /**
         * Gets the new value of the ObservableItem
         * 
         * @return The new value of the ObservableItem for which
         *         a change has occurred
         */
        public V getNewValue(){
            return this.item.getValue();
        }
    }

}
