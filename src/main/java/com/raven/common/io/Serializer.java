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

package com.raven.common.io;

/**
 * A Serializer is an object used for serialization and deserialization of objects
 * of a specific type. Classes implementing the <code>Serializer</code> interface
 * must implement two methods:<br>
 * <ul>
 *   <li><code>serialize()</code> for serializing
 *   objects of type <code>T</code> </li>
 *   <li><code>deserialize()</code> for deserializing
 *   objects of type <code>T</code> </li>
 * </ul>
 * 
 * This interface represents a common abtraction layer
 * for serialization of objects.<br>
 * 
 * @author Phil Gaiser
 * @since 3.0.0
 * 
 * @param <T> The type of objects to be serialized
 *            and deserialized by the Serializer
 * 
 */
public interface Serializer<T> {

    /**
     * Serializes the specified object to an array of bytes
     * 
     * @param object The object to serialize
     * @return An array of bytes which represents the specified
     *         object in a serialized form
     * @throws SerializationException Thrown by an implementation of this
     *                                method if an error occurs during serialization
     */
    public byte[] serialize(T object) throws SerializationException;

    /**
     * Deserializes the specified array of bytes to an object 
     * 
     * @param bytes The array of bytes to deserialize
     * @return An object of the type the underlying serializer
     *         is responsible for
     * @throws SerializationException Thrown by an implementation of
     *                                this method if an error occurs
     *                                during deserialization
     */
    public T deserialize(byte[] bytes) throws SerializationException;

}
