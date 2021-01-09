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

package com.raven.common.io;

import com.raven.common.struct.DataFrame;

/**
 * A {@link Serializer} for serializing and deserializing DataFrames based on
 * a specified compression mode. This class is provided to support circumstances
 * where a concrete instance of a <code>Serializer</code> is required. API users
 * can usually call the static methods of {@link DataFrameSerializer} directly.
 * 
 * @author Phil Gaiser
 * @since 3.0.0
 *
 */
public class DataFrameSerializerInstance implements Serializer<DataFrame> {

    private boolean mode;

    /**
     * Constructs a <code>DataFrameSerializerInstance</code> with
     * an enabled compression mode
     */
    public DataFrameSerializerInstance(){
        this(DataFrameSerializer.MODE_COMPRESSED);
    }

    /**
     * Constructs a <code>DataFrameSerializerInstance</code> with
     * the specified compression mode
     * 
     * @param mode The compression mode to be used by the
     *             constructed DataFrameSerializerInstance object. Must be either
     *             {@link DataFrameSerializer#MODE_COMPRESSED}
     *             or {@link DataFrameSerializer#MODE_UNCOMPRESSED}
     */
    public DataFrameSerializerInstance(final boolean mode){
        this.mode = mode;
    }

    /**
     * Serializes the specified DataFrame to an array of bytes
     * 
     * @param object The DataFrame object to serialize
     * @return An array of bytes which represents the specified DataFrame
     *         in a serialized form
     * @throws SerializationException If an error occurs during serialization
     * @throws NullPointerException If the specified DataFrame is null
     */
    @Override
    public byte[] serialize(DataFrame object) throws SerializationException{
        return DataFrameSerializer.serialize(object, mode);
    }

    /**
     * Deserializes the specified array of bytes to a DataFrame object
     * 
     * @param bytes The array of bytes to deserialize
     * @return A DataFrame from the specified bytes
     * @throws SerializationException If an error occurs during deserialization
     * @throws NullPointerException If the specified byte array is null
     */
    @Override
    public DataFrame deserialize(byte[] bytes) throws SerializationException{
        return DataFrameSerializer.deserialize(bytes);
    }
}
