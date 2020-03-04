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

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * A {@link Serializer} for serializing and deserializing Strings based on
 * a specified character set.
 * 
 * @author Phil Gaiser
 * @since 3.0.0
 *
 */
public class StringSerializer implements Serializer<String> {

    private Charset charset;

    /**
     * Constructs a new <code>StringSerializer</code> for serializing
     * and deserializing <b>UTF-8</b> encoded Strings
     * 
     * @see #StringSerializer(Charset)
     */
    public StringSerializer(){
        this(StandardCharsets.UTF_8);
    }

    /**
     * Constructs a new <code>StringSerializer</code> for serializing
     * and deserializing Strings encoded with the specified character set
     * 
     * @param charset The character set to be used when encoding and decoding Strings
     */
    public StringSerializer(final Charset charset){
        if(charset == null){
            throw new IllegalArgumentException("Charset argument must not be null");
        }
        this.charset = charset;
    }

    /**
     * Serializes the specified String to an array of bytes
     * 
     * @param object The String object to serialize
     * @return An array of bytes which represents the specified String in a serialized form
     * @throws SerializationException If an error occurs during serialization
     * @throws NullPointerException If the specified object is null
     */
    @Override
    public byte[] serialize(String object) throws SerializationException{
        return object.getBytes(charset);
    }

    /**
     * Deserializes the specified array of bytes to a String object
     * 
     * @param bytes The array of bytes to deserialize
     * @return A String from the specified bytes
     * @throws SerializationException If an error occurs during deserialization
     * @throws NullPointerException If the specified byte array is null
     */
    @Override
    public String deserialize(byte[] bytes) throws SerializationException{
        return new String(bytes, charset);
    }

    /**
     * Creates a new <code>StringSerializer</code> for serializing and
     * deserializing UTF-8 encoded Strings
     * 
     * @return A <code>StringSerializer</code> for serializing and deserializing
     *         UTF-8 encoded Strings
     */
    public static StringSerializer forUTF8Encoding(){
        return new StringSerializer();
    }
}
