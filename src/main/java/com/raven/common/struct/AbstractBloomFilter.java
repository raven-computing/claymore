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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;

import com.raven.common.io.SerializationException;
import com.raven.common.io.Serializer;

/**
 * An abstract super class for <code>ProbabilisticSets</code> implementing
 * a bloom filter data structure. This class provides common utility methods
 * used by concrete bloom filter implementations.
 * 
 * @author Phil Gaiser
 * @see ProbabilisticSet
 *
 * @param <E> The type of elements to be used by the bloom filter
 */
public abstract class AbstractBloomFilter<E> implements ProbabilisticSet<E> {

    protected Serializer<E> serializer;

    public AbstractBloomFilter(final Serializer<E> serializer){
        this.serializer = serializer;
    }

    /**
     * Hashes the specified element
     * 
     * @param element The element to hash
     * @return A hash for the specified element as a primitive long
     */
    protected long hash(final E element){
        if(serializer != null){
            return hash(serializer.serialize(element));
        }else if(element instanceof String){
            return hash((String)element);
        }else if(element instanceof Serializable){
            return hash(serialize(element));
        }else{
            return hash(element.toString());
        }
    }

    /**
     * Hashes the specified String value
     * 
     * @param str The String object to hash
     * @return A hash for the specified String value as a primitive long
     */
    private long hash(final String str){
        if(str.isEmpty()){
            throw new IllegalArgumentException(
                    "Invalid Bloom filter element: empty String");

        }
        return hash(str.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Hashes the specified bytes
     * 
     * @param bytes The bytes to hash
     * @return A hash for the specified byte values as a primitive long
     */
    private long hash(final byte[] bytes){
        return hashMurmur3(bytes);
    }

    /**
     * Serializes the specified Serializable object by the standard
     * serialization mechanism
     * 
     * @param element The element to serialize.
     *                Must be of type <code>Serializable</code>
     * @return A serialized form of the specified element
     * @throws SerializationException If an error occurs during serialization
     */
    private byte[] serialize(final Object element) throws SerializationException{
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oss = null;
        try{
            oss = new ObjectOutputStream(baos);
            oss.writeObject(element);
            return baos.toByteArray();
        }catch(IOException ex){
            throw new SerializationException("Failed to serialize element", ex);
        }finally{
            if(oss != null){
                try{
                    oss.close();
                }catch(IOException ex){
                    throw new SerializationException(
                            "Failed to close stream for element", ex);

                }
            }
        }
    }

    /**
     * Computes the Murmur3 hash value from the specified bytes.<br>
     * The original implementation of this method
     * was taken from Google's Guava library which is a
     * port of Austin Applebys original C++ code
     */
    private static long hashMurmur3(final byte[] data){
        long h1 = 0;
        long h2 = 0;
        int ptr = -1;
        final int blocks = (data.length / 16);
        for(int i = 0; i<blocks; ++i){
            long k1 = (((long) (data[++ptr]) << 56)
                     | ((long) (data[++ptr] & 0xff) << 48)
                     | ((long) (data[++ptr] & 0xff) << 40)
                     | ((long) (data[++ptr] & 0xff) << 32)
                     | ((long) (data[++ptr] & 0xff) << 24)
                     | ((long) (data[++ptr] & 0xff) << 16)
                     | ((long) (data[++ptr] & 0xff) << 8)
                     | ((long) (data[++ptr] & 0xff)));

            long k2 = (((long) (data[++ptr]) << 56)
                     | ((long) (data[++ptr] & 0xff) << 48)
                     | ((long) (data[++ptr] & 0xff) << 40)
                     | ((long) (data[++ptr] & 0xff) << 32)
                     | ((long) (data[++ptr] & 0xff) << 24)
                     | ((long) (data[++ptr] & 0xff) << 16)
                     | ((long) (data[++ptr] & 0xff) << 8)
                     | ((long) (data[++ptr] & 0xff)));

            h1 ^= mixK1(k1);

            h1 = Long.rotateLeft(h1, 27);
            h1 += h2;
            h1 = h1 * 5 + 0x52dce729;

            h2 ^= mixK2(k2);

            h2 = Long.rotateLeft(h2, 31);
            h2 += h1;
            h2 = h2 * 5 + 0x38495ab5;
        }
        final int remaining = (data.length % 16);
        if(remaining > 0){
            ++ptr;
            long k1 = 0;
            long k2 = 0;
            switch (remaining){
            case 15:
                k2 ^= (long) (data[ptr + 14] & 0xff) << 48;

            case 14:
                k2 ^= (long) (data[ptr + 13] & 0xff) << 40;

            case 13:
                k2 ^= (long) (data[ptr + 12] & 0xff) << 32;

            case 12:
                k2 ^= (long) (data[ptr + 11] & 0xff) << 24;

            case 11:
                k2 ^= (long) (data[ptr + 10] & 0xff) << 16;

            case 10:
                k2 ^= (long) (data[ptr + 9] & 0xff) << 8;

            case 9:
                k2 ^= (long) (data[ptr + 8] & 0xff);

            case 8:
                k1 ^= (((long) (data[ptr++]) << 56)
                     | ((long) (data[ptr++] & 0xff) << 48)
                     | ((long) (data[ptr++] & 0xff) << 40)
                     | ((long) (data[ptr++] & 0xff) << 32)
                     | ((long) (data[ptr++] & 0xff) << 24)
                     | ((long) (data[ptr++] & 0xff) << 16)
                     | ((long) (data[ptr++] & 0xff) << 8)
                     | ((long) (data[ptr++] & 0xff)));

                break;
            case 7:
                k1 ^= (long) (data[ptr + 6] & 0xff) << 48;

            case 6:
                k1 ^= (long) (data[ptr + 5] & 0xff) << 40;

            case 5:
                k1 ^= (long) (data[ptr + 4] & 0xff) << 32;

            case 4:
                k1 ^= (long) (data[ptr + 3] & 0xff) << 24;

            case 3:
                k1 ^= (long) (data[ptr + 2] & 0xff) << 16;

            case 2:
                k1 ^= (long) (data[ptr + 1] & 0xff) << 8;

            case 1:
                k1 ^= (long) (data[ptr] & 0xff);
                break;

            default:
                throw new AssertionError("Implementation Error");
            }
            h1 ^= mixK1(k1);
            h2 ^= mixK2(k2);
        }
        h1 += h2;
        h2 += h1;

        h1 = fmix64(h1);
        h2 = fmix64(h2);

        h1 += h2;
        h2 += h1;

        return h1;
    }

    private static long mixK1(long k1){
        k1 *= 0x87c37b91114253d5L;
        k1 = Long.rotateLeft(k1, 31);
        k1 *= 0x4cf5ad432745937fL;
        return k1;
    }

    private static long mixK2(long k2){
        k2 *= 0x4cf5ad432745937fL;
        k2 = Long.rotateLeft(k2,  33);
        k2 *= 0x87c37b91114253d5L;
        return k2;
    }

    private static long fmix64(long k){
        k ^= k >>> 33;
                k *= 0xff51afd7ed558ccdL;
                k ^= k >>> 33;
                k *= 0xc4ceb9fe1a85ec53L;
                k ^= k >>> 33;
                return k;
    }

    /**
     * Computes the base 2 logarithm of the specified double value, rounded
     * to the nearest integer. The specified double value must be
     * a positive and finite number
     * 
     * @param number The number to compute the base 2 logarithm for
     * @return The logarithm of base 2 of the specified number
     *         rounded to the nearest integer
     */
    public static int log2(final double number){
        final int exponent = Math.getExponent(number);
        if(!(exponent >= Double.MIN_EXPONENT)){
            return log2(number * (0x000fffffffffffffL + 1)) - 52;
        }
        //If the number is positive, finite and normal,
        //then increment the exponent
        boolean add;
        if((number > 0.0) && (exponent <= Double.MAX_EXPONENT)){//Is positive and finite
            long bits = Double.doubleToRawLongBits(number);
            bits &= 0x000fffffffffffffL;
            final long significand = (exponent == (Double.MIN_EXPONENT - 1))
                    ? (bits << 1)
                    : (bits | 0x000fffffffffffffL + 1);

                    add = ((significand & (significand - 1)) != 0);
        }else{
            add = true;
        }
        return (add ? (exponent + 1) : exponent);
    }
}
