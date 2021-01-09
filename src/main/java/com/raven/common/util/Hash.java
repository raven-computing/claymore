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

package com.raven.common.util;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * This class provides static utility methods for hashing strings
 * and binary data.<br>
 * The following hash functions are supported:<br>
 * <ul>
 *   <li>MD5</li>
 *   <li>SHA-1</li>
 *   <li>SHA-224</li>
 *   <li>SHA-256</li>
 *   <li>SHA-384</li>
 *   <li>SHA-512</li>
 * </ul>
 * 
 * <p>All methods of this class may wrap a <code>NoSuchAlgorithmException</code>
 * into a <code>IllegalStateException</code> if applicable. If not otherwise
 * stated by concrete methods, passing null as an argument to a method may result
 * in a <code>NullPointerException</code> being thrown.
 * 
 * <p>This class cannot be instantiated.
 * 
 * @author Phil Gaiser
 * @see HashFunction
 * @since 3.0.0
 *
 */
public final class Hash {

    private Hash(){ }

    /**
     * Returns the MD5 hash of the specified <b>UTF-8</b> encoded string
     * 
     * @param input The UTF-8 encoded String to hash
     * @return The MD5 hash of the specified String
     */
    public static byte[] md5(final String input){
        return md5(input, StandardCharsets.UTF_8);
    }

    /**
     * Returns the MD5 hash of the specified string encoded with
     * the specified character set
     * 
     * @param input The String to hash
     * @param charset The <code>Charset</code> to be used for encoding
     *                the specified String
     * @return The MD5 hash of the specified String
     */
    public static byte[] md5(final String input, final Charset charset){
        return md5(input.getBytes(charset));
    }

    /**
     * Returns the MD5 hash of the specified bytes
     * 
     * @param input The bytes to hash
     * @return The MD5 hash of the specified bytes
     */
    public static byte[] md5(final byte[] input){
        return hash(input, "MD5");
    }

    /**
     * Returns the SHA-1 hash of the specified <b>UTF-8</b> encoded string
     * 
     * @param input The UTF-8 encoded String to hash
     * @return The SHA-1 hash of the specified String
     */
    public static byte[] sha1(final String input){
        return sha1(input, StandardCharsets.UTF_8);
    }

    /**
     * Returns the SHA-1 hash of the specified string encoded with
     * the specified character set
     * 
     * @param input The String to hash
     * @param charset The <code>Charset</code> to be used for encoding
     *                the specified String
     * @return The SHA-1 hash of the specified String
     */
    public static byte[] sha1(final String input, final Charset charset){
        return sha1(input.getBytes(charset));
    }

    /**
     * Returns the SHA-1 hash of the specified bytes
     * 
     * @param input The bytes to hash
     * @return The SHA-1 hash of the specified bytes
     */
    public static byte[] sha1(final byte[] input){
        return hash(input, "SHA-1");
    }

    /**
     * Returns the SHA-224 hash of the specified <b>UTF-8</b> encoded string
     * 
     * @param input The UTF-8 encoded String to hash
     * @return The SHA-224 hash of the specified String
     */
    public static byte[] sha224(final String input){
        return sha224(input, StandardCharsets.UTF_8);
    }

    /**
     * Returns the SHA-224 hash of the specified string encoded with
     * the specified character set
     * 
     * @param input The String to hash
     * @param charset The <code>Charset</code> to be used for encoding
     *                the specified String
     * @return The SHA-224 hash of the specified String
     */
    public static byte[] sha224(final String input, final Charset charset){
        return sha224(input.getBytes(charset));
    }

    /**
     * Returns the SHA-224 hash of the specified bytes
     * 
     * @param input The bytes to hash
     * @return The SHA-224 hash of the specified bytes
     */
    public static byte[] sha224(final byte[] input){
        return hash(input, "SHA-224");
    }

    /**
     * Returns the SHA-256 hash of the specified <b>UTF-8</b> encoded string
     * 
     * @param input The UTF-8 encoded String to hash
     * @return The SHA-256 hash of the specified String
     */
    public static byte[] sha256(final String input){
        return sha256(input, StandardCharsets.UTF_8);
    }

    /**
     * Returns the SHA-256 hash of the specified string encoded with
     * the specified character set
     * 
     * @param input The String to hash
     * @param charset The <code>Charset</code> to be used for encoding
     *                the specified String
     * @return The SHA-256 hash of the specified String
     */
    public static byte[] sha256(final String input, final Charset charset){
        return sha256(input.getBytes(charset));
    }

    /**
     * Returns the SHA-256 hash of the specified bytes
     * 
     * @param input The bytes to hash
     * @return The SHA-256 hash of the specified bytes
     */
    public static byte[] sha256(final byte[] input){
        return hash(input, "SHA-256");
    }

    /**
     * Returns the SHA-384 hash of the specified <b>UTF-8</b> encoded string
     * 
     * @param input The UTF-8 encoded String to hash
     * @return The SHA-384 hash of the specified String
     */
    public static byte[] sha384(final String input){
        return sha384(input, StandardCharsets.UTF_8);
    }

    /**
     * Returns the SHA-384 hash of the specified string encoded with
     * the specified character set
     * 
     * @param input The String to hash
     * @param charset The <code>Charset</code> to be used for encoding
     *                the specified String
     * @return The SHA-384 hash of the specified String
     */
    public static byte[] sha384(final String input, final Charset charset){
        return sha384(input.getBytes(charset));
    }

    /**
     * Returns the SHA-384 hash of the specified bytes
     * 
     * @param input The bytes to hash
     * @return The SHA-384 hash of the specified bytes
     */
    public static byte[] sha384(final byte[] input){
        return hash(input, "SHA-384");
    }

    /**
     * Returns the SHA-512 hash of the specified <b>UTF-8</b> encoded string
     * 
     * @param input The UTF-8 encoded String to hash
     * @return The SHA-512 hash of the specified String
     */
    public static byte[] sha512(final String input){
        return sha512(input, StandardCharsets.UTF_8);
    }

    /**
     * Returns the SHA-512 hash of the specified string encoded with
     * the specified character set
     * 
     * @param input The String to hash
     * @param charset The <code>Charset</code> to be used for encoding
     *                the specified String
     * @return The SHA-512 hash of the specified String
     */
    public static byte[] sha512(final String input, final Charset charset){
        return sha512(input.getBytes(charset));
    }

    /**
     * Returns the SHA-512 hash of the specified bytes
     * 
     * @param input The bytes to hash
     * @return The SHA-512 hash of the specified bytes
     */
    public static byte[] sha512(final byte[] input){
        return hash(input, "SHA-512");
    }

    /**
     * Returns a hash of the specified bytes using the specified hash function
     * 
     * @param hashFunction The <code>HashFunction</code> to use to create
     *                     the hash of the specified bytes
     * @param input The bytes to hash with the specified function
     * @return The hash of the specified bytes produced
     *         by the specified hash function
     */
    public static byte[] of(final HashFunction hashFunction, final byte[] input){
        return hashFunction.apply(input);
    }

    /**
     * Returns a hash for the specified bytes using the hash function
     * denoted by the specified name
     * 
     * @param input The bytes to hash with the specified function
     * @param algorithm The name of the algorithm to be used for hashing
     * @return The hash of the specified bytes produced
     *         by the specified algorithm
     * @throws IllegalStateException If the specified algorithm cannot
     *                               be provided in this environment and
     *                               <code>MessageDigest.getInstance()</code>
     *                               throws a <code>NoSuchAlgorithmException</code>
     */
    private static byte[] hash(final byte[] input, final String algorithm)
            throws IllegalStateException{

        try{
            final MessageDigest md = MessageDigest.getInstance(algorithm);
            md.update(input);
            return md.digest();
        }catch(NoSuchAlgorithmException ex){
            throw new IllegalStateException(ex);
        }
    }
}
