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

/**
 * Enumerates all hash functions supported by the <code>Hash</code> class.<br>
 * A HashFunction must implement the <code>ByteFunction</code> interface.
 * 
 * @author Phil Gaiser
 * @since 3.0.0
 *
 */
public enum HashFunction implements ByteFunction {

    /**
     * A hash function implementing the MD5 algorithm.
     *
     */
    MD5{
        @Override
        public byte[] apply(byte[] input){
            return Hash.md5(input);
        }
    },

    /**
     * A hash function implementing the SHA-1 algorithm.
     *
     */
    SHA_1{
        @Override
        public byte[] apply(byte[] input){
            return Hash.sha1(input);
        }
    },

    /**
     * A hash function implementing the SHA-224 algorithm.
     *
     */
    SHA_224{
        @Override
        public byte[] apply(byte[] input){
            return Hash.sha224(input);
        }
    },

    /** 
     * A hash function implementing the SHA-256 algorithm.
     *
     */
    SHA_256{
        @Override
        public byte[] apply(byte[] input){
            return Hash.sha256(input);
        }
    },

    /**
     * A hash function implementing the SHA-384 algorithm.
     *
     */
    SHA_384{
        @Override
        public byte[] apply(byte[] input){
            return Hash.sha384(input);
        }
    },

    /**
     * A hash function implementing the SHA-512 algorithm.
     *
     */
    SHA_512{
        @Override
        public byte[] apply(byte[] input){
            return Hash.sha512(input);
        }
    };

}
