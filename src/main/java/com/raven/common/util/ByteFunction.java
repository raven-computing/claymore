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

package com.raven.common.util;

/**
 * A function which takes bytes as its input and outputs bytes.
 * 
 * @author Phil Gaiser
 * @since 3.0.0
 *
 */
@FunctionalInterface
public interface ByteFunction {
    
    /**
     * Applies this function to the input argument
     * 
     * @param input The input bytes for this function
     * @return The resulting bytes of this function applied to the specified bytes
     */
    public byte[] apply(byte[] input);
    
}
