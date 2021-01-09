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
 * Functional interface for replacing values in a {@link Column} of
 * a {@link DataFrame}. This interface defines one method for replacing a
 * column value with a user defined value.
 * 
 * @author Phil Gaiser
 * @see IndexedValueReplacement
 * @since 4.0.0
 *
 */
@FunctionalInterface
public interface ValueReplacement<T> {

    /**
     * Replaces the specified value passed to this method with
     * the value returned by this method 
     * 
     * @param value The value currently in place in the underlying column
     * @return The new value in the underlying column
     */
    public T replace(T value);

}
