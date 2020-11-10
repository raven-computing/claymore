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

/**
 * Functional interface for replacing values in a {@link Column} of
 * a {@link DataFrame}. This interface defines one method for replacing a
 * column value at a specific row index with a user defined value.
 * 
 * @author Phil Gaiser
 * @see ValueReplacement
 * @since 4.0.0
 *
 */
@FunctionalInterface
public interface IndexedValueReplacement<T> {

    /**
     * Replaces the specified value at the specified row index passed to
     * this method with the value returned by this method 
     * 
     * @param index The row index of the column value to replace
     * @param value The value currently in place in the column at the
     *              specified row index
     * @return The new value of column at the given row index
     */
    public T replace(int index, T value);

}
