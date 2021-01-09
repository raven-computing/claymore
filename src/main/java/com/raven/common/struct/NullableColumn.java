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
 * Abstract class all columns permitting the use of null values must extend.
 * 
 * @author Phil Gaiser
 * @see NullableByteColumn
 * @see NullableShortColumn
 * @see NullableIntColumn
 * @see NullableLongColumn
 * @see NullableFloatColumn
 * @see NullableDoubleColumn
 * @see NullableStringColumn
 * @see NullableCharColumn
 * @see NullableBooleanColumn
 * @see NullableBinaryColumn
 *
 */
public abstract class NullableColumn extends Column {

    @Override
    public boolean isNullable(){
        return true;
    }

    @Override
    public Object getDefaultValue(){
        return null;
    }
}
