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

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * Indicates that a member field represents an item in a row
 * of a DataFrame.<br>
 * The one and only attribute of this annotation specifies the
 * name of the column the annotated field belongs to. If the name is
 * omitted, then the identifying name of the underlying field is
 * used to reference the column.
 * 
 * <p>When using DataFrames with a static structure, i.e. the defined 
 * column structure does not change at runtime, the API user can define
 * a custom class that represents a row for the given DataFrame. Instances
 * of that class can then be used to add, insert and get rows from the 
 * DataFrame without having to deal with an array of objects directly.
 * 
 * <p>Every class which is supposed to use this feature must be marked as
 * a <code>Row</code> by implementing the {@link Row} interface and have
 * this annotation attached to at least one member field.<br>
 * Furthermore, such a class must declare a default no-arguments constructor. 
 * For this feature to work, all columns must be labeled before any
 * method exposed by this feature is called. Failing to do so will
 * result in an exception being raised at runtime.<br>
 * The type of the field this annotation is attached to must be equal
 * to the type used by the column that field belongs to. For
 * DefaultDataFrames all annotated fields can be both of primitive or
 * the equivalent wrapper object type. For NullableDataFrames however,
 * only the wrapper object types are permitted.
 * 
 * <p>Not all fields of a row class must be annotated as a <code>RowItem</code>. 
 * Those not annotated are exluded and ignored when passing an
 * instance of a class to  a DataFrame for an operation regarding rows.
 * That way a class being used as a row for DataFrames does not necessarily
 * have to be a plain row model class but instead can carry other
 * data as well.
 * 
 * @author Phil Gaiser
 * @see Row
 * @since 2.0.0
 *
 */
@Documented
@Retention(RUNTIME)
@Target(FIELD)
public @interface RowItem {

    String value() default "";

}
