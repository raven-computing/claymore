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
 * Marker interface to be used by classes that wish to utilise the row 
 * item annotation feature.
 * 
 * <p>When using DataFrames with a static structure, i.e. the defined 
 * column structure does not change at runtime, the API user can define
 * a custom class that represents a row for the given DataFrame. Instances
 * of that class can then be used to add, insert and get rows from the 
 * DataFrame without having to deal with an array of objects directly.
 * 
 * <p>Every class which is supposed to use this feature must be marked as
 * a <code>Row</code> by implementing this interface and annotating at least
 * one member field with the <code>RowItem</code> annotation.<br>
 * Furthermore, that class must declare a default no-args constructor.
 * 
 * <p>Generally, a <code>RowItem</code> annotation attached to a member field 
 * specifies to which column it belongs to. For this feature all columns are 
 * only referenced by their name. For that reason, columns must be labeled before
 * any method exposed by this feature is called. Failing to do so will result
 * in an exception being raised at runtime.
 * 
 * <p>Classes implementing this interface do not necessarily have to be plain 
 * model classes. They can have an arbitrary positive number of member fields, 
 * but not all of them have to be annotated as a row item. 
 * Those fields are exluded and ignored when passing an instance of a class to 
 * a DataFrame for an operation regarding rows. 
 * 
 * @author Phil Gaiser
 * @see RowItem
 * @since 2.0.0
 *
 */
public interface Row {

}
