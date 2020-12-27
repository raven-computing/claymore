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

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.raven.common.io.DataFrameSerializer;
import com.raven.common.io.SerializationException;

/**
 * An object that binds strongly typed columns into one data structure. All
 * columns have equal length but each column can be of a different type. A Column
 * can have a name associated with it so it can be accessed through that
 * name without knowledge of the index the column is located at. Methods provided
 * by this interface allow columns to be referenced both by index and name. Access
 * to any column-row value is guaranteed to be fulfilled in constant time.
 * 
 * <p>All implementations of this interface work with the {@link Column} type, which
 * represents any column of a particular DataFrame. In order to ensure type safety,
 * all concrete column types work with elements of one, and only one, concrete type.
 * It is not possible to mix elements of different types within one Column instance.
 * 
 * <p>Overall, ten different element types are supported by any
 * DataFrame implementation:<br>
 * <b>byte</b> (int8), <b>short</b> (int16), <b>int</b> (int32), <b>long</b> (int64),
 * <b>float</b> (float32), <b>double</b> (float64), <b>string</b>, <b>char</b>
 * (single ASCII-character), <b>boolean</b> and <b>binary</b>.
 * 
 * <p>A DataFrame manages <code>Column</code> instances and the underlying
 * data arrays. When adding or removing rows, a buffer in every column of a DataFrame
 * may be present, i.e. the low-level array inside a Column of a DataFrame may be
 * longer than the number of rows in the underlying DataFrame at that time. It is the
 * responsibility of each DataFrame implementation to correctly manage any unused
 * space. This interface provides methods to determine the used number of columns and
 * rows as well as the actual memory capacity used by all Columns. A DataFrame can
 * be flushed at any time which releases any unused memory from the Columns
 * it manages. Since Columns do not manage their capacity independently, care
 * must be taken when working with Column instances directly. Users are generally
 * advised to only manipulate Columns through methods provided by the
 * DataFrame interface. Moreover, because a DataFrame is a collection of references
 * to Columns, it is possible for a user to generate an inconsistent state
 * in DataFrames. For example, two DataFrames can have a reference to the same
 * Column instance to safe memory. However, if one DataFrame then removes rows
 * from the underlying Column, then the other DataFrame will not be aware of
 * that change. It is the responsibility of the user to explicitly copy (clone)
 * Columns whenever needed to avoid conflicts. 
 * 
 * <p>Two concrete <code>Column</code> implementations are provided for all core types.
 * On the one hand, a default implementation is provided which does not allow the
 * use of null values. Therefore, when using default columns, all entries are always
 * guaranteed to be set to a valid value of the corresponding type. On the other hand,
 * a second column implementation is provided, allowing the use of null values.<br>
 * Default column implementations are generally speaking more efficient than nullable
 * implementations both in terms of runtime and memory footprint.
 * 
 * <p>Just as there are two different <code>Column</code> implementations for all
 * supported element types, there are also two separate implementations of
 * this interface:<br>
 * {@link DefaultDataFrame} and {@link NullableDataFrame}.
 * 
 * <p>Both DataFrame implementations can only work with the corresponding appropriate
 * <code>Column</code> type. That is, a <code>DefaultDataFrame</code> must use default
 * (non-nullable) Column instances whereas a <code>NullableDataFrame</code> must use
 * nullable Column instances. All nullable Column instances
 * extend the {@link NullableColumn} class. 
 * 
 * <p>All implementing classes provide at least three constructors: a void
 * (no arguments) constructor which creates an empty DataFrame, a constructor which
 * takes multiple arguments of type <code>Column</code>, creating a DataFrame composed
 * of the given columns, and a constructor which takes a String array followed by
 * multiple arguments of type <code>Column</code>, which creates a DataFrame composed
 * of the given named columns.<br>
 * Column names can be created and changed at any time by calling
 * the appropriate methods.
 * 
 * <p>DataFrames can also be constructed by passing a class implementing
 * the {@link Row} interface to the constructor. That constructor will infer all
 * column types and names from the fields annotated with {@link RowItem} within
 * the provided class.<br>
 * Consequently rows can be also retrieved, set and inserted in a similar manner.
 * However, please note that this feature can only be used when dealing with
 * static DataFrames, i.e. the defined column structure does not change at runtime.
 * When using dynamic DataFrames with changing column structures, then the normal
 * way of working with rows is to use arrays of Objects. Be aware that type safety
 * is part of the specification and is always enforced
 * by all DataFrame implementations.
 * 
 * <p>This interface defines various methods for numerical computations. These
 * computations should whenever possible be directly executed with the corresponding
 * element type of the underlying Column. Methods may still return a <i>double</i>
 * value representing the final return value computed. Individual methods
 * should document whether numeric values returned by them can be safely cast
 * without losing information. Please note that numeric values smaller
 * than -{@link Double#MAX_VALUE} or larger than +{@link Double#MAX_VALUE} may not
 * be represented by operations defined in this interface. Numeric computations
 * may be subject to floating point errors.
 * 
 * <p>Any method defined by this interface can throw a {@link DataFrameException} at
 * runtime if any argument passed to it is invalid, for example an out of bounds
 * index, or if that operation would result in an incoherent/invalid state of
 * that DataFrame. Javadocs of specific methods do not have to mention this
 * explicitly. A <code>DataFrameException</code> is a <Code>RuntimeException</code>.
 * 
 * <p>This interface also provides various static utility methods
 * to work with DataFrames. This includes a standard API for serialization and
 * file I/O support for DataFrames. See {@link DataFrameSerializer} for
 * more options.
 * 
 * <p>A DataFrame is {@link Cloneable}, {@link Iterable}
 * 
 * @author Phil Gaiser
 * @see DefaultDataFrame
 * @see NullableDataFrame
 * @since 1.0.0
 *
 */
public interface DataFrame extends Cloneable, Iterable<Column> {

    /**
     * Gets the byte at the specified column and row index. If the
     * underlying DataFrame implementation doesn't support null values,
     * then the returned Byte object is guaranteed to be non-null
     * 
     * @param col The column index of the value to get
     * @param row The row index of the value to get
     * @return The byte value at the specified position
     */
    public Byte getByte(int col, int row);

    /**
     * Gets the byte from the specified column at the specified row index.
     * The column must be specified by name. If the underlying
     * DataFrame implementation doesn't support null values, then the
     * returned Byte object is guaranteed to be non-null
     * 
     * @param col The name of the column to get the value from
     * @param row The row index of the value to get
     * @return The byte value at the specified position
     */
    public Byte getByte(String col, final int row);

    /**
     * Gets the short at the specified column and row index. If the
     * underlying DataFrame implementation doesn't support null values,
     * then the returned Short object is guaranteed to be non-null
     * 
     * @param col The column index of the value to get
     * @param row The row index of the value to get
     * @return The short value at the specified position
     */
    public Short getShort(int col, int row);

    /**
     * Gets the short from the specified column at the specified row index.
     * The column must be specified by name. If the underlying
     * DataFrame implementation doesn't support null values, then the
     * returned Short object is guaranteed to be non-null
     * 
     * @param col The name of the column to get the value from
     * @param row The row index of the value to get
     * @return The short value at the specified position
     */
    public Short getShort(String col, int row);

    /**
     * Gets the int at the specified column and row index. If the
     * underlying DataFrame implementation doesn't support null values,
     * then the returned Integer object is guaranteed to be non-null
     * 
     * @param col The column index of the value to get
     * @param row The row index of the value to get
     * @return The int value at the specified position
     */
    public Integer getInt(int col, int row);

    /**
     * Gets the int from the specified column at the specified row index.
     * The column must be specified by name. If the underlying
     * DataFrame implementation doesn't support null values, then the
     * returned Integer object is guaranteed to be non-null
     * 
     * @param col The name of the column to get the value from
     * @param row The row index of the value to get
     * @return The int value at the specified position
     */
    public Integer getInt(String col, int row);

    /**
     * Gets the long at the specified column and row index. If the
     * underlying DataFrame implementation doesn't support null values,
     * then the returned Long object is guaranteed to be non-null
     * 
     * @param col The column index of the value to get
     * @param row The row index of the value to get
     * @return The long value at the specified position
     */
    public Long getLong(int col, int row);

    /**
     * Gets the long from the specified column at the specified row index.
     * The column must be specified by name. If the underlying
     * DataFrame implementation doesn't support null values, then the
     * returned Long object is guaranteed to be non-null
     * 
     * @param col The name of the column to get the value from
     * @param row The row index of the value to get
     * @return The long value at the specified position
     */
    public Long getLong(String col, int row);

    /**
     * Gets the string at the specified column and row index. If the
     * underlying DataFrame implementation doesn't support null values,
     * then the returned String object is guaranteed to be non-null
     * 
     * @param col The column index of the value to get
     * @param row The row index of the value to get
     * @return The string value at the specified position
     */
    public String getString(int col, int row);

    /**
     * Gets the string from the specified column at the specified row index.
     * The column must be specified by name. If the underlying
     * DataFrame implementation doesn't support null values, then the
     * returned String object is guaranteed to be non-null
     * 
     * @param col The name of the column to get the value from
     * @param row The row index of the value to get
     * @return The string value at the specified position
     */
    public String getString(String col, int row);

    /**
     * Gets the float at the specified column and row index. If the
     * underlying DataFrame implementation doesn't support null values,
     * then the returned Float object is guaranteed to be non-null
     * 
     * @param col The column index of the value to get
     * @param row The row index of the value to get
     * @return The float value at the specified position
     */
    public Float getFloat(int col, int row);

    /**
     * Gets the float from the specified column at the specified row index.
     * The column must be specified by name. If the underlying
     * DataFrame implementation doesn't support null values, then the
     * returned Float object is guaranteed to be non-null
     * 
     * @param col The name of the column to get the value from
     * @param row The row index of the value to get
     * @return The float value at the specified position
     */
    public Float getFloat(String col, int row);

    /**
     * Gets the double at the specified column and row index. If the
     * underlying DataFrame implementation doesn't support null values,
     * then the returned Double object is guaranteed to be non-null
     * 
     * @param col The column index of the value to get
     * @param row The row index of the value to get
     * @return The double value at the specified position
     */
    public Double getDouble(int col, int row);

    /**
     * Gets the double from the specified column at the specified row index.
     * The column must be specified by name. If the underlying
     * DataFrame implementation doesn't support null values,
     * then the returned Double object is guaranteed to be non-null
     * 
     * @param col The name of the column to get the value from
     * @param row The row index of the value to get
     * @return The double value at the specified position
     */
    public Double getDouble(String col, int row);

    /**
     * Gets the char at the specified column and row index. If the
     * underlying DataFrame implementation doesn't support null values,
     * then the returned Character object is guaranteed to be non-null
     * 
     * @param col The column index of the value to get
     * @param row The row index of the value to get
     * @return The char value at the specified position
     */
    public Character getChar(int col, int row);

    /**
     * Gets the char from the specified column at the specified row index.
     * The column must be specified by name. If the underlying
     * DataFrame implementation doesn't support null values,
     * then the returned Character object is guaranteed to be non-null
     * 
     * @param col The name of the column to get the value from
     * @param row The row index of the value to get
     * @return The char value at the specified position
     */
    public Character getChar(String col, int row);

    /**
     * Gets the boolean at the specified column and row index. If the
     * underlying DataFrame implementation doesn't support null values,
     * then the returned Boolean object is guaranteed to be non-null
     * 
     * @param col The column index of the value to get
     * @param row The row index of the value to get
     * @return The boolean value at the specified position
     */
    public Boolean getBoolean(int col, int row);

    /**
     * Gets the boolean from the specified column at the specified row index.
     * The column must be specified by name. If the underlying
     * DataFrame implementation doesn't support null values,
     * then the returned Boolean object is guaranteed to be non-null
     * 
     * @param col The name of the column to get the value from
     * @param row The row index of the value to get
     * @return The boolean value at the specified position
     */
    public Boolean getBoolean(String col, int row);

    /**
     * Gets the binary data at the specified column and row index. If the
     * underlying DataFrame implementation doesn't support null values,
     * then the returned byte array is guaranteed to be non-null
     * 
     * @param col The column index of the binary data to get
     * @param row The row index of the binary data to get
     * @return The binary data at the specified position
     */
    public byte[] getBinary(int col, int row);

    /**
     * Gets the binary data from the specified column at the specified row index.
     * The column must be specified by name. If the underlying
     * DataFrame implementation doesn't support null values,
     * then the returned byte array is guaranteed to be non-null
     * 
     * @param col The name of the column to get the binary data from
     * @param row The row index of the binary data to get
     * @return The binary data at the specified position
     */
    public byte[] getBinary(String col, int row);

    /**
     * Gets the number at the specified column and row index.
     * If the underlying DataFrame implementation doesn't support
     * null values, then the returned Number object is guaranteed to be non-null.
     * 
     * <p>Please note that this method is not part of the DataFrame API
     * and is provided here for convenience
     * 
     * @param col The column index of the number data to get
     * @param row The row index of the number data to get
     * @return The number at the specified position
     *         as a <code>Number</code> object
     */
    public Number getNumber(int col, int row);

    /**
     * Gets the number from the specified column at the specified row index.
     * The column must be specified by name. If the underlying
     * DataFrame implementation doesn't support null values, then the
     * returned Number object is guaranteed to be non-null.
     * 
     * <p>Please note that this method is not part of the DataFrame API
     * and is provided here for convenience
     * 
     * @param col The name of the column to get the number from
     * @param row The row index of the number to get
     * @return The number at the specified position
     *         as a <code>Number</code> object
     */
    public Number getNumber(String col, int row);

    /**
     * Sets the byte at the specified column and row index. If the
     * underlying DataFrame implementation doesn't support null values,
     * then the provided Byte object must not be null
     * 
     * @param col The column index of the value to set
     * @param row The row index of the value to set
     * @param value The byte value to set
     */
    public void setByte(int col, int row, Byte value);

    /**
     * Sets the byte at the specified column at the specified row index.
     * The column must be specified by name. If the underlying
     * DataFrame implementation doesn't support null values, then the
     * provided Byte object must not be null
     * 
     * @param col The name of the column to set the value at
     * @param row The row index of the value to set
     * @param value The byte value to set
     */
    public void setByte(String col, int row, Byte value);

    /**
     * Sets the short at the specified column and row index. If the
     * underlying DataFrame implementation doesn't support null values,
     * then the provided Short object must not be null
     * 
     * @param col The column index of the value to set
     * @param row The row index of the value to set
     * @param value The short value to set
     */
    public void setShort(int col, int row, Short value);

    /**
     * Sets the short at the specified column at the specified row index.
     * The column must be specified by name. If the underlying
     * DataFrame implementation doesn't support null values, then the
     * provided Short object must not be null
     * 
     * @param col The name of the column to set the value at
     * @param row The row index of the value to set
     * @param value The short value to set
     */
    public void setShort(String col, int row, Short value);

    /**
     * Sets the int at the specified column and row index. If the
     * underlying DataFrame implementation doesn't support null values,
     * then the provided Integer object must not be null
     * 
     * @param col The column index of the value to set
     * @param row The row index of the value to set
     * @param value The int value to set
     */
    public void setInt(int col, int row, Integer value);

    /**
     * Sets the int at the specified column at the specified row index.
     * The column must be specified by name. If the underlying
     * DataFrame implementation doesn't support null values, then the
     * provided Integer object must not be null
     * 
     * @param col The name of the column to set the value at
     * @param row The row index of the value to set
     * @param value The int value to set
     */
    public void setInt(String col, int row, Integer value);

    /**
     * Sets the long at the specified column and row index. If the
     * underlying DataFrame implementation doesn't support null values,
     * then the provided Long object must not be null
     * 
     * @param col The column index of the value to set
     * @param row The row index of the value to set
     * @param value The long value to set
     */
    public void setLong(int col, int row, Long value);

    /**
     * Sets the long at the specified column at the specified row index.
     * The column must be specified by name. If the underlying
     * DataFrame implementation doesn't support null values, then the
     * provided Long object must not be null
     * 
     * @param col The name of the column to set the value at
     * @param row The row index of the value to set
     * @param value The long value to set
     */
    public void setLong(String col, int row, Long value);

    /**
     * Sets the string at the specified column and row index. If the
     * underlying DataFrame implementation doesn't support null values
     * or empty strings, then the provided String object will be
     * converted to "n/a" if it is null or empty
     * 
     * @param col The column index of the value to set
     * @param row The row index of the value to set
     * @param value The string value to set
     */
    public void setString(int col, int row, String value);

    /**
     * Sets the string at the specified column at the specified row index.
     * The column must be specified by name. If the underlying
     * DataFrame implementation doesn't support null values or empty strings,
     * then the provided String object will be converted
     * to "n/a" if it is null or empty
     * 
     * @param col The name of the column to set the value at
     * @param row The row index of the value to set
     * @param value The string value to set
     */
    public void setString(String col, int row, String value);

    /**
     * Sets the float at the specified column and row index. If the
     * underlying DataFrame implementation doesn't support null values,
     * then the provided Float object must not be null
     * 
     * @param col The column index of the value to set
     * @param row The row index of the value to set
     * @param value The float value to set
     */
    public void setFloat(int col, int row, Float value);

    /**
     * Sets the float at the specified column at the specified row index.
     * The column must be specified by name. If the underlying
     * DataFrame implementation doesn't support null values, then the
     * provided Float object must not be null
     * 
     * @param col The name of the column to set the value at
     * @param row The row index of the value to set
     * @param value The float value to set
     */
    public void setFloat(String col, int row, Float value);

    /**
     * Sets the double at the specified column and row index. If the
     * underlying DataFrame implementation doesn't support null values,
     * then the provided Double object must not be null
     * 
     * @param col The column index of the value to set
     * @param row The row index of the value to set
     * @param value The double value to set
     */
    public void setDouble(int col, int row, Double value);

    /**
     * Sets the double at the specified column at the specified row index.
     * The column must be specified by name. If the underlying
     * DataFrame implementation doesn't support null values, then the
     * provided Double object must not be null
     * 
     * @param col The name of the column to set the value at
     * @param row The row index of the value to set
     * @param value The double value to set
     */
    public void setDouble(String col, int row, Double value);

    /**
     * Sets the char at the specified column and row index. If the
     * underlying DataFrame implementation doesn't support null values,
     * then the provided Character object must not be null
     * 
     * @param col The column index of the value to set
     * @param row The row index of the value to set
     * @param value The char value to set
     */
    public void setChar(int col, int row, Character value);

    /**
     * Sets the char at the specified column at the specified row index.
     * The column must be specified by name. If the underlying
     * DataFrame implementation doesn't support null values, then the
     * provided Character object must not be null
     * 
     * @param col The name of the column to set the value at
     * @param row The row index of the value to set
     * @param value The char value to set
     */
    public void setChar(String col, int row, Character value);

    /**
     * Sets the boolean at the specified column and row index. If the
     * underlying DataFrame implementation doesn't support null values,
     * then the provided Boolean object must not be null
     * 
     * @param col The column index of the value to set
     * @param row The row index of the value to set
     * @param value The boolean value to set
     */
    public void setBoolean(int col, int row, Boolean value);

    /**
     * Sets the boolean at the specified column at the specified row index.
     * The column must be specified by name. If the underlying
     * DataFrame implementation doesn't support null values, then the
     * provided Boolean object must not be null
     * 
     * @param col The name of the column to set the value at
     * @param row The row index of the value to set
     * @param value The boolean value to set
     */
    public void setBoolean(String col, int row, Boolean value);

    /**
     * Sets the binary data at the specified column and row index.
     * If the underlying DataFrame implementation doesn't support null values,
     * then the provided byte array must not be null
     * 
     * @param col The column index of the binary data to set
     * @param row The row index of the binary data to set
     * @param value The binary data to set
     */
    public void setBinary(int col, int row, byte[] value);

    /**
     * Sets the binary data at the specified column at the specified row index.
     * The column must be specified by name. If the underlying
     * DataFrame implementation doesn't support null values, then the
     * provided byte array must not be null
     * 
     * @param col The name of the column to set the binary data at
     * @param row The row index of the binary data to set
     * @param value The binary data to set
     */
    public void setBinary(String col, int row, byte[] value);

    /**
     * Sets the number at the specified column and row index. If the underlying
     * DataFrame implementation doesn't support null values, then the provided
     * Number object must not be null
     * 
     * <p>Please note that this method is not part of the DataFrame API
     * specification and is provided here for convenience
     * 
     * @param col The column index of the number to set
     * @param row The row index of the number to set
     * @param value The number to set
     */
    public void setNumber(int col, int row, Number value);

    /**
     * Sets the number in the specified column at the specified row index.
     * The column must be specified by name. If the underlying
     * DataFrame implementation doesn't support null values, then the
     * provided Number object must not be null
     * 
     * <p>Please note that this method is not part of the DataFrame API
     * specification and is provided here for convenience
     * 
     * @param col The name of the column to set the number
     * @param row The row index of the number to set
     * @param value The number to set
     */
    public void setNumber(String col, int row, Number value);

    /**
     * Gets the name of all columns of this DataFrame in proper order.
     * Columns which have not been named will be represented
     * as their index within the DataFrame
     * 
     * @return The names of all columns, or null if no
     *         column names have been set
     */
    public String[] getColumnNames();

    /**
     * Gets the name of the column at the specified index
     * 
     * @param col The index of the column
     * @return The name of the column, or null if no name
     *         has been set for this column
     */
    public String getColumnName(int col);

    /**
     * Gets the index of the column with the specified name. 
     * <p>This method may throw a {@link DataFrameException} if no
     * column of this DataFrame has the specified name or if
     * no names have been set
     * 
     * @param col The name of the column to get the index for.
     *            Must not be null or empty
     * @return The index of the column with the specified name
     */
    public int getColumnIndex(String col);

    /**
     * Assigns all columns the specified names. Any already set name
     * will get overridden. The length of the provided array must
     * match the number of columns in this DataFrame.
     * 
     * <p>Please note that no checks will be performed regarding duplicates.
     * Assigning more than one column the same name will
     * result in undefined behaviour
     * 
     * @param names The names of the columns as a String array.
     *              Must not be null. Must not contain null
     *              or empty strings
     * @return This <code>DataFrame</code> instance
     */
    public DataFrame setColumnNames(String... names);

    /**
     * Assigns the specified column the specified name. No checks will
     * be performed regarding duplicates. Assigning more than one column the
     * same name will result in undefined behaviour. A return value is
     * being provided to indicate whether any previous name of the specified
     * column was overridden as a result of this operation
     * 
     * @param col The index of the column
     * @param name The name to assign to the column
     * @return True if any previous name was overridden, false otherwise
     */
    public boolean setColumnName(int col, String name);

    /**
     * Assigns the column with the specified name the given new name.
     * No checks will be performed regarding duplicates. Assigning more
     * than one column the same name will result in undefined behaviour
     * 
     * @param col The name of the column to rename.
     *            Must not be null or empty
     * @param name The new name to assign to the column.
     *             Must not be null or empty
     * @return This <code>DataFrame</code> instance
     */
    public DataFrame setColumnName(String col, String name);

    /**
     * Removes all column names from this DataFrame instance
     * 
     * @return This <code>DataFrame</code> instance
     */
    public DataFrame removeColumnNames();
    
    /**
     * Indicates whether this DataFrame has a column with
     * the specified name
     * 
     * @param col The name of the column. Must not be null or empty
     * @return True if this DataFrame has a column with the specified name,
     *         false if no column with the specified name exists
     *         in this DataFrame
     */
    public boolean hasColumn(String col);

    /**
     * Indicates whether this DataFrame has any column names set.
     * Please note that this method returns true even when column names
     * are only partially set, i.e. not all columns must have a name
     * assigned to them. In other words, this method returns true if at
     * least one column has a name assigned to it
     * 
     * @return True if this DataFrame has any column names set,
     *         false if no column names have been set
     */
    public boolean hasColumnNames();

    /**
     * Gets the row at the specified index. If the underlying DataFrame
     * implementation doesn't support null values, then the returned array is
     * guaranteed to consist of non-null values
     * 
     * @param index The index of the row to get
     * @return The row at the specified index as an array of Objects
     */
    public Object[] getRow(int index);

    /**
     * Gets the row at the specified index. This method returns a custom object
     * implementing the {@link Row} interface. All columns must be labeled by the
     * time this method is called. The class of the returned type must be provided
     * as an argument. Passing null to the class argument will result in
     * a <code>DataFrameException</code>.<br>
     * The class implementing <code>Row</code> must properly annotate all members
     * representing row items. If the class complies to the contract, a new instance
     * of the specified type will be created through the default no-args constructor
     * with all annotated fields being set to the value of the
     * corresponding row item.<br>
     * If the underlying DataFrame implementation doesn't support null values, then
     * all annotated members of the returned <code>Row</code> are guaranteed
     * to consist of non-null values
     * 
     * @param <T> The type of the desired row
     * @param index The index of the row to get
     * @param classOfT The class of the type <b>T</b>
     * @return The row at the specified index as the specified type <b>T</b>
     */
    public <T extends Row> T getRow(int index, Class<T> classOfT);

    /**
     * Gets the rows located in the specified range. If the underlying DataFrame
     * implementation doesn't support null values, then the returned DataFrame is
     * guaranteed to consist of non-null values. The returned DataFrame is of the
     * same type as the DateFrame this method is called upon.
     * 
     * <p>All rows in the returned DataFrame are copies of the original rows, so
     * changing values within the returned DataFrame has no effect on the original
     * DataFrame and vice versa. Please note that this does not apply to byte arrays
     * of BinaryColumns, in which case the references to the underlying arrays are
     * copied to the rows of the returned DataFrame
     * 
     * @param from The index from which to get all rows from (inclusive)
     * @param to The index to which to get all rows from (exclusive)
     * @return A DataFrame with the same column structure and all rows from
     *         the specified start index to the specified end index
     */
    public DataFrame getRows(int from, int to);

    /**
     * Sets and replaces the provided row within this DataFrame
     * at the specified index.
     * 
     * <p>The type of each element must be equal to the type of the column
     * it is placed in. If the underlying DataFrame implementation doesn't
     * support null values, then passing an array with null values will result
     * in a {@link DataFrameException}. The number of provided row items must
     * be equal to the number of columns in this DataFrame
     * 
     * @param index The index of the row
     * @param row The row items to set, represented as an array of Objects
     * @return This <code>DataFrame</code> instance
     */
    public DataFrame setRow(int index, Object... row);

    /**
     * Sets and replaces the provided row within this DataFrame
     * at the specified index.
     * 
     * <p>The provided row is a custom object implementing
     * the {@link Row} interface.<br>
     * All columns must be labeled by the time this method is called.<br>
     * The class implementing <code>Row</code> must properly annotate all
     * members representing row items.<br>
     * If the underlying DataFrame implementation doesn't support null values,
     * then passing a row with members that consist of null values
     * will result in a {@link DataFrameException}.
     * 
     * @param index The index of the row
     * @param row The row to set, represented as a custom object
     *            implementing the {@link Row} interface
     * @return This <code>DataFrame</code> instance
     */
    public DataFrame setRow(int index, Row row);

    /**
     * Adds the provided row to the end of this DataFrame.
     * 
     * <p>The type of each element must be equal to the type of the
     * column it is placed in. If the underlying DataFrame implementation
     * doesn't support null values, then passing an array with null values
     * will result in a {@link DataFrameException}. The number of provided
     * row items must be equal to the number of columns in this DataFrame
     * 
     * @param row The items of the row to add, represented as an array of Objects
     * @return This <code>DataFrame</code> instance
     */
    public DataFrame addRow(Object... row);

    /**
     * Adds the provided row to the end of this DataFrame.
     * 
     * <p>The provided row is a custom object implementing
     * the {@link Row} interface.<br>
     * All columns must be labeled by the time this method is called.<br>
     * The class implementing <code>Row</code> must properly annotate all
     * members representing row items.<br>
     * If the underlying DataFrame implementation doesn't support
     * null values, then passing a row with members that consist of null
     * values will result in a {@link DataFrameException}.
     * 
     * @param row The row to add represented as a custom object
     *            implementing the {@link Row} interface
     * @return This <code>DataFrame</code> instance
     */
    public DataFrame addRow(Row row);

    /**
     * Adds all rows from the specified DataFrame to this DataFrame. If the specified
     * DataFrame has labeled columns, then row items are matched according to the
     * respective column name. If the specified DataFrame has unlabeled columns, then
     * row items are matched according to the respective index of the column they
     * originate from. The type of all row items must be equal to the element type
     * of the corresponding column. If the underlying DataFrame implementation doesn't
     * support null values, then all rows must consist of non-null values.
     * 
     * <p>Excessive columns within the specified DataFrame instance are ignored
     * when adding rows, i.e. the column structure of this DataFrame is never
     * changed by this operation. Missing items in rows to be added are substituted
     * with either null values or default values of the corresponding column element
     * type if the underlying DataFrame implementation doesn't support null values
     * 
     * @param rows The <code>DataFrame</code> instance holding all rows to add.
     *             Must not be null
     * @return This <code>DataFrame</code> instance
     */
    public DataFrame addRows(DataFrame rows);

    /**
     * Inserts the provided row into this DataFrame at the specified index.
     * Shifts the row currently at that position and any subsequent rows
     * down (adds one to their indices).
     *  
     * <p>The type of each element must be equal to the type of the column it
     * is placed in. If the underlying DataFrame implementation doesn't support
     * null values, then passing an array with null values will result
     * in a {@link DataFrameException}. The number of provided row
     * items must be equal to the number of columns in this DataFrame
     * 
     * @param index The index at which the specified row is to be inserted
     * @param row The row items to be inserted
     * @return This <code>DataFrame</code> instance
     */
    public DataFrame insertRow(int index, Object... row);

    /**
     * Inserts the provided row into this DataFrame at the specified index.
     * Shifts the row currently at that position and any subsequent
     * rows down (adds one to their indices).
     * 
     * <p>The provided row is a custom object implementing the {@link Row}
     * interface.<br>
     * All columns must be labeled by the time this method is called.<br>
     * The class implementing <code>Row</code> must properly annotate
     * all members representing row items.<br>
     * If the underlying DataFrame implementation doesn't support null values,
     * then passing a row with members that consist of null values
     * will result in a {@link DataFrameException}.
     * 
     * @param index The index at which the specified row is to be inserted
     * @param row The row to be inserted
     * @return This <code>DataFrame</code> instance
     */
    public DataFrame insertRow(int index, Row row);

    /**
     * Removes the row at the specified index
     * 
     * @param index The index of the row to be removed
     * @return This <code>DataFrame</code> instance
     */
    public DataFrame removeRow(int index);

    /**
     * Removes all rows from (inclusive) the specified index
     * to (exclusive) the specified index
     * 
     * @param from The index from which all rows should be removed (inclusive)
     * @param to The index to which all rows should be removed (exclusive)
     * @return This <code>DataFrame</code> instance
     */
    public DataFrame removeRows(int from, int to);

    /**
     * Removes all rows that match the specified regular expression in
     * the column at the specified index
     * 
     * @param col The index of the column that the specified
     *            regex is matched against
     * @param regex The regular expression that row entries in
     *              the specified column must match. May be null or empty
     * @return The number of removed rows
     */
    public int removeRows(int col, String regex);

    /**
     * Removes all rows that match the specified regular expression in
     * the column with the specified name
     * 
     * @param col The name of the column that the specified regex
     *            is matched against
     * @param regex The regular expression that row entries in
     *              the specified column must match. May be null or empty
     * @return The number of removed rows
     */
    public int removeRows(String col, String regex);

    /**
     * Adds the provided Column to this DataFrame. If the specified Column is empty,
     * then it will be resized to match the number of rows within this DataFrame.
     * If this DataFrame implementation supports null values, then all missing
     * Column entries are initialized with null values. If this DataFrame
     * implementation does not support null values, then all missing Column entries
     * are initialized with the default value of the corresponding Column.
     * 
     * <p>If the underlying DataFrame implementation supports null values and has a
     * deviating size or the provided Column has a deviating size, then the DataFrame
     * will refill all missing entries with null values to match the largest Column.
     * If the underlying DataFrame implementation does not support null values, then
     * the size of the provided Column must match the size of the already existing
     * Columns or be empty.
     * 
     * <p>If the added Column was labeled during its construction, then it will be 
     * referenceable by its name.
     * 
     * @param col The <code>Column</code> to add to the DataFrame. Must not be null
     * @return This <code>DataFrame</code> instance
     */
    public DataFrame addColumn(Column col);

    /**
     * Adds the provided Column to this DataFrame and assigns it the
     * specified name. If the specified Column is empty, then it will be resized to
     * match the number of rows within this DataFrame. If this DataFrame implementation
     * supports null values, then all missing Column entries are initialized with
     * null values. If this DataFrame implementation does not support null values, then
     * all missing Column entries are initialized with the default value of the
     * corresponding Column.
     * 
     * <p>If the underlying DataFrame implementation supports null values and has a
     * deviating size or the provided Column has a deviating size, then the DataFrame
     * will refill all missing entries with null values to match the largest Column.
     * If the underlying DataFrame implementation does not support null values, then
     * the size of the provided Column must match the size of the already existing
     * Columns or be empty.
     * 
     * <p>If the added column was labeled during its construction, then the
     * label will get overridden by the specified name.
     * 
     * <p>Please note that no checks will be performed regarding duplicate
     * column names. Assigning more than one column the same name will result
     * in undefined behaviour
     * 
     * @param colName The name of the column to be added. Must not be null or empty
     * @param col The <code>Column</code> to be added. Must not be null
     * @return This <code>DataFrame</code> instance
     */
    public DataFrame addColumn(String colName, Column col);

    /**
     * Removes the Column at the specified index from this DataFrame
     * 
     * @param col The index of the <code>Column</code> to remove
     * @return The removed <code>Column</code> instance
     */
    public Column removeColumn(int col);

    /**
     * Removes the Column with the specified name from this DataFrame
     * 
     * @param col The name of the <code>Column</code> to remove.
     *            Must not be null or empty
     * @return The removed <code>Column</code> instance
     */
    public Column removeColumn(String col);

    /**
     * Removes the specified Column from this DataFrame if present. Please note that
     * if the specified column is not part of this DataFrame, then this
     * operation has no effect. This method matches columns by their memory reference
     * 
     * @param col The reference to the <code>Column</code> to remove
     * @return True if the specified <code>Column</code> instance was removed from
     *         this DataFrame, false otherwise
     */
    public boolean removeColumn(Column col);

    /**
     * Inserts the provided Column at the specified index to this DataFrame.
     * Shifts the column currently at that position and any subsequent columns
     * to the right (adds one to their indices).
     * 
     * <p>If the specified Column is empty, then it will be resized to match the
     * number of rows within this DataFrame. If this DataFrame implementation supports
     * null values, then all missing Column entries are initialized with null values.
     * If this DataFrame implementation does not support null values, then all
     * missing Column entries are initialized with the default value of
     * the corresponding Column.
     * 
     * <p>If the underlying DataFrame implementation supports null values and has a
     * deviating size or the provided Column has a deviating size, then the DataFrame
     * will refill all missing entries with null values to match the largest Column.
     * If the underlying DataFrame implementation does not support null values, then
     * the size of the provided Column must match the size of the already existing
     * Columns or be empty.
     * 
     * <p>If the inserted Column was labeled during its construction, then it will be 
     * referenceable by its name.
     * 
     * @param index The index at which the specified column is to be inserted
     * @param col The column to be inserted
     * @return This <code>DataFrame</code> instance
     */
    public DataFrame insertColumn(int index, Column col);

    /**
     * Inserts the provided Column at the specified index to this DataFrame and
     * assigns it the specified name. Shifts the column currently at that
     * position and any subsequent columns to the right (adds one to their indices).
     * 
     * <p>If the specified Column is empty, then it will be resized to match the
     * number of rows within this DataFrame. If this DataFrame implementation supports
     * null values, then all missing Column entries are initialized with null values.
     * If this DataFrame implementation does not support null values, then all
     * missing Column entries are initialized with the default value of
     * the corresponding Column.
     * 
     * <p>If the underlying DataFrame implementation supports null values and has a
     * deviating size or the provided Column has a deviating size, then the DataFrame
     * will refill all missing entries with null values to match the largest Column.
     * If the underlying DataFrame implementation does not support null values, then
     * the size of the provided Column must match the size of the already existing
     * Columns or be empty.
     * 
     * <p>If the inserted column was labeled during its construction, then the
     * label will get overridden by the specified name.
     * 
     * <p>Please note that no checks will be performed regarding duplicate
     * column names. Assigning more than one column the same name will
     * result in undefined behaviour
     * 
     * @param index The index at which the specified column is to be inserted
     * @param colName The name of the column to be inserted
     * @param col The column to be inserted
     * @return This <code>DataFrame</code> instance
     */
    public DataFrame insertColumn(int index, String colName, Column col);

    /**
     * Indicates whether the column at the specified index contains an element
     * that matches the specified regular expression 
     * 
     * @param col The index of the <code>Column</code> to search
     * @param regex The regular expression that an element in the
     *              specified <code>Column</code> must match.
     *              May be null
     * @return True if the <code>Column</code> at the specified index contains
     *         at least one element that matches the given regular expression
     */
    public boolean contains(int col, String regex);

    /**
     * Indicates whether the column with the specified name contains an element
     * that matches the specified regular expression 
     * 
     * @param col The name of the <code>Column</code> to search
     * @param regex The regular expression that an element in the
     *              specified <code>Column</code> must match.
     *              May be null
     * @return True if the <code>Column</code> with the specified name contains
     *         at least one element that matches the given regular expression
     */
    public boolean contains(String col, String regex);

    /**
     * Indicates the number of columns this DataFrame currently holds
     * 
     * @return The number of columns of this DataFrame
     */
    public int columns();

    /**
     * Indicates the capacity of each column within this DataFrame. The capacity
     * is the number of entries any given column can hold without the
     * need of resizing. Therefore this method is different
     * from <code>rows()</code> because <code>capacity()</code> also indicates
     * the allocated space of the underlying array of each {@link Column}.
     * 
     * @return The capacity of each column of this DataFrame
     */
    public int capacity();

    /**
     * Indicates the number of rows this DataFrame currently holds
     * 
     * @return The number of rows of this DataFrame
     */
    public int rows();

    /**
     * Indicates whether this DataFrame is empty, i.e. it has no rows
     * 
     * @return True if this DataFrame is empty, false otherwise
     */
    public boolean isEmpty();

    /**
     * Indicates whether this DataFrame supports null values
     * 
     * @return True if this DataFrame supports null values,
     *         false if it does not support null values
     * @see DefaultDataFrame
     * @see NullableDataFrame
     */
    public boolean isNullable();

    /**
     * Removes all rows from this DataFrame and frees up allocated space.
     * However, the column structure will not be changed by this operation
     */
    public void clear();

    /**
     * Changes the capacity of each column to match the actually needed space
     * by the entries currently in this DataFrame. Therefore, subsequently adding
     * rows will require further resizing.
     * 
     * <p>This method can be called when unnecessary space
     * allocation should get freed up
     */
    public void flush();

    /**
     * Gets a reference of the {@link Column} instance at the specified index.
     * Any changes to values in that column are reflected in
     * the DataFrame and vice versa
     * 
     * @param col The index of the Column to get
     * @return The Column at the specified index
     */
    public Column getColumn(int col);

    /**
     * Gets a reference of the {@link Column} instance with the specified name.
     * Any changes to values in that column are reflected in
     * the DataFrame and vice versa
     * 
     * @param col The name of the Column to get
     * @return The Column with the specified name
     */
    public Column getColumn(String col);

    /**
     * Returns a DataFrame consisting of all columns specified by
     * the given indices. Please note that the selected Column instances are only
     * passed to the returned DataFrame by reference. Therefore, changing values
     * within the returned DataFrame has the same effect on this DataFrame
     * and vice versa. However, adding or removing rows produces an inconsistency
     * between this and the returned DataFrame. If the row structure of either
     * DataFrame is to be manipulated, then ensure to copy the DataFrame returned
     * by this method.
     * 
     * <p>The order of columns in the returned DataFrame is equal to the order
     * of the specified indices
     * 
     * @param cols The indices of all <code>Column</code> instances to select.
     *        Must not be null
     * @return A <code>DataFrame</code> containing references to all columns
     *         denoted by the specified indices
     */
    public DataFrame getColumns(int... cols);

    /**
     * Returns a DataFrame consisting of all columns specified by
     * the given names. Please note that the selected Column instances are only
     * passed to the returned DataFrame by reference. Therefore, changing values
     * within the returned DataFrame has the same effect on this DataFrame
     * and vice versa. However, adding or removing rows produces an inconsistency
     * between this and the returned DataFrame. If the row structure of either
     * DataFrame is to be manipulated, then ensure to copy the DataFrame returned
     * by this method
     * 
     * <p>The order of columns in the returned DataFrame is equal to the order
     * of the specified names
     * 
     * @param cols The names of all <code>Column</code> instances to select.
     *             Must not be null or contain null or empty strings
     * @return A <code>DataFrame</code> containing references to all columns
     *         denoted by the specified names
     */
    public DataFrame getColumns(String... cols);

    /**
     * Returns a DataFrame consisting of all columns specified by the given element
     * types. Please note that the selected Column instances are only passed to
     * the returned DataFrame by reference. Therefore, changing values within the
     * returned DataFrame has the same effect on this DataFrame and vice versa.
     * However, adding or removing rows produces an inconsistency between this and
     * the returned DataFrame. If the row structure of either DataFrame is to be
     * manipulated, then ensure to copy the DataFrame returned by this method
     * 
     * <p>The order of columns in the returned DataFrame is equal to the order
     * of the specified types as encountered from lower indices to higher indices
     * 
     * @param cols The element types of all <code>Column</code> instances to select.
     *             Each element type must only be specified once. Must not null
     * @return A <code>DataFrame</code> containing references to all columns
     *         denoted by the specified element types
     */
    public DataFrame getColumns(Class<?>... cols);

    /**
     * Sets and replaces the {@link Column} at the specified index
     * with the provided Column.
     * 
     * <p>If the specified Column is empty, then it will be resized to match the
     * number of rows within this DataFrame. If this DataFrame implementation supports
     * null values, then all missing Column entries are initialized with null values.
     * If this DataFrame implementation does not support null values, then all
     * missing Column entries are initialized with the default value of
     * the corresponding Column.
     * 
     * <p>If the specified Column is not empty, then its size must match the size of
     * the already existing Columns.
     * 
     * <p>If the specified Column has a label associated with it, then that
     * label will be incorporated into the DataFrame. If the provided Column
     * is not labeled, then the label of the provided Column will be set to
     * the label of the replaced Column
     * 
     * @param index The index of the Column to set
     * @param col The Column to set at the specified index
     * @return This <code>DataFrame</code> instance
     */
    public DataFrame setColumn(int index, Column col);

    /**
     * Sets the {@link Column} with the specified name to be part of this DataFrame.
     * If a column with the specified name already exists in this DataFrame, it will
     * be replaced by the specified column. If no column with the specified name
     * exists in this DataFrame, then the specified column will be added to it. If the
     * specified name differs from the set label of the specified column, the provided
     * string will take precedence and the name of the column will be set
     * to the specified name.
     * 
     * <p>If the specified Column is empty, then it will be resized to match the
     * number of rows within this DataFrame. If this DataFrame implementation supports
     * null values, then all missing Column entries are initialized with null values.
     * If this DataFrame implementation does not support null values, then all
     * missing Column entries are initialized with the default value of
     * the corresponding Column.
     * 
     * <p>If the specified Column is not empty, then its size must match the size of
     * the already existing Columns.
     * 
     * @param colName The name of the Column to set. Must not be null or empty
     * @param col The Column to set within this DataFrame. Must not be null
     * @return This <code>DataFrame</code> instance
     */
    public DataFrame setColumn(String colName, Column col);

    /**
     * Converts the column at the specified index to a <code>Column</code> instance of
     * the specified type code. This method may throw a <code>DataFrameException</code>
     * if the specified column cannot be converted
     * 
     * @param col The index of the column to convert and replace
     * @param typeCode The type code of the <code>Column</code> to convert
     *                 the specified column to
     * @return This <code>DataFrame</code> instance
     */
    public DataFrame convert(int col, byte typeCode);

    /**
     * Converts the column with the specified name to a <code>Column</code> instance of
     * the specified type code. This method may throw a <code>DataFrameException</code>
     * if the specified column cannot be converted
     * 
     * @param col The name of the column to convert and replace
     * @param typeCode The type code of the <code>Column</code> to convert
     *                 the specified column to
     * @return This <code>DataFrame</code> instance
     */
    public DataFrame convert(String col, byte typeCode);

    /**
     * Computes and returns the row index of the first occurrence
     * that matches the specified regular expression in
     * the column at the specified index
     * 
     * @param col The index of the column to search 
     * @param regex The regular expression to search for. May be null
     * @return The index of the row which matches the given regular
     *         expression in the specified column.<br>
     *         <b>-1</b> if nothing in the column matches
     *         the given regular expression
     */
    public int indexOf(int col, String regex);

    /**
     * Computes and returns the row index of the first occurrence that matches
     * the specified regular expression in the column with the specified name
     * 
     * @param col The name of the Column to search
     * @param regex The regular expression to search for. May be null
     * @return The index of the row which matches the given regular
     *         expression in the specified column.<br>
     * 		   <b>-1</b> if nothing in the column matches
     *         the given regular expression
     */
    public int indexOf(String col, String regex);

    /**
     * Computes and returns the row index of the first occurrence that matches
     * the specified regular expression in the column at the specified index,
     * while starting to search from the given row index to the end of the DataFrame
     * 
     * @param col The index of the column to search 
     * @param startFrom The row index from which to start searching
     * @param regex The regular expression to search for. May be null
     * @return The index of the row which matches the given regular
     *         expression in the specified column.<br>
     * 		   <b>-1</b> if nothing in the column matches
     *         the given regular expression
     */
    public int indexOf(int col, int startFrom, String regex);

    /**
     * Computes and returns the row index of the first occurrence that matches
     * the specified regular expression in the column with the specified name,
     * while starting to search from the given row index to the end of the DataFrame
     * 
     * @param col The name of the Column to search
     * @param startFrom The row index from which to start searching
     * @param regex The regular expression to search for. May be null
     * @return The index of the row which matches the given regular expression
     *         in the specified column.<br>
     * 		   <b>-1</b> if nothing in the column matches
     *         the given regular expression
     */
    public int indexOf(String col, int startFrom, String regex);

    /**
     * Computes and returns the row indices of all occurrences that match
     * the specified regular expression in the column at the specified index
     * 
     * @param col The index of the column to search 
     * @param regex The regular expression to search for. May be null
     * @return An array containing all row indices in proper order of all
     *         occurrences that match the given regular expression.
     *         Returns an empty array if nothing in the column matches
     *         the given regular expression
     */
    public int[] indexOfAll(int col, String regex);

    /**
     * Computes and returns the row indices of all occurrences that match
     * the specified regular expression in the column with the specified name
     * 
     * @param col The name of the Column to search
     * @param regex The regular expression to search for. May be null
     * @return An array containing all row indices in proper order of
     *         all occurrences that match the given regular expression.
     *         Returns an empty array if nothing in the column matches
     *         the given regular expression
     */
    public int[] indexOfAll(String col, String regex);

    /**
     * Computes and returns a DataFrame containing all rows that
     * match the specified regular expression in the column at the specified index.
     * This DataFrame is not changed by this operation.
     * 
     * <p>All rows in the returned DataFrame are copies of the original
     * rows, so changing values within the returned DataFrame has no effect on
     * the original DataFrame and vice versa. Please note that this does not apply
     * to byte arrays of BinaryColumns, in which case the references to the
     * underlying arrays are copied to the rows of the returned DataFrame
     * 
     * @param col The index of the column to search
     * @param regex The regular expression to search for. May be null
     * @return A sub-DataFrame containing all rows that match
     *         the given regular expression in the specified column.<br>
     *         Returns an empty DataFrame if nothing in
     *         the column matches the given regular expression
     * @see DataFrame#include(int, String)
     */
    public DataFrame filter(int col, String regex);

    /**
     * Computes and returns a DataFrame containing all rows
     * that match the specified regular expression in the column with
     * the specified name. This DataFrame is not changed by this operation.
     * 
     * <p>All rows in the returned DataFrame are copies of the original
     * rows, so changing values within the returned DataFrame has no effect on
     * the original DataFrame and vice versa. Please note that this does not apply
     * to byte arrays of BinaryColumns, in which case the references to the
     * underlying arrays are copied to the rows of the returned DataFrame
     * 
     * @param col The name of the Column to search
     * @param regex The regular expression to search for. May be null
     * @return A sub-DataFrame containing all rows that match
     *         the given regular expression in the specified column.<br>
     *         Returns an empty DataFrame if nothing in
     *         the column matches the given regular expression
     * @see DataFrame#include(String, String)
     */
    public DataFrame filter(String col, String regex);

    /**
     * Retains all rows in this DataFrame that match the specified regular
     * expression in the column at the specified index
     * 
     * @param col The index of the column to search
     * @param regex The regular expression to search for. May be null
     * @return This <code>DataFrame</code> instance
     * @see DataFrame#filter(int, String)
     */
    public DataFrame include(int col, String regex);

    /**
     * Retains all rows in this DataFrame that match the specified regular
     * expression in the column with the specified name
     * 
     * @param col The name of the Column to search
     * @param regex The regular expression to search for. May be null
     * @return This <code>DataFrame</code> instance
     * @see DataFrame#filter(String, String)
     */
    public DataFrame include(String col, String regex);

    /**
     * Computes and returns a DataFrame containing all rows that
     * do not match the specified regular expression in the column at the
     * specified index. This DataFrame is not changed by this operation.
     * 
     * <p>All rows in the returned DataFrame are copies of the original
     * rows, so changing values within the returned DataFrame has no effect on
     * the original DataFrame and vice versa. Please note that this does not apply
     * to byte arrays of BinaryColumns, in which case the references to the
     * underlying arrays are copied to the rows of the returned DataFrame
     * 
     * @param col The index of the column to search
     * @param regex The regular expression to search for. May be null
     * @return A sub-DataFrame containing all rows that do not match
     *         the given regular expression in the specified column.<br>
     *         Returns an empty DataFrame if everything in
     *         the column matches the given regular expression
     * @see DataFrame#exclude(int, String)
     */
    public DataFrame drop(int col, String regex);

    /**
     * Computes and returns a DataFrame containing all rows that
     * do not match the specified regular expression in the column with the
     * specified name. This DataFrame is not changed by this operation.
     * 
     * <p>All rows in the returned DataFrame are copies of the original
     * rows, so changing values within the returned DataFrame has no effect on
     * the original DataFrame and vice versa. Please note that this does not apply
     * to byte arrays of BinaryColumns, in which case the references to the
     * underlying arrays are copied to the rows of the returned DataFrame
     * 
     * @param col The name of the Column to search
     * @param regex The regular expression to search for. May be null
     * @return A sub-DataFrame containing all rows that do not match
     *         the given regular expression in the specified column.<br>
     *         Returns an empty DataFrame if everything in
     *         the column matches the given regular expression
     * @see DataFrame#exclude(String, String)
     */
    public DataFrame drop(String col, String regex);

    /**
     * Removes all rows in this DataFrame that match the specified regular
     * expression in the column at the specified index
     * 
     * @param col The index of the column to search
     * @param regex The regular expression to search for. May be null
     * @return This <code>DataFrame</code> instance
     * @see DataFrame#drop(int, String)
     */
    public DataFrame exclude(int col, String regex);

    /**
     * Removes all rows in this DataFrame that match the specified regular
     * expression in the column with the specified name
     * 
     * @param col The name of the Column to search
     * @param regex The regular expression to search for. May be null
     * @return This <code>DataFrame</code> instance
     * @see DataFrame#drop(String, String)
     */
    public DataFrame exclude(String col, String regex);

    /**
     * Replaces all values in the column at the specified index that match
     * the specified regular expression. All matched values are replaced with
     * the specified value. If the underlying DataFrame implementation doesn't
     * support null values, then the replacement value must not be null.
     * 
     * <p>Passing either null or an empty string as the regex argument to
     * this method is equivalent to matching null values
     * 
     * @param col The index of the column to replace values in
     * @param regex The regular expression that all column values
     *              to be replaced must match. May be null or empty
     * @param value The value to replace all matches with
     * @return The number of values that were replaced by this operation
     */
    public int replace(int col, String regex, Object value);

    /**
     * Replaces all values in specified column that match the specified
     * regular expression. All matched values are replaced with the
     * specified value. If the underlying DataFrame implementation doesn't
     * support null values, then the replacement value must not be null.
     * 
     * <p>Passing either null or an empty string as the regex argument to
     * this method is equivalent to matching null values
     * 
     * @param col The name of the column to replace values in
     * @param regex The regular expression that all column values
     *              to be replaced must match. May be null or empty
     * @param value The value to replace all matches with
     * @return The number of values that were replaced by this operation
     */
    public int replace(String col, String regex, Object value);

    /**
     * Replaces all values in the column at the specified index with the
     * value returned by the specified {@link ValueReplacement} functional
     * interface. If the underlying DataFrame implementation doesn't support
     * null values, then the replacement value returned by the specified
     * functional interface must not be null.
     * 
     * @param <T> The type used by the underlying column
     * @param col The index of the column to replace values in
     * @param value The <code>ValueReplacement</code> functional interface
     *              to determine the new value for each position. Passing null
     *              as a replacement argument will result in no change being applied
     * @return The number of values that were replaced by this operation
     */
    public <T> int replace(int col, ValueReplacement<T> value);

    /**
     * Replaces all values in the specified column with the value returned
     * by the specified {@link ValueReplacement} functional interface. If the
     * underlying DataFrame implementation doesn't support null values, then
     * the replacement value returned by the specified functional interface
     * must not be null.
     * 
     * @param <T> The type used by the underlying column
     * @param col The name of the column to replace values in
     * @param value The <code>ValueReplacement</code> functional interface
     *              to determine the new value for each position. Passing null
     *              as a replacement argument will result in no change being applied
     * @return The number of values that were replaced by this operation
     */
    public <T> int replace(String col, ValueReplacement<T> value);

    /**
     * Replaces all values in the column at the specified index with the
     * value returned by the specified {@link IndexedValueReplacement} functional
     * interface. If the underlying DataFrame implementation doesn't support
     * null values, then the replacement value returned by the specified
     * functional interface must not be null.
     * 
     * @param <T> The type used by the underlying column
     * @param col The index of the column to replace values in
     * @param value The <code>IndexedValueReplacement</code> functional interface
     *              to determine the new value for each position. Passing null
     *              as a replacement argument will result in no change being applied
     * @return The number of values that were replaced by this operation
     */
    public <T> int replace(int col, IndexedValueReplacement<T> value);

    /**
     * Replaces all values in the specified column with the value returned
     * by the specified {@link IndexedValueReplacement} functional interface.
     * If the underlying DataFrame implementation doesn't support null values,
     * then the replacement value returned by the specified functional interface
     * must not be null.
     * 
     * @param <T> The type used by the underlying column
     * @param col The name of the column to replace values in
     * @param value The <code>IndexedValueReplacement</code> functional interface
     *              to determine the new value for each position. Passing null
     *              as a replacement argument will result in no change being applied
     * @return The number of values that were replaced by this operation
     */
    public <T> int replace(String col, IndexedValueReplacement<T> value);

    /**
     * Replaces all values in the column at the specified index that match
     * the specified regular expression. All matched values are replaced
     * with the value returned by the specified {@link ValueReplacement}
     * functional interface. If the underlying DataFrame implementation
     * doesn't support null values, then the replacement value returned
     * by the specified functional interface must not be null.
     * 
     * <p>Passing either null or an empty string as the regex argument to
     * this method is equivalent to matching null values
     * 
     * @param <T> The type used by the underlying column
     * @param col The index of the column to replace values in
     * @param regex The regular expression that all column values
     *              to be replaced must match. May be null or empty
     * @param value The <code>ValueReplacement</code> functional interface
     *              to determine the new value for each matched position.
     *              Passing null as a replacement argument will result in
     *              no change being applied
     * @return The number of values that were replaced by this operation
     */
    public <T> int replace(int col, String regex, ValueReplacement<T> value);

    /**
     * Replaces all values in the specified column that match the specified
     * regular expression. All matched values are replaced with the value
     * returned by the specified {@link ValueReplacement} functional interface.
     * If the underlying DataFrame implementation doesn't support null values,
     * then the replacement value returned by the specified functional
     * interface must not be null.
     * 
     * <p>Passing either null or an empty string as the regex argument to
     * this method is equivalent to matching null values
     * 
     * @param <T> The type used by the underlying column
     * @param col The name of the column to replace values in
     * @param regex The regular expression that all column values
     *              to be replaced must match. May be null or empty
     * @param value The <code>ValueReplacement</code> functional interface
     *              to determine the new value for each matched position.
     *              Passing null as a replacement argument will result in
     *              no change being applied
     * @return The number of values that were replaced by this operation
     */
    public <T> int replace(String col, String regex, ValueReplacement<T> value);

    /**
     * Replaces all values in the column at the specified index that match
     * the specified regular expression. All matched values are replaced
     * with the value returned by the specified {@link IndexedValueReplacement}
     * functional interface. If the underlying DataFrame implementation
     * doesn't support null values, then the replacement value returned
     * by the specified functional interface must not be null.
     * 
     * <p>Passing either null or an empty string as the regex argument to
     * this method is equivalent to matching null values
     * 
     * @param <T> The type used by the underlying column
     * @param col The index of the column to replace values in
     * @param regex The regular expression that all column values
     *              to be replaced must match. May be null or empty
     * @param value The <code>IndexedValueReplacement</code> functional interface
     *              to determine the new value for each matched position.
     *              Passing null as a replacement argument will result in
     *              no change being applied
     * @return The number of values that were replaced by this operation
     */
    public <T> int replace(int col, String regex, IndexedValueReplacement<T> value);

    /**
     * Replaces all values in the specified column that match the specified
     * regular expression. All matched values are replaced with the value
     * returned by the specified {@link IndexedValueReplacement} functional interface.
     * If the underlying DataFrame implementation doesn't support null values,
     * then the replacement value returned by the specified functional
     * interface must not be null.
     * 
     * <p>Passing either null or an empty string as the regex argument to
     * this method is equivalent to matching null values
     * 
     * @param <T> The type used by the underlying column
     * @param col The name of the column to replace values in
     * @param regex The regular expression that all column values
     *              to be replaced must match. May be null or empty
     * @param value The <code>IndexedValueReplacement</code> functional interface
     *              to determine the new value for each matched position.
     *              Passing null as a replacement argument will result in
     *              no change being applied
     * @return The number of values that were replaced by this operation
     */
    public <T> int replace(String col, String regex, IndexedValueReplacement<T> value);

    /**
     * Replaces all columns in this DataFrame with matched columns from the
     * specified DataFrame. If both DataFrame instances have labeled columns,
     * then matching is performed via column names. If both DataFrame instances
     * are not labeled, then all columns are set from lower indices to higher
     * indices, i.e. left to right, from the specified DataFrame.<br>
     * Please note that DataFrames must be both either labeled or unlabeled.<br>
     * Both DataFrames must have the same number of rows
     * 
     * @param df The <code>DataFrame</code> instance holding all columns that should
     *           replace the corresponding columns in this instance. It must have the
     *           same number of rows as this DataFrame. Passing null as a replacement
     *           DataFrame will result in no change being applied
     * @return The number of <code>Column</code> instances that were replaced
     *         by this operation
     */
    public int replace(DataFrame df);

    /**
     * Changes the categorical data in the column at the specified index
     * into factors.<br>
     * The produced factors are unordered. The specified column is converted to
     * an {@link IntColumn} or {@link NullableIntColumn} respectively. The conducted
     * change to factors is reflected by the returned map, which maps every category
     * to the factor in the specified column.<br>
     * If the specified column is already numeric, then no change is applied
     * to this DataFrame and an empty map is returned. If the underlying DataFrame
     * implementation supports null values, then null values are excluded
     * from this operation
     * 
     * @param col The index of the column to change categories into factors
     * @return A <code>Map</code> holding the mapping from the encountered categories
     *         to the produced factors
     */
    public Map<Object, Integer> factor(int col);

    /**
     * Changes the categorical data in the column with the specified name
     * into factors.<br>
     * The produced factors are unordered. The specified column is converted to
     * an {@link IntColumn} or {@link NullableIntColumn} respectively. The conducted
     * change to factors is reflected by the returned map, which maps every category
     * to the factor in the specified column.<br>
     * If the specified column is already numeric, then no change is applied
     * to this DataFrame and an empty map is returned. If the underlying DataFrame
     * implementation supports null values, then null values are excluded
     * from this operation
     * 
     * @param col The name of the column to change categories into factors
     * @return A <code>Map</code> holding the mapping from the encountered categories
     *         to the produced factors
     */
    public Map<Object, Integer> factor(String col);

    /**
     * Counts the number of occurrences of all unique values in the column at
     * the specified index. Every unique value is described by a row
     * in the returned DataFrame. It has three columns: The column at index 0 is
     * of the same type as the specified column and it contains all unique values
     * encountered in the column at the specified index, with the
     * same name if set in this DataFrame. The column at index 1 ("count") contains
     * the quantity of the corresponding value as an int. The column at index 2 ("%")
     * contains the quantity of each value relative to the total number of
     * rows in this DataFrame as a float.
     * 
     * <p>If the underlying DataFrame implementation supports null values,
     * then the occurrence of null values is included as the last row in
     * the returned DataFrame
     * 
     * @param col The index of the column to count values for
     * @return A <code>DataFrame</code> describing the count of every unique
     *         value in this DataFrame. It is of the same type as this DataFrame
     */
    public DataFrame count(int col);

    /**
     * Counts the number of occurrences of all unique values in the column with
     * the specified name. Every unique value is described by a row
     * in the returned DataFrame. It has three columns: The column at index 0 is
     * of the same type as the specified column and it contains all unique values
     * encountered in the column with the specified name, with the
     * same name if set in this DataFrame. The column at index 1 ("count") contains
     * the quantity of the corresponding value as an int. The column at index 2 ("%")
     * contains the quantity of each value relative to the total number of
     * rows in this DataFrame as a float.
     * 
     * <p>If the underlying DataFrame implementation supports null values,
     * then the occurrence of null values is included as the last row in
     * the returned DataFrame
     * 
     * @param col The name of the column to count values for
     * @return A <code>DataFrame</code> describing the count of every unique
     *         value in this DataFrame. It is of the same type as this DataFrame
     */
    public DataFrame count(String col);

    /**
     * Counts the number of occurrences in the column at the specified index
     * that match the specified regular expression
     * 
     * @param col The index of the column to search
     * @param regex The regular expression to count matches for. May be null
     * @return The number of entries that match then given regular
     *         expression in the specified column
     */
    public int count(int col, String regex);

    /**
     * Counts the number of occurrences in the specified column that match
     * the specified regular expression
     * 
     * @param col The name of the column to search
     * @param regex The regular expression to count matches for. May be null
     * @return The number of entries that match then given regular
     *         expression in the specified column
     */
    public int count(String col, String regex);

    /**
     * Counts the number of unique elements in the column with the specified
     * index. Please note that if the underlying DataFrame implementation
     * supports null values, then the occurrence of null values is excluded
     * in the computed number
     * 
     * @param col The index of the column to count the number
     *            of unique elements for
     * @return The number of unique non-null elements in the column
     *         at the specified index
     */
    public int countUnique(int col);

    /**
     * Counts the number of unique elements in the column with the specified
     * name. Please note that if the underlying DataFrame implementation
     * supports null values, then the occurrence of null values is excluded
     * in the computed number
     * 
     * @param col The name of the column to count the number
     *            of unique elements for 
     * @return The number of unique non-null elements in the column
     *         with the specified name
     */
    public int countUnique(String col);

    /**
     * Returns the set of unique elements in the column with the specified index.
     * Please note that if the underlying DataFrame implementation
     * supports null values, then null values are not included
     * in the computed set.<br>
     * The returned set contains elements whose types are equal to the
     * types in the underlying column
     * 
     * @param <T> The element type used by the underlying column
     * @param col The index of the column to return all unique elements for 
     * @return A <code>Set</code> which contains all unique elements in
     *         the <code>Column</code> at the specified index
     */
    public <T> Set<T> unique(int col);

    /**
     * Returns the set of unique elements in the column with the specified name.
     * Please note that if the underlying DataFrame implementation
     * supports null values, then null values are not included
     * in the computed set.<br>
     * The returned set contains elements whose types are equal to the
     * types in the underlying column
     * 
     * @param <T> The element type used by the underlying column
     * @param col The name of the column to return all unique elements for 
     * @return A <code>Set</code> which contains all unique elements in
     *         the <code>Column</code> with the specified name
     */
    public <T> Set<T> unique(String col);

    /**
     * Computes the set-theoretic difference of this DataFrame and the specified
     * DataFrame instance. The difference is created with respect to all
     * encountered columns. This operation therefore returns a DataFrame with
     * all columns that are part of either this DataFrame or the specified
     * DataFrame instance but not both. Columns are only matched by their name.
     * Therefore, all columns must be labeled at the time this method is called.
     * The specified DataFrame must have the same type and number of
     * rows as this DataFrame.
     * 
     * <p>All Column instances included in the returned DataFrame are only added by
     * reference. Therefore, changing values within the returned DataFrame has the
     * same effect on the respective DataFrame and vice versa. However, adding or
     * removing rows produces an inconsistency between this and
     * the returned DataFrame. If the row structure of any DataFrame involved in
     * this operation is to be manipulated, then ensure to copy the
     * DataFrame returned by this method
     * 
     * @param df The <code>DataFrame</code> instance to be used in the
     *           difference operation
     * @return A <code>DataFrame</code> holding references to all columns that are
     *         either in this DataFrame or the specified DataFrame but not in both
     */
    public DataFrame differenceColumns(DataFrame df);

    /**
     * Computes the set-theoretic union of this DataFrame and the specified
     * DataFrame instance. The union is created with respect to all
     * encountered columns. This operation therefore returns a DataFrame with all
     * columns from both this DataFrame and the specified DataFrame instance, ignoring
     * duplicates. In the case of duplicates, the returned DataFrame only holds the
     * column references from this DataFrame. Columns are only matched by their name.
     * Therefore, all columns must be labeled at the time this method is called. The
     * specified DataFrame must have the same type and number of rows as this DataFrame.
     * 
     * <p>All Column instances included in the returned DataFrame are only added by
     * reference. Therefore, changing values within the returned DataFrame has the
     * same effect on the respective DataFrame and vice versa. However, adding or
     * removing rows produces an inconsistency between this and the returned DataFrame.
     * If the row structure of any DataFrame involved in this operation is to be
     * manipulated, then ensure to copy the DataFrame returned by this method
     * 
     * @param df The <code>DataFrame</code> instance to be used in the
     *           union operation
     * @return A <code>DataFrame</code> holding references to all columns from both
     *         this DataFrame and the specified DataFrame instance,
     *         ignoring duplicates
     */
    public DataFrame unionColumns(DataFrame df);

    /**
     * Computes the set-theoretic intersection of this DataFrame and the specified
     * DataFrame instance. The intersection is created with respect to all
     * encountered columns. This operation therefore returns a DataFrame with all
     * columns from this DataFrame that are also in the specified DataFrame instance.
     * The returned DataFrame only holds the column references from this DataFrame.
     * Columns are only matched by their name. Therefore, all columns must be labeled
     * at the time this method is called. The specified DataFrame must have the same
     * type and number of rows as this DataFrame.
     * 
     * <p>All Column instances included in the returned DataFrame are only added by
     * reference. Therefore, changing values within the returned DataFrame has the
     * same effect on the respective DataFrame and vice versa. However, adding or
     * removing rows produces an inconsistency between this and
     * the returned DataFrame. If the row structure of any DataFrame involved
     * in this operation is to be manipulated, then ensure to copy the
     * DataFrame returned by this method
     * 
     * @param df The <code>DataFrame</code> instance to be used in the
     *           intersection operation
     * @return A <code>DataFrame</code> holding references to all columns from
     *         this DataFrame also present in the specified DataFrame instance
     */
    public DataFrame intersectionColumns(DataFrame df);

    /**
     * Computes the set-theoretic difference of this DataFrame and the specified
     * DataFrame instance. The difference is created with respect to all
     * encountered rows. This operation therefore returns a DataFrame with all rows
     * that are part of either this DataFrame or the specified DataFrame instance
     * but not both. Both DataFrame instances must have either labeled or unlabeled
     * columns. The specified DataFrame must have the same column structure and
     * order as this DataFrame, however, it may be of any type.
     * 
     * <p>All rows included in the returned DataFrame are copies of the original values
     * with the exception of values from binary columns which are passed by reference.
     * 
     * @param df The <code>DataFrame</code> instance to be used in the
     *           difference operation
     * @return A <code>DataFrame</code> holding all rows that are either in this
     *         DataFrame or the specified DataFrame but not in both
     */
    public DataFrame differenceRows(DataFrame df);

    /**
     * Computes the set-theoretic union of this DataFrame and the specified
     * DataFrame instance. The union is created with respect to all
     * encountered rows. This operation therefore returns a DataFrame with all
     * rows from both this DataFrame and the specified DataFrame instance, ignoring
     * duplicates. Both DataFrame instances must have either labeled or unlabeled
     * columns. The specified DataFrame must have the same column structure and
     * order as this DataFrame, however, it may be of any type.
     * 
     * <p>All rows included in the returned DataFrame are copies of the original values
     * with the exception of values from binary columns which are passed by reference.
     * 
     * @param df The <code>DataFrame</code> instance to be used in the
     *           union operation
     * @return A <code>DataFrame</code> holding all rows from both this DataFrame and
     *         the specified DataFrame instance, ignoring duplicates
     */
    public DataFrame unionRows(DataFrame df);

    /**
     * Computes the set-theoretic intersection of this DataFrame and the specified
     * DataFrame instance. The intersection is created with respect to all
     * encountered rows. This operation therefore returns a DataFrame with all rows
     * from this DataFrame that are also in the specified DataFrame instance. Both
     * DataFrame instances must have either labeled or unlabeled columns. The
     * specified DataFrame must have the same column structure and order
     * as this DataFrame, however, it may be of any type.
     * 
     * <p>All rows included in the returned DataFrame are copies of the original values
     * with the exception of values from binary columns which are passed by reference.
     * 
     * @param df The <code>DataFrame</code> instance to be used in the
     *           intersection operation
     * @return A <code>DataFrame</code> holding all rows from this DataFrame that are
     *         also in the specified DataFrame instance
     */
    public DataFrame intersectionRows(DataFrame df);

    /**
     * Groups minimum values in all numeric columns by the unique values in the
     * column at the specified index. Every unique value in the specified column is
     * represented by a row in the returned DataFrame. The summary column is
     * located at index 0. All subsequent columns hold the minimum values for the
     * corresponding unique entry in the specified column.
     * 
     * <p>All columns must be labeled at the time this method is called. All columns in
     * the returned DataFrame have the same type and name as their correspondent
     * 
     * @param col The index of the <code>Column</code> to group minimum values for
     * @return A <code>DataFrame</code> holding all minimum values in all numeric
     *         Columns for each unique value in the Column at the specified index
     */
    public DataFrame groupMinimumBy(int col);

    /**
     * Groups minimum values in all numeric columns by the unique values in the
     * column with the specified name. Every unique value in the specified column is
     * represented by a row in the returned DataFrame. The summary column is
     * located at index 0. All subsequent columns hold the minimum values for the
     * corresponding unique entry in the specified column.
     * 
     * <p>All columns must be labeled at the time this method is called. All columns in
     * the returned DataFrame have the same type and name as their correspondent
     * 
     * @param col The name of the <code>Column</code> to group minimum values for
     * @return A <code>DataFrame</code> holding all minimum values in all numeric
     *         Columns for each unique value in the Column with the specified name
     */
    public DataFrame groupMinimumBy(String col);

    /**
     * Groups maximum values in all numeric columns by the unique values in the
     * column at the specified index. Every unique value in the specified column is
     * represented by a row in the returned DataFrame. The summary column is
     * located at index 0. All subsequent columns hold the maximum values for the
     * corresponding unique entry in the specified column.
     * 
     * <p>All columns must be labeled at the time this method is called. All columns in
     * the returned DataFrame have the same type and name as their correspondent
     * 
     * @param col The index of the <code>Column</code> to group maximum values for
     * @return A <code>DataFrame</code> holding all maximum values in all numeric
     *         Columns for each unique value in the Column at the specified index
     */
    public DataFrame groupMaximumBy(int col);

    /**
     * Groups maximum values in all numeric columns by the unique values in the
     * column with the specified name. Every unique value in the specified column is
     * represented by a row in the returned DataFrame. The summary column is
     * located at index 0. All subsequent columns hold the maximum values for the
     * corresponding unique entry in the specified column.
     * 
     * <p>All columns must be labeled at the time this method is called. All columns in
     * the returned DataFrame have the same type and name as their correspondent
     * 
     * @param col The name of the <code>Column</code> to group maximum values for
     * @return A <code>DataFrame</code> holding all maximum values in all numeric
     *         Columns for each unique value in the Column with the specified name
     */
    public DataFrame groupMaximumBy(String col);

    /**
     * Groups average values in all numeric columns by the unique values in the
     * column at the specified index. Every unique value in the specified column is
     * represented by a row in the returned DataFrame. The summary column is
     * located at index 0. All subsequent columns hold the average values for the
     * corresponding unique entry in the specified column.
     * 
     * <p>All columns must be labeled at the time this method is called. All columns in
     * the returned DataFrame have the same name as their correspondent. All numeric
     * columns are represented by <code>DoubleColumns</code> or
     * <code>NullableDoubleColumns</code> depending on the type of this DataFrame
     * 
     * @param col The index of the <code>Column</code> to group average values for
     * @return A <code>DataFrame</code> holding all average values in all numeric
     *         Columns for each unique value in the Column at the specified index
     */
    public DataFrame groupAverageBy(int col);

    /**
     * Groups average values in all numeric columns by the unique values in the
     * column with the specified name. Every unique value in the specified column is
     * represented by a row in the returned DataFrame. The summary column is
     * located at index 0. All subsequent columns hold the average values for the
     * corresponding unique entry in the specified column.
     * 
     * <p>All columns must be labeled at the time this method is called. All columns in
     * the returned DataFrame have the same name as their correspondent. All numeric
     * columns are represented by <code>DoubleColumns</code> or
     * <code>NullableDoubleColumns</code> depending on the type of this DataFrame
     * 
     * @param col The name of the <code>Column</code> to group average values for
     * @return A <code>DataFrame</code> holding all average values in all numeric
     *         Columns for each unique value in the Column with the specified name
     */
    public DataFrame groupAverageBy(String col);

    /**
     * Groups sum values in all numeric columns by the unique values in the
     * column at the specified index. Every unique value in the specified column is
     * represented by a row in the returned DataFrame. The summary column is
     * located at index 0. All subsequent columns hold the sums for the
     * corresponding unique entry in the specified column.
     * 
     * <p>All columns must be labeled at the time this method is called. All columns in
     * the returned DataFrame have the same name as their correspondent. All numeric
     * columns are represented by <code>DoubleColumns</code> or
     * <code>NullableDoubleColumns</code> depending on the type of this DataFrame
     * 
     * @param col The index of the <code>Column</code> to group sum values for
     * @return A <code>DataFrame</code> holding all sum values in all numeric
     *         Columns for each unique value in the Column at the specified index
     */
    public DataFrame groupSumBy(int col);

    /**
     * Groups sum values in all numeric columns by the unique values in the
     * column with the specified name. Every unique value in the specified column is
     * represented by a row in the returned DataFrame. The summary column is
     * located at index 0. All subsequent columns hold the sums for the
     * corresponding unique entry in the specified column.
     * 
     * <p>All columns must be labeled at the time this method is called. All columns in
     * the returned DataFrame have the same name as their correspondent. All numeric
     * columns are represented by <code>DoubleColumns</code> or
     * <code>NullableDoubleColumns</code> depending on the type of this DataFrame
     * 
     * @param col The name of the <code>Column</code> to group sum values for
     * @return A <code>DataFrame</code> holding all sum values in all numeric
     *         Columns for each unique value in the Column with the specified name
     */
    public DataFrame groupSumBy(String col);

    /**
     * Combines all rows from this and the specified DataFrame which have matching
     * values in their common column. Both DataFrames must have exactly one column
     * with an identical name and element type. All columns in both DataFrame
     * instances must be labeled by the time this method is called. The specified
     * DataFrame may be of any type
     * 
     * @param df The <code>DataFrame</code> to join with. Must not be null
     * @return A <code>DataFrame</code> with joined rows from both this and
     *         the specified DataFrame that have matching values in a column
     *         common to both DataFrame instances
     * @see DataFrame#join(DataFrame, String)
     */
    public DataFrame join(DataFrame df);

    /**
     * Combines all rows from this and the specified DataFrame which have matching
     * values in their columns with the mutual specified name. Both DataFrames must
     * have a column with the specified name and an identical element type. All
     * columns in both DataFrame instances must be labeled by the time this method
     * is called. The specified DataFrame may be of any type.
     * 
     * <p>All columns in the DataFrame argument that are also existent in
     * this DataFrame are excluded in the result DataFrame returned by this method.
     * Therefore, in the case of duplicate columns, the returned DataFrame only
     * contains the corresponding column from this DataFrame
     * 
     * @param df The <code>DataFrame</code> to join with. Must not be null
     * @param col The name of the <code>Column</code>s to match values
     *            for both DataFrames. Must not be null or empty
     * @return A <code>DataFrame</code> with joined rows from both this and
     *         the specified DataFrame that have matching values in a column
     *         with the specified name
     * @see DataFrame#join(DataFrame, String, String)
     */
    public DataFrame join(DataFrame df, String col);

    /**
     * Combines all rows from this and the specified DataFrame which have matching
     * values in their columns with the corresponding specified name. Both
     * DataFrames must have a column with the corresponding specified name
     * and an identical element type. All columns in both DataFrame instances must
     * be labeled by the time this method is called. The specified DataFrame may be
     * of any type.
     * 
     * <p>All columns in the DataFrame argument that are also existent in
     * this DataFrame are excluded in the result DataFrame returned by this method.
     * Therefore, in the case of duplicate columns, the returned DataFrame only
     * contains the corresponding column from this DataFrame
     * 
     * @param df The <code>DataFrame</code> to join with. Must not be null
     * @param col1 The name of the <code>Column</code> in this DataFrame to match
     *             values for. Must not be null or empty
     * @param col2 The name of the <code>Column</code> in the specified DataFrame
     *             to match values for. Must not be null or empty
     * @return A <code>DataFrame</code> with joined rows from both this and
     *         the specified DataFrame that have matching values in the columns
     *         with the specified names
     */
    public DataFrame join(DataFrame df, String col1, String col2);

    /**
     * Computes the average of all entries in the specified column.
     * If the underlying DataFrame implementation supports null values, then
     * null values are excluded from the computation and do not contribute to
     * the total number of entries. The average can only be computed for
     * numeric columns
     * 
     * @param col The index of the column to compute the average for
     * @return The average of all entries in the specified column
     * @see #median(int)
     */
    public double average(int col);

    /**
     * Computes the average of all entries in the specified column. If the
     * underlying DataFrame implementation supports null values, then null
     * values are excluded from the computation and do not contribute to the
     * total number of entries. The average can only be computed for
     * numeric columns
     * 
     * @param col The name of the column to compute the average for
     * @return The average of all entries in the specified column
     * @see #median(String)
     */
    public double average(String col);
    
    /**
     * Computes the median of all entries in the specified column.
     * If the underlying DataFrame implementation supports null values, then
     * null values are excluded from the computation. The median can only
     * be computed for numeric columns
     * 
     * @param col The index of the column to compute the median for
     * @return The median of all entries in the specified column
     * @see #average(int)
     */
    public double median(int col);

    /**
     * Computes the median of all entries in the specified column. If the
     * underlying DataFrame implementation supports null values, then null
     * values are excluded from the computation. The median can only
     * be computed for numeric columns
     * 
     * @param col The name of the column to compute the median for
     * @return The median of all entries in the specified column
     * @see #average(String)
     */
    public double median(String col);

    /**
     * Computes the minimum of all entries in the specified column.
     * If the underlying DataFrame implementation supports null values, then null
     * values are excluded from the computation. The minimum can only
     * be computed for numeric columns
     * 
     * @param col The index of the column to compute the minimum for
     * @return The minimum of all entries in the specified column
     */
    public double minimum(int col);

    /**
     * Computes the minimum of all entries in the specified column.
     * If the underlying DataFrame implementation supports null values, then
     * null values are excluded from the computation.
     * The minimum can only be computed for numeric columns
     * 
     * @param col The name of the column to compute the minimum for
     * @return The minimum of all entries in the specified column
     */
    public double minimum(String col);

    /**
     * Computes the <code>n</code>-minimum entries in the specified column and
     * returns the corresponding rows as a DataFrame. The rank specifies the
     * maximum number of rows to return (the number <code>n</code>). The returned
     * DataFrame is ordered ascendingly according to the values in the specified
     * column, i.e. the minimum is located at row 0, the second minimum
     * at row 1, etc.
     * 
     * <p>If the underlying DataFrame implementation supports null values, then null
     * values are excluded from the computation. The minimum can only be computed for
     * numeric columns
     * 
     * @param col The index of the column to compute the <code>n</code>-minima for
     * @param rank The maximum number of rows to return
     * @return A <code>DataFrame</code> containing at most <code>n</code> rows,
     *         ordered ascendingly by the column with the specified index
     */
    public DataFrame minimum(int col, int rank);

    /**
     * Computes the <code>n</code>-minimum entries in the specified column and
     * returns the corresponding rows as a DataFrame. The rank specifies the
     * maximum number of rows to return (the number <code>n</code>). The returned
     * DataFrame is ordered ascendingly according to the values in the specified
     * column, i.e. the minimum is located at row 0, the second minimum
     * at row 1, etc.
     * 
     * <p>If the underlying DataFrame implementation supports null values, then null
     * values are excluded from the computation. The minimum can only be computed for
     * numeric columns
     * 
     * @param col The name of the column to compute the <code>n</code>-minima for
     * @param rank The maximum number of rows to return
     * @return A <code>DataFrame</code> containing at most <code>n</code> rows,
     *         ordered ascendingly by the column with the specified name
     */
    public DataFrame minimum(String col, int rank);

    /**
     * Computes the maximum of all entries in the specified column.
     * If the underlying DataFrame implementation supports null values,
     * then null values are excluded from the computation.
     * The maximum can only be computed for numeric columns
     * 
     * @param col The index of the column to compute the maximum for
     * @return The maximum of all entries in the specified column
     */
    public double maximum(int col);

    /**
     * Computes the maximum of all entries in the specified column.
     * If the underlying DataFrame implementation supports null values,
     * then null values are excluded from the computation.
     * The maximum can only be computed for numeric columns
     * 
     * @param col The name of the column to compute the maximum for
     * @return The maximum of all entries in the specified column
     */
    public double maximum(String col);

    /**
     * Computes the <code>n</code>-maximum entries in the specified column and
     * returns the corresponding rows as a DataFrame. The rank specifies the
     * maximum number of rows to return (the number <code>n</code>). The returned
     * DataFrame is ordered descendingly according to the values in the specified
     * column, i.e. the maximum is located at row 0, the second maximum
     * at row 1, etc.
     * 
     * <p>If the underlying DataFrame implementation supports null values, then null
     * values are excluded from the computation. The maximum can only be computed for
     * numeric columns
     * 
     * @param col The index of the column to compute the <code>n</code>-maxima for
     * @param rank The maximum number of rows to return
     * @return A <code>DataFrame</code> containing at most <code>n</code> rows,
     *         ordered descendingly by the column with the specified index
     */
    public DataFrame maximum(int col, int rank);

    /**
     * Computes the <code>n</code>-maximum entries in the specified column and
     * returns the corresponding rows as a DataFrame. The rank specifies the
     * maximum number of rows to return (the number <code>n</code>). The returned
     * DataFrame is ordered descendingly according to the values in the specified
     * column, i.e. the maximum is located at row 0, the second maximum
     * at row 1, etc.
     * 
     * <p>If the underlying DataFrame implementation supports null values, then null
     * values are excluded from the computation. The maximum can only be computed for
     * numeric columns
     * 
     * @param col The name of the column to compute the <code>n</code>-maxima for
     * @param rank The maximum number of rows to return
     * @return A <code>DataFrame</code> containing at most <code>n</code> rows,
     *         ordered descendingly by the column with the specified name
     */
    public DataFrame maximum(String col, int rank);

    /**
     * Computes the sum of all entries in the specified column.
     * If the underlying DataFrame implementation supports null values,
     * then null values are excluded from the computation.
     * The sum can only be computed for numeric columns
     * 
     * @param col The index of the column to compute the sum for
     * @return The sum of all entries in the specified column
     */
    public double sum(int col);

    /**
     * Computes the sum of all entries in the specified column.
     * If the underlying DataFrame implementation supports null values,
     * then null values are excluded from the computation.
     * The sum can only be computed for numeric columns
     * 
     * @param col The name of the column to compute the sum for
     * @return The sum of all entries in the specified column
     */
    public double sum(String col);

    /**
     * Computes the absolute value for all numeric values in the column
     * at the specified index. All values are replaced by their absolute value.
     * If the underlying DataFrame implementation supports null values, then
     * null values are excluded from the computation.<br>
     * The absolute can only be computed for numeric columns
     * 
     * @param col The index of the column to compute the absolutes for
     * @return This <code>DataFrame</code> instance
     */
    public DataFrame absolute(int col);

    /**
     * Computes the absolute value for all numeric values in the column
     * with the specified name. All values are replaced by their absolute value.
     * If the underlying DataFrame implementation supports null values, then
     * null values are excluded from the computation.<br>
     * The absolute can only be computed for numeric columns
     * 
     * @param col The name of the column to compute the absolutes for
     * @return This <code>DataFrame</code> instance
     */
    public DataFrame absolute(String col);

    /**
     * Computes the value from the ceil function for all numeric values in
     * the column at the specified index. The ceil function rounds numbers to the
     * next largest integer that is equal or greater than the input value. All
     * values in the specified column are replaced by their ceil value. If the
     * underlying DataFrame implementation supports null values, then null values
     * are excluded from the computation.<br>
     * The ceil can only be computed for numeric columns
     * 
     * @param col The index of the column to ceil values for
     * @return This <code>DataFrame</code> instance
     */
    public DataFrame ceil(int col);

    /**
     * Computes the value from the ceil function for all numeric values in
     * the column with the specified name. The ceil function rounds numbers to the
     * next largest integer that is equal or greater than the input value. All
     * values in the specified column are replaced by their ceil value. If the
     * underlying DataFrame implementation supports null values, then null values
     * are excluded from the computation.<br>
     * The ceil can only be computed for numeric columns
     * 
     * @param col The name of the column to ceil values for
     * @return This <code>DataFrame</code> instance
     */
    public DataFrame ceil(String col);

    /**
     * Computes the value from the floor function for all numeric values in
     * the column at the specified index. The floor function returns the largest
     * integer less than or equal to the input. All values in the specified column
     * are replaced by their floor value. If the underlying DataFrame implementation
     * supports null values, then null values are excluded from the computation.<br>
     * The floor can only be computed for numeric columns
     * 
     * @param col The index of the column to floor values for
     * @return This <code>DataFrame</code> instance
     */
    public DataFrame floor(int col);

    /**
     * Computes the value from the floor function for all numeric values in
     * the column with the specified name. The floor function returns the largest
     * integer less than or equal to the input. All values in the specified column
     * are replaced by their floor value. If the underlying DataFrame implementation
     * supports null values, then null values are excluded from the computation.<br>
     * The floor can only be computed for numeric columns
     * 
     * @param col The name of the column to floor values for
     * @return This <code>DataFrame</code> instance
     */
    public DataFrame floor(String col);

    /**
     * Rounds all values in the column at the specified index to the specified
     * number of decimal places. All values in the specified column are replaced by
     * their rounded value. If the underlying DataFrame implementation supports null
     * values, then null values are excluded from the computation.<br>
     * Rounding can only be conducted for numeric columns
     * 
     * @param col The index of the column to round values for
     * @param decPlaces The number of decimal places to round to. Must be non-negative
     * @return This <code>DataFrame</code> instance
     */
    public DataFrame round(int col, int decPlaces);

    /**
     * Rounds all values in the column with the specified name to the specified
     * number of decimal places. All values in the specified column are replaced by
     * their rounded value. If the underlying DataFrame implementation supports null
     * values, then null values are excluded from the computation.<br>
     * Rounding can only be conducted for numeric columns
     * 
     * @param col The name of the column to round values for
     * @param decPlaces The number of decimal places to round to. Must be non-negative
     * @return This <code>DataFrame</code> instance
     */
    public DataFrame round(String col, int decPlaces);

    /**
     * Applies a range threshold to all numeric values in the column at the specified
     * index. This operation ensures that all numeric values in the specified column
     * are within the specified range. If the underlying DataFrame implementation
     * supports null values, then null values are excluded from the computation.<br>
     * If a particular threshold side is null, then no threshold is applied to all
     * numeric values for that side. The lower clip boundary must be smaller than
     * the upper boundary
     * 
     * @param col The index of the column to clip numeric values in
     * @param low The lower boundary for all numeric values. May be null
     * @param high The upper boundary for all numeric values. May be null
     * @return This <code>DataFrame</code> instance
     */
    public DataFrame clip(int col, Number low, Number high);

    /**
     * Applies a range threshold to all numeric values in the column with the specified
     * name. This operation ensures that all numeric values in the specified column
     * are within the specified range. If the underlying DataFrame implementation
     * supports null values, then null values are excluded from the computation.<br>
     * If a particular threshold side is null, then no threshold is applied to all
     * numeric values for that side. The lower clip boundary must be smaller than
     * the upper boundary
     * 
     * @param col The name of the column to clip numeric values in
     * @param low The lower boundary for all numeric values. May be null
     * @param high The upper boundary for all numeric values. May be null
     * @return This <code>DataFrame</code> instance
     */
    public DataFrame clip(String col, Number low, Number high);

    /**
     * Sorts the entire DataFrame according to the values in the specified column.
     * <br>The DataFrame is sorted in ascending order
     * 
     * @param col The index of the column to sort the DataFrame by
     * @return This <code>DataFrame</code> instance
     */
    public DataFrame sortBy(int col);

    /**
     * Sorts the entire DataFrame according to the values in the specified column.
     * <br>The DataFrame is sorted in ascending order
     * 
     * @param col The name of the column to sort the DataFrame by
     * @return This <code>DataFrame</code> instance
     */
    public DataFrame sortBy(String col);

    /**
     * Sorts the entire DataFrame according to the values in the specified column.
     * <br>The DataFrame is sorted in ascending order
     * 
     * @param col The index of the column to sort the DataFrame by
     * @return This <code>DataFrame</code> instance
     */
    public DataFrame sortAscendingBy(int col);

    /**
     * Sorts the entire DataFrame according to the values in the specified column.
     * <br>The DataFrame is sorted in ascending order
     * 
     * @param col The name of the column to sort the DataFrame by
     * @return This <code>DataFrame</code> instance
     */
    public DataFrame sortAscendingBy(String col);

    /**
     * Sorts the entire DataFrame according to the values in the specified column.
     * <br>The DataFrame is sorted in descending order
     * 
     * @param col The index of the column to sort the DataFrame by
     * @return This <code>DataFrame</code> instance
     */
    public DataFrame sortDescendingBy(int col);

    /**
     * Sorts the entire DataFrame according to the values in the specified column.
     * <br>The DataFrame is sorted in descending order
     * 
     * @param col The name of the column to sort the DataFrame by
     * @return This <code>DataFrame</code> instance
     */
    public DataFrame sortDescendingBy(String col);

    /**
     * Returns the first 5 rows of this DataFrame. The returned DataFrame is not
     * backed by this DataFrame, so changing entries in one DataFrame has
     * no effect on the other DataFrame and vice versa
     * 
     * @return A <code>DataFrame</code> containing at most 5 anterior rows
     * @see DataFrame#head(int)
     */
    public DataFrame head();

    /**
     * Returns the first <code>n</code> rows of this DataFrame. The returned
     * DataFrame is not backed by this DataFrame, so changing entries in
     * one DataFrame has no effect on the other DataFrame and vice versa
     * 
     * @param rows The number of anterior rows to return
     * @return A <code>DataFrame</code> containing at most <code>n</code>
     *         anterior rows
     */
    public DataFrame head(int rows);

    /**
     * Returns the last 5 rows of this DataFrame. The returned DataFrame is not
     * backed by this DataFrame, so changing entries in one DataFrame has
     * no effect on the other DataFrame and vice versa
     * 
     * @return A <code>DataFrame</code> containing at most 5 posterior rows
     * @see DataFrame#tail(int)
     */
    public DataFrame tail();

    /**
     * Returns the last <code>n</code> rows of this DataFrame. The returned
     * DataFrame is not backed by this DataFrame, so changing entries in
     * one DataFrame has no effect on the other DataFrame and vice versa
     * 
     * @param rows The number of posterior rows to return
     * @return A <code>DataFrame</code> containing at most <code>n</code>
     *         posterior rows
     */
    public DataFrame tail(int rows);

    /**
     * Creates an informative string about this DataFrame
     * 
     * @return A string providing information describing this DataFrame
     */
    public String info();

    /**
     * Returns this DataFrame as an array of Objects. The first
     * dimension contains the columns of the DataFrame and the second dimension
     * contains the entries of each column (i.e. rows). The returned array is
     * not backed by the DataFrame, so changing entries in the array has
     * no effect on the DataFrame and vice versa
     * 
     * @return An array of Objects representing this DataFrame
     */
    public Object[][] toArray();

    /**
     * Returns a human readable string representation of this DataFrame
     * 
     * @return A string representation of this DataFrame
     * @see java.lang.Object#toString()
     */
    public String toString();

    /**
     * Creates and returns a copy of this DataFrame
     * 
     * @return A copy of this DataFrame
     * @see java.lang.Object#clone()
     */
    public DataFrame clone();

    /**
     * Returns a hash code value for this DataFrame. This method has
     * to be provided by all DataFrame implementations in order to fulfill
     * the general contract of <code>hashCode()</code> and <code>equals()</code>
     * 
     * @return A hash code value for this DataFrame
     * @see java.lang.Object#hashCode()
     */
    public int hashCode();

    /**
     * Indicates whether this DataFrame's column structure and content
     * is equal to the structure and content of the specified DataFrame.
     * In that regard the order and types of the underlying columns matter
     * 
     * @param obj The reference DataFrame with which to compare. May be null
     * @return True if this DataFrame is equal to the obj argument, false otherwise
     * @see java.lang.Object#equals(Object)
     */
    public boolean equals(Object obj);

    /**
     * Indicates the current memory usage of this DataFrame in bytes. The returned
     * value refers to the minimum amount of memory needed to store the values of
     * all columns in the underlying arrays.
     * 
     * <p>Please note that the memory usage is computed for the raw payload data
     * of the underlying columns, comparable to the space needed in an uncompressed
     * serialized form. Other data e.g. column labels, internal representations,
     * encodings etc., are not taken into account. The actual memory required by the
     * underlying <code>DataFrame</code> instance might be considerably higher
     * than the value indicated by this method
     * 
     * @return The current memory usage of this DataFrame in bytes
     */
    public int memoryUsage();

    /**
     * Returns an iterator over all Columns in this DataFrame
     * 
     * @return An Iterator over all Columns in this DataFrame
     * @see java.lang.Iterable#iterator()
     */
    public Iterator<Column> iterator();

    /**
     * Creates and returns a copy of the specified {@link DataFrame}
     * 
     * @param df The <code>DataFrame</code> instance to copy
     * @return A copy of the specified DataFrame or null if the argument is null
     */
    public static DataFrame copy(final DataFrame df){
        return DataFrameUtils.copyOf(df);
    }

    /**
     * Creates and returns a DataFrame which has the same column structure
     * and column names as the specified DataFrame instance
     * but is otherwise empty
     * 
     * @param df The <code>DataFrame</code> from which to copy the
     *           column structure
     * @return A <code>DataFrame</code> with the same column structure and names
     *         as the specified DataFrame, or null if the specified DataFrame is null
     */
    public static DataFrame like(final DataFrame df){
        return DataFrameUtils.like(df);
    }

    /**
     * Merges all given {@link DataFrame} instances into one DataFrame.
     * All DataFames are merged by columns. All DataFrames must have an equal
     * number of rows but may be of any type. All columns are added to the
     * returned DataFrame in the order of the arguments passed to this method.
     * Only passing one DataFrame to this method will simply return
     * that instance.<br>
     * Columns with duplicate names are included in the returned DataFrame
     * and a postfix is added to each duplicate column name. 
     * 
     * <p>All columns of the returned DataFrame are backed by their origin, which
     * means that changes to the original DataFrame are reflected in the merged
     * DataFrame and vice versa. This does not apply, however, if columns need
     * to be converted to a nullable type. For example, if one DataFrame argument
     * is nullable, then all columns from non-nullable DataFrame arguments are
     * converted to their corresponding nullable equivalent.<br>
     * 
     * If columns should be independent from their origin, then simply pass
     * a clone (copy) of each DataFrame argument to this method.
     * <p>Example:<br> 
     * <code>
     * DataFrame merged = DataFrame.merge(DataFrame.copf(df1), DataFrame.copy(df2));
     * </code>
     * 
     * @param dataFrames The DataFrames to be merged
     * @return A DataFrame composed of all columns of the given DataFrames
     */
    public static DataFrame merge(final DataFrame... dataFrames){
        return DataFrameUtils.merge(dataFrames);
    }

    /**
     * Converts the given {@link DataFrame} from a {@link DefaultDataFrame} to a 
     * {@link NullableDataFrame} or vice versa.<br>
     * Converting a DefaultDataFrame to a NullableDataFrame will not change
     * any internal values, except that now you can add/insert null values to it.<br>
     * Converting a NullableDataFrame to a DefaultDataFrame will convert all null
     * occurrences to the primitive defaults according to the column they are located.
     * 
     * <p>Example: (if 'mydf' is a DefaultDataFrame)<br>
     * <code>DataFrame df = DataFrame.convert(mydf, NullableDataFrame.class);</code>
     * 
     * @param df The DataFrame instance to convert
     * @param type The type to convert the given DataFrame to
     * @return A DataFrame converted from the type of the argument passed to this 
     * 		   method to the type specified
     */
    public static DataFrame convert(final DataFrame df, final Class<?> type){
        return DataFrameUtils.convert(df, type);
    }

    /**
     * Serializes the given <code>DataFrame</code> to an array of bytes.<br>
     * The returned array is not compressed. The compression of the array can be 
     * controlled by passing an additional boolean flag to the arguments.
     * 
     * <p>See {@link DataFrame#serialize(DataFrame, boolean)}
     * 
     * @param df The DataFrame to serialize. Must not be null
     * @return A byte array representing the given DataFrame in a serialized form
     * @throws SerializationException If any errors occur during serialization
     */
    public static byte[] serialize(final DataFrame df) throws SerializationException{
        return DataFrameSerializer.serialize(df);
    }

    /**
     * Serializes the given <code>DataFrame</code> to an array of bytes.<br>
     * The compression of the returned array is controlled by the additional boolean 
     * flag of this method.
     * 
     * @param df The DataFrame to serialize. Must not be null
     * @param compress A boolean flag indicating whether to compress the serialized bytes.
     *                 Must be either {@link DataFrameSerializer#MODE_COMPRESSED}
     *                 or {@link DataFrameSerializer#MODE_UNCOMPRESSED}
     * @return A byte array representing the given DataFrame in a serialized form
     * @throws SerializationException If any errors occur during
     *                                serialization or compression
     */
    public static byte[] serialize(final DataFrame df, final boolean compress)
            throws SerializationException{

        return DataFrameSerializer.serialize(df, compress);
    }

    /**
     * Deserializes the given array of bytes to a <code>DataFrame</code>.
     * 
     * <p>If the given byte array is compressed, it will be automatically
     * decompressed before the deserialization is executed. The byte array of
     * the provided reference may be affected by this operation as long as
     * decompression is in process. The original state, however, will be restored
     * after decompression. This approach helps avoid additional copy operations and
     * should be considered when writing multi-threaded code that uses deserialization
     * as provided by this method.
     * 
     * <p>Deserialization of uncompressed arrays does only require read access and
     * therefore will never alter the content of the provided array
     * 
     * @param bytes The byte array representing the DataFrame to deserialize.
     *              Must not be null
     * @return A DataFrame from the given array of bytes
     * @throws SerializationException If any errors occur during deserialization
     *                                or decompression, or if the given byte array
     *                                does not constitute a DataFrame
     */
    public static DataFrame deserialize(final byte[] bytes)
            throws SerializationException{

        return DataFrameSerializer.deserialize(bytes);
    }

    /**
     * Reads the specified file and returns a DataFrame constituted by the 
     * content of that file
     * 
     * @param file The file to read. Must be a <code>.df</code> file
     * @return A DataFrame from the specified file
     * @throws IOException If any errors occur during file reading
     * @throws SerializationException If any errors occur during deserialization
     */
    public static DataFrame read(final String file)
            throws IOException, SerializationException{

        return DataFrameSerializer.readFile(file);
    }

    /**
     * Reads the specified file and returns a DataFrame constituted by the 
     * content of that file
     * 
     * @param file The file to read. Must be a <code>.df</code> file
     * @return A DataFrame from the specified file
     * @throws IOException If any errors occur during deserialization or file reading
     * @throws SerializationException If any errors occur during deserialization
     */
    public static DataFrame read(final File file)
            throws IOException, SerializationException{

        return DataFrameSerializer.readFile(file);
    }

    /**
     * Persists the given DataFrame to the specified file
     * 
     * @param file The file to write the DataFrame to. Must not be null
     * @param df The DataFrame to persist. Must not be null
     * @throws IOException If any errors occur during file persistence
     * @throws SerializationException If any errors occur during serialization
     */
    public static void write(final String file, final DataFrame df)
            throws IOException, SerializationException{

        DataFrameSerializer.writeFile(file, df);
    }

    /**
     * Persists the given DataFrame to the specified file
     * 
     * @param file The file to write the DataFrame to. Must not be null
     * @param df The DataFrame to persist. Must not be null
     * @throws IOException If any errors occur during file persistence
     * @throws SerializationException If any errors occur during serialization
     */
    public static void write(final File file, final DataFrame df)
            throws SerializationException, IOException{

        DataFrameSerializer.writeFile(file, df);
    }

    /**
     * Serializes the given DataFrame to a <code>Base64</code> encoded string
     * 
     * @param df The DataFrame to serialize to a Base64 encoded string.
     *           Must not be null
     * @return A Base64 encoded string representing the given DataFrame
     * @throws SerializationException If any errors occur during serialization
     */
    public static String toBase64(final DataFrame df)
            throws SerializationException{

        return DataFrameSerializer.toBase64(df);
    }

    /**
     * Deserializes the given <code>Base64</code> encoded string to a DataFrame
     * 
     * @param string The Base64 encoded string representing the DataFrame to deserialize.
     *               Must not be null
     * @return A DataFrame from the given Base64 string
     * @throws SerializationException If any errors occur during deserialization
     */
    public static DataFrame fromBase64(final String string)
            throws SerializationException{

        return DataFrameSerializer.fromBase64(string);
    }
}
