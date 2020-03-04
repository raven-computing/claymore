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

import java.util.Iterator;

/**
 * An object that binds strongly typed columns into one data structure. All columns
 * have equal length but each column can be of a different type. Columns can have
 * a name associated with them so they can be accessed through that name without
 * knowledge of the index the column is located at.
 * 
 * <p>All implementations of this interface work with the {@link Column} type, which
 * represents any column of a particular DataFrame. In order to ensure type safety
 * the concrete type of each column must be specified at compile time.
 * 
 * <p>All implementing classes provide at least three constructors: a void (no arguments) 
 * constructor which creates an empty DataFrame, a constructor which takes multiple
 * arguments of type <code>Column</code>, creating a DataFrame composed of the given
 * columns, and a constructor which takes a String array followed by multiple arguments
 * of type <code>Column</code>, which creates a DataFrame composed of the given
 * named columns.<br>
 * Column names can be created and changed at any time by calling the appropriate methods.
 * 
 * <p>DataFrames can also be constructed by passing a class implementing the {@link Row} 
 * interface to the constructor. That constructor will infer all column types
 * and names from the fields annotated with {@link RowItem} within the provided class.<br>
 * Consequently rows can be also retrieved, set and inserted in a similar manner.
 * However, please note that this feature can only be used when dealing with
 * static DataFrames, i.e. the defined column structure does not change at runtime.
 * When using dynamic DataFrames with changing column structures, then the normal
 * way of working with rows is to use arrays of Objects. Be aware that type safety
 * is part of the specification and is always enforced by all DataFrame implementations.
 * 
 * <p>Most methods can throw a {@link DataFrameException} at runtime if any argument
 * passed to it is invalid, for example an out of bounds index, or if that operation
 * would result in an incoherent/invalid state of that DataFrame. Javadocs of specific
 * methods will not mention this again since <code>DataFrameException</code>
 * is a <Code>RuntimeException</code>.
 * 
 * <p>This interface also provides various static utility methods
 * to work with DataFrames.
 * 
 * @author Phil Gaiser
 * @see DefaultDataFrame
 * @see NullableDataFrame
 * @since 1.0.0
 *
 */
public interface DataFrame extends Cloneable, Iterable<Column> {

    /**
     * Gets the byte at the specified column and row index. If the underlying DataFrame
     * implementation doesn't support null values, then the returned
     * Byte object is guaranteed to be non-null
     * 
     * @param col The column index of the value to get
     * @param row The row index of the value to get
     * @return The byte value at the specified position
     */
    public Byte getByte(int col, int row);

    /**
     * Gets the byte from the specified column at the specified row index.
     * The column must be specified by name. If the underlying DataFrame implementation
     * doesn't support null values, then the returned Byte object
     * is guaranteed to be non-null
     * 
     * @param colName The name of the column to get the value from
     * @param row The row index of the value to get
     * @return The byte value at the specified position
     */
    public Byte getByte(final String colName, final int row);

    /**
     * Gets the short at the specified column and row index. If the underlying DataFrame
     * implementation doesn't support null values, then the returned
     * Short object is guaranteed to be non-null
     * 
     * @param col The column index of the value to get
     * @param row The row index of the value to get
     * @return The short value at the specified position
     */
    public Short getShort(final int col, int row);

    /**
     * Gets the short from the specified column at the specified row index.
     * The column must be specified by name. If the underlying DataFrame implementation
     * doesn't support null values, then the returned Short object
     * is guaranteed to be non-null
     * 
     * @param colName The name of the column to get the value from
     * @param row The row index of the value to get
     * @return The short value at the specified position
     */
    public Short getShort(String colName, int row);

    /**
     * Gets the int at the specified column and row index. If the underlying DataFrame
     * implementation doesn't support null values, then the returned
     * Integer object is guaranteed to be non-null
     * 
     * @param col The column index of the value to get
     * @param row The row index of the value to get
     * @return The int value at the specified position
     */
    public Integer getInt(int col, int row);

    /**
     * Gets the int from the specified column at the specified row index.
     * The column must be specified by name. If the underlying DataFrame implementation
     * doesn't support null values, then the returned Integer object
     * is guaranteed to be non-null
     * 
     * @param colName The name of the column to get the value from
     * @param row The row index of the value to get
     * @return The int value at the specified position
     */
    public Integer getInt(String colName, int row);

    /**
     * Gets the long at the specified column and row index. If the underlying DataFrame
     * implementation doesn't support null values, then the returned
     * Long object is guaranteed to be non-null
     * 
     * @param col The column index of the value to get
     * @param row The row index of the value to get
     * @return The long value at the specified position
     */
    public Long getLong(int col, int row);

    /**
     * Gets the long from the specified column at the specified row index.
     * The column must be specified by name. If the underlying DataFrame implementation
     * doesn't support null values, then the returned Long object
     * is guaranteed to be non-null
     * 
     * @param colName The name of the column to get the value from
     * @param row The row index of the value to get
     * @return The long value at the specified position
     */
    public Long getLong(String colName, int row);

    /**
     * Gets the string at the specified column and row index. If the underlying DataFrame
     * implementation doesn't support null values, then the returned
     * String object is guaranteed to be non-null
     * 
     * @param col The column index of the value to get
     * @param row The row index of the value to get
     * @return The string value at the specified position
     */
    public String getString(int col, int row);

    /**
     * Gets the string from the specified column at the specified row index.
     * The column must be specified by name. If the underlying DataFrame implementation
     * doesn't support null values, then the returned String object
     * is guaranteed to be non-null
     * 
     * @param colName The name of the column to get the value from
     * @param row The row index of the value to get
     * @return The string value at the specified position
     */
    public String getString(String colName, int row);

    /**
     * Gets the float at the specified column and row index. If the underlying DataFrame
     * implementation doesn't support null values, then the returned
     * Float object is guaranteed to be non-null
     * 
     * @param col The column index of the value to get
     * @param row The row index of the value to get
     * @return The float value at the specified position
     */
    public Float getFloat(int col, int row);

    /**
     * Gets the float from the specified column at the specified row index.
     * The column must be specified by name. If the underlying DataFrame implementation
     * doesn't support null values, then the returned Float object
     * is guaranteed to be non-null
     * 
     * @param colName The name of the column to get the value from
     * @param row The row index of the value to get
     * @return The float value at the specified position
     */
    public Float getFloat(String colName, int row);

    /**
     * Gets the double at the specified column and row index. If the underlying DataFrame
     * implementation doesn't support null values, then the returned
     * Double object is guaranteed to be non-null
     * 
     * @param col The column index of the value to get
     * @param row The row index of the value to get
     * @return The double value at the specified position
     */
    public Double getDouble(int col, int row);

    /**
     * Gets the double from the specified column at the specified row index.
     * The column must be specified by name. If the underlying DataFrame implementation
     * doesn't support null values, then the returned Double object
     * is guaranteed to be non-null
     * 
     * @param colName The name of the column to get the value from
     * @param row The row index of the value to get
     * @return The double value at the specified position
     */
    public Double getDouble(String colName, int row);

    /**
     * Gets the char at the specified column and row index. If the underlying DataFrame
     * implementation doesn't support null values, then the returned
     * Character object is guaranteed to be non-null
     * 
     * @param col The column index of the value to get
     * @param row The row index of the value to get
     * @return The char value at the specified position
     */
    public Character getChar(int col, int row);

    /**
     * Gets the char from the specified column at the specified row index.
     * The column must be specified by name. If the underlying DataFrame implementation
     * doesn't support null values, then the returned Character object
     * is guaranteed to be non-null
     * 
     * @param colName The name of the column to get the value from
     * @param row The row index of the value to get
     * @return The char value at the specified position
     */
    public Character getChar(String colName, int row);

    /**
     * Gets the boolean at the specified column and row index. If the underlying DataFrame
     * implementation doesn't support null values, then the returned
     * Boolean object is guaranteed to be non-null
     * 
     * @param col The column index of the value to get
     * @param row The row index of the value to get
     * @return The boolean value at the specified position
     */
    public Boolean getBoolean(int col, int row);

    /**
     * Gets the boolean from the specified column at the specified row index.
     * The column must be specified by name. If the underlying DataFrame implementation
     * doesn't support null values, then the returned Boolean object
     * is guaranteed to be non-null
     * 
     * @param colName The name of the column to get the value from
     * @param row The row index of the value to get
     * @return The boolean value at the specified position
     */
    public Boolean getBoolean(String colName, int row);

    /**
     * Gets the binary data at the specified column and row index.
     * If the underlying DataFrame implementation doesn't support
     * null values, then the returned byte array is guaranteed to be non-null
     * 
     * @param col The column index of the binary data to get
     * @param row The row index of the binary data to get
     * @return The binary data at the specified position
     */
    public byte[] getBinary(int col, int row);

    /**
     * Gets the binary data from the specified column at the specified row index.
     * The column must be specified by name. If the underlying DataFrame implementation
     * doesn't support null values, then the returned byte array
     * is guaranteed to be non-null
     * 
     * @param colName The name of the column to get the binary data from
     * @param row The row index of the binary data to get
     * @return The binary data at the specified position
     */
    public byte[] getBinary(String colName, int row);

    /**
     * Sets the byte at the specified column and row index. If the underlying DataFrame
     * implementation doesn't support null values, then the provided
     * Byte object must not be null
     * 
     * @param col The column index of the value to set
     * @param row The row index of the value to set
     * @param value The byte value to set
     */
    public void setByte(int col, int row, Byte value);

    /**
     * Sets the byte at the specified column at the specified row index.
     * The column must be specified by name. If the underlying DataFrame implementation
     * doesn't support null values, then the provided Byte object must not be null
     * 
     * @param colName The name of the column to set the value at
     * @param row The row index of the value to set
     * @param value The byte value to set
     */
    public void setByte(String colName, int row, Byte value);

    /**
     * Sets the short at the specified column and row index. If the underlying DataFrame
     * implementation doesn't support null values, then the provided
     * Short object must not be null
     * 
     * @param col The column index of the value to set
     * @param row The row index of the value to set
     * @param value The short value to set
     */
    public void setShort(int col, int row, Short value);

    /**
     * Sets the short at the specified column at the specified row index.
     * The column must be specified by name. If the underlying DataFrame implementation
     * doesn't support null values, then the provided Short object must not be null
     * 
     * @param colName The name of the column to set the value at
     * @param row The row index of the value to set
     * @param value The short value to set
     */
    public void setShort(String colName, int row, Short value);

    /**
     * Sets the int at the specified column and row index. If the underlying DataFrame
     * implementation doesn't support null values, then the provided
     * Integer object must not be null
     * 
     * @param col The column index of the value to set
     * @param row The row index of the value to set
     * @param value The int value to set
     */
    public void setInt(int col, int row, Integer value);

    /**
     * Sets the int at the specified column at the specified row index.
     * The column must be specified by name. If the underlying DataFrame implementation
     * doesn't support null values, then the provided Integer object must not be null
     * 
     * @param colName The name of the column to set the value at
     * @param row The row index of the value to set
     * @param value The int value to set
     */
    public void setInt(String colName, int row, Integer value);

    /**
     * Sets the long at the specified column and row index. If the underlying DataFrame
     * implementation doesn't support null values, then the provided
     * Long object must not be null
     * 
     * @param col The column index of the value to set
     * @param row The row index of the value to set
     * @param value The long value to set
     */
    public void setLong(int col, int row, Long value);

    /**
     * Sets the long at the specified column at the specified row index.
     * The column must be specified by name. If the underlying DataFrame implementation
     * doesn't support null values, then the provided Long object must not be null
     * 
     * @param colName The name of the column to set the value at
     * @param row The row index of the value to set
     * @param value The long value to set
     */
    public void setLong(String colName, int row, Long value);

    /**
     * Sets the string at the specified column and row index. If the underlying DataFrame
     * implementation doesn't support null values or empty strings,
     * then the provided String object will be converted to 'n/a' if it is null or empty
     * 
     * @param col The column index of the value to set
     * @param row The row index of the value to set
     * @param value The string value to set
     */
    public void setString(int col, int row, String value);

    /**
     * Sets the string at the specified column at the specified row index.
     * The column must be specified by name. If the underlying DataFrame implementation
     * doesn't support null values or empty strings, then the provided String object
     * will be converted to 'n/a' if it is null or empty
     * 
     * @param colName The name of the column to set the value at
     * @param row The row index of the value to set
     * @param value The string value to set
     */
    public void setString(String colName, int row, String value);

    /**
     * Sets the float at the specified column and row index. If the underlying DataFrame
     * implementation doesn't support null values, then the provided
     * Float object must not be null
     * 
     * @param col The column index of the value to set
     * @param row The row index of the value to set
     * @param value The float value to set
     */
    public void setFloat(int col, int row, Float value);

    /**
     * Sets the float at the specified column at the specified row index.
     * The column must be specified by name. If the underlying DataFrame implementation
     * doesn't support null values, then the provided Float object must not be null
     * 
     * @param colName The name of the column to set the value at
     * @param row The row index of the value to set
     * @param value The float value to set
     */
    public void setFloat(String colName, int row, Float value);

    /**
     * Sets the double at the specified column and row index. If the underlying DataFrame
     * implementation doesn't support null values, then the provided
     * Double object must not be null
     * 
     * @param col The column index of the value to set
     * @param row The row index of the value to set
     * @param value The double value to set
     */
    public void setDouble(int col, int row, Double value);

    /**
     * Sets the double at the specified column at the specified row index.
     * The column must be specified by name. If the underlying DataFrame implementation
     * doesn't support null values, then the provided Double object must not be null
     * 
     * @param colName The name of the column to set the value at
     * @param row The row index of the value to set
     * @param value The double value to set
     */
    public void setDouble(String colName, int row, Double value);

    /**
     * Sets the char at the specified column and row index. If the underlying DataFrame
     * implementation doesn't support null values, then the provided
     * Character object must not be null
     * 
     * @param col The column index of the value to set
     * @param row The row index of the value to set
     * @param value The char value to set
     */
    public void setChar(int col, int row, Character value);

    /**
     * Sets the char at the specified column at the specified row index.
     * The column must be specified by name. If the underlying DataFrame implementation
     * doesn't support null values, then the provided Character object must not be null
     * 
     * @param colName The name of the column to set the value at
     * @param row The row index of the value to set
     * @param value The char value to set
     */
    public void setChar(String colName, int row, Character value);

    /**
     * Sets the boolean at the specified column and row index. If the underlying DataFrame
     * implementation doesn't support null values, then the provided
     * Boolean object must not be null
     * 
     * @param col The column index of the value to set
     * @param row The row index of the value to set
     * @param value The boolean value to set
     */
    public void setBoolean(int col, int row, Boolean value);

    /**
     * Sets the boolean at the specified column at the specified row index.
     * The column must be specified by name. If the underlying DataFrame implementation
     * doesn't support null values, then the provided Boolean object must not be null
     * 
     * @param colName The name of the column to set the value at
     * @param row The row index of the value to set
     * @param value The boolean value to set
     */
    public void setBoolean(String colName, int row, Boolean value);

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
     * The column must be specified by name. If the underlying DataFrame implementation
     * doesn't support null values, then the provided byte array must not be null
     * 
     * @param colName The name of the column to set the binary data at
     * @param row The row index of the binary data to set
     * @param value The binary data to set
     */
    public void setBinary(String colName, int row, byte[] value);

    /**
     * Gets the name of all columns of this DataFrame in proper order.
     * Columns which have not been named will be represented
     * as their index within the DataFrame
     * 
     * @return The names of all columns, or null if no column names have been set
     */
    public String[] getColumnNames();

    /**
     * Gets the name of the column at the specified index
     * 
     * @param col The index of the column
     * @return The name of the column, or null if no name has been set for this column
     */
    public String getColumnName(int col);

    /**
     * Gets the index of the column with the specified name. 
     * <p>This method may throw a {@link DataFrameException} if no column of this DataFrame 
     * has the specified name or if no names have been set
     * 
     * @param colName The name of the column to get the index for. Must not be null
     * @return The index of the column with the specified name
     */
    public int getColumnIndex(String colName);

    /**
     * Assigns all columns the specified names. Any already set name will get overridden.
     * Make sure that the length of the provided array matches the number of
     * columns in this DataFrame.
     * 
     * <p>Please note that no checks will be performed regarding duplicates.
     * Assigning more than one column the same name will result in undefined behaviour
     * 
     * @param names The names of the columns as a String array. Must not be null
     */
    public void setColumnNames(String... names);

    /**
     * Assigns the specified column the specified name. No checks will
     * be performed regarding duplicates. Assigning more than one column the
     * same name will result in undefined behaviour. A return value is being provided
     * to indicate whether any previous name of the specified column was overridden
     * as a result of this operation
     * 
     * @param col The index of the column
     * @param name The name to assign to the column
     * @return True if any previous name was overridden, false otherwise
     */
    public boolean setColumnName(int col, String name);

    /**
     * Removes all column names of this DataFrame instance
     */
    public void removeColumnNames();

    /**
     * Indicates whether this DataFrame has any column names set. Please note
     * that this method returns true even when column names are only partially
     * set, i.e. not all columns must have a name assigned to them. In other words,
     * this method returns true if at least one column has a name assigned to it
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
    public Object[] getRowAt(int index);

    /**
     * Gets the row at the specified index. This method returns a custom object
     * implementing the {@link Row} interface. All columns must be labeled by the
     * time this method is called. The class of the returned type must be provided
     * as an argument. Passing null to the class argument will result in
     * a <code>NullPointerException</code>.<br>
     * The class implementing <code>Row</code> must properly annotate all members
     * representing row items. If the class complies to the contract, a new instance
     * of the specified type will be created through the default no-args constructor
     * with all annotated fields being set to the value of the
     * corresponding row item.<br>
     * If the underlying DataFrame implementation doesn't support null values, then
     * all annotated fields of the returned <code>Row</code> are guaranteed
     * to consist of non-null values
     * 
     * @param <T> The type of the desired row
     * @param index The index of the row to get
     * @param classOfT The class of the type <b>T</b>
     * @return The row at the specified index as the specified type <b>T</b>
     */
    public <T extends Row> T getRowAt(int index, Class<T> classOfT);

    /**
     * Sets and replaces the provided row within this DataFrame at the specified index.
     * <p>The type of each element must be equal to the type of the column it is placed in.
     * If the underlying DataFrame implementation doesn't support null values, then passing
     * an array with null values will result in a {@link DataFrameException}. Make sure the 
     * number of provided row items is equal to the number of columns in this DataFrame
     * 
     * @param index The index of the row
     * @param row The row itmes to set represented as an array of Objects
     */
    public void setRowAt(int index, Object... row);

    /**
     * Sets and replaces the provided row within this DataFrame at the specified index.
     * 
     * <p>The provided row is a custom object implementing the {@link Row} interface.<br>
     * All columns must be labeled by the time this method is called.<br>
     * The class implementing <code>Row</code> must properly annotate all
     * members representing row items.<br>
     * If the underlying DataFrame implementation doesn't support null values,
     * then passing a row with fields that consist of null values
     * will result in a {@link DataFrameException}.
     * 
     * @param index The index of the row
     * @param row The row to set represented as a custom object implementing the 
     *            {@link Row} interface
     */
    public void setRowAt(int index, Row row);

    /**
     * Adds the provided row to the end of this DataFrame.
     * 
     * <p>The type of each element must be equal to the type of the column it is placed in.
     * If the underlying DataFrame implementation doesn't support null values, then passing
     * an array with null values will result in a {@link DataFrameException}. Make sure
     * the number of provided row items is equal to the number of columns in this DataFrame
     * 
     * @param row The items of the row to add represented as an array of Objects
     */
    public void addRow(Object... row);

    /**
     * Adds the provided row to the end of this DataFrame.
     * <p>The provided row is a custom object implementing the {@link Row} interface.<br>
     * All columns must be labeled by the time this method is called.<br>
     * The class implementing <code>Row</code> must properly annotate all
     * members representing row items.<br>
     * If the underlying DataFrame implementation doesn't support null values,
     * then passing a row with fields that consist of null values will result
     * in a {@link DataFrameException}.
     * 
     * @param row The row to add represented as a custom object implementing the 
     *            {@link Row} interface
     */
    public void addRow(Row row);

    /**
     * Inserts the provided row into this DataFrame at the specified index. Shifts all
     * rows currently at that position and any subsequent rows down (adds one to their indices). 
     * <p>The type of each element must be equal to the type of the column it is placed in.
     * If the underlying DataFrame implementation doesn't support null values, then passing
     * an array with null values will result in a {@link DataFrameException}.
     * Make sure the number of provided row items is equal to the number
     * of columns in this DataFrame
     * 
     * @param index The index at which the specified row is to be inserted
     * @param row The row items to be inserted
     */
    public void insertRowAt(int index, Object... row);

    /**
     * Inserts the provided row into this DataFrame at the specified index.
     * Shifts all rows currently at that position and any subsequent
     * rows down (adds one to their indices).
     * 
     * <p>The provided row is a custom object implementing the {@link Row} interface.<br>
     * All columns must be labeled by the time this method is called.<br>
     * The class implementing <code>Row</code> must properly annotate
     * all members representing row items.<br>
     * If the underlying DataFrame implementation doesn't support null values,
     * then passing a row with fields that consist of null values
     * will result in a {@link DataFrameException}.
     * 
     * @param index The index at which the specified row is to be inserted
     * @param row The row to be inserted
     */
    public void insertRowAt(int index, Row row);

    /**
     * Removes the row at the specified index
     * 
     * @param index The index of the row to be removed
     */
    public void removeRow(int index);

    /**
     * Removes all rows from (inclusive) the specified index
     * to (exclusive) the specified index
     * 
     * @param from The index from which all rows should be removed (inclusive)
     * @param to The index to which all rows should be removed (exclusive)
     */
    public void removeRows(int from, int to);

    /**
     * Adds the provided column to this DataFrame. If the underlying DataFrame
     * implementation doesn't support null values, then the size of the provided
     * column must match the size of the already existing columns. If the underlying
     * DataFrame implementation supports null values and has a deviating size or the
     * provided column has a deviating size, then the DataFrame will refill all
     * missing entries with null values to match the largest column.
     * 
     * <p>If the added column was labeled during its construction, then it will be 
     * referenceable by its name.
     * 
     * @param col The column to add to the DataFrame
     */
    public void addColumn(Column col);

    /**
     * Adds the provided column to this DataFrame and assigns it the specified name.
     * If the underlying DataFrame implementation doesn't support null values, then
     * the size of the provided column must match the size of the already existing columns.
     * If the underlying DataFrame implementation supports null values and has a deviating
     * size or the provided column has a deviating size, then the DataFrame will refill
     * all missing entries with null values to match the largest column.
     * 
     * <p>If the added column was labeled during its construction, then the
     * label will get overridden by the specified name.
     * 
     * <p>Please note that no checks will be performed regarding duplicate column names.
     * Assigning more than one column the same name will result in undefined behaviour
     * 
     * @param colName The name of the column to be added
     * @param col The column to be added
     */
    public void addColumn(String colName, Column col);

    /**
     * Removes the column at the specified index and its name (if set) from this DataFrame
     * 
     * @param col The index of the column to remove
     */
    public void removeColumn(int col);

    /**
     * Removes the column with the specified name from this DataFrame
     * 
     * @param colName The name of the column to remove
     */
    public void removeColumn(String colName);

    /**
     * Inserts the provided column at the specified index to this DataFrame. Shifts all 
     * columns currently at that position and any subsequent columns to the right 
     * (adds one to their indices).
     * 
     * <p>If the underlying DataFrame implementation doesn't support null values, then the 
     * size of the provided column must match the size of the already existing columns. 
     * If the underlying DataFrame implementation supports null values and has a deviating
     * size or the provided column has a deviating size, then the DataFrame will refill all
     * missing entries with null values to match the largest column
     * 
     * <p>If the inserted column was labeled during its construction, then it will be 
     * referenceable by its name.
     * 
     * @param index The index at which the specified column is to be inserted
     * @param col The column to be inserted
     */
    public void insertColumnAt(int index, Column col);

    /**
     * Inserts the provided column at the specified index to this DataFrame and
     * assigns it the specified name. Shifts all columns currently at that position and
     * any subsequent columns to the right (adds one to their indices).
     * 
     * <p>If the underlying DataFrame implementation doesn't support null values,
     * then the size of the provided column must match the size of the already
     * existing columns. If the underlying DataFrame implementation supports null values
     * and has a deviating size or the provided column has a deviating size, then the
     * DataFrame will refill all missing entries with null
     * values to match the largest column.
     * 
     * <p>If the inserted column was labeled during its construction, then the
     * label will get overridden by the specified name.
     * 
     * <p>Please note that no checks will be performed regarding duplicate column names.
     * Assigning more than one column the same name will result in undefined behaviour
     * 
     * @param index The index at which the specified column is to be inserted
     * @param colName The name of the column to be inserted
     * @param col The column to be inserted
     */
    public void insertColumnAt(int index, String colName, Column col);

    /**
     * Indicates the number of columns this DataFrame currently holds
     * 
     * @return The number of columns of this DataFrame
     */
    public int columns();

    /**
     * Indicates the capacity of each column within this DataFrame. The Capacity
     * is the number of entries any given column can hold without the need of resizing.
     * Therefore this method is different from <code>rows()</code> because
     * <code>capacity()</code> also indicates the allocated space of the 
     * underlying array of each {@link Column}.
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
     * Any changes to that column are reflected in the DataFrame and vice-versa
     * 
     * @param col The index of the Column to get
     * @return The Column at the specified index
     */
    public Column getColumnAt(int col);

    /**
     * Gets a reference of the {@link Column} instance with the specified name.
     * Any changes to that column are reflected in the DataFrame, and vice-versa
     * 
     * @param colName The name of the Column to get
     * @return The Column with the specified name
     */
    public Column getColumn(String colName);

    /**
     * Sets and replaces the {@link Column} at the specified index
     * with the provided Column.
     * 
     * <p>If the specified column has a label associated with it, then that
     * label will be incorporated into the DataFrame. If the provided column
     * is not labeled, then the label of the provided column will be set to
     * the label of the replaced column
     * 
     * @param index The index of the Column to set
     * @param col The Column to set at the specified index
     */
    public void setColumnAt(int index, Column col);

    /**
     * Computes and returns the index of the first occurrence
     * that matches the specified regular expression in
     * the column at the specified index
     * 
     * @param col The index of the column to search 
     * @param regex The regular expression to search for
     * @return The index of the row which matches the given regular
     *         expression in the specified column.<br>
     *         <b>-1</b> if nothing in the column matches
     *         the given regular expression
     */
    public int indexOf(int col, String regex);

    /**
     * Computes and returns the index of the first occurrence that matches
     * the specified regular expression in the column with the specified name
     * 
     * @param colName The name of the Column to search
     * @param regex The regular expression to search for
     * @return The index of the row which matches the given regular
     *         expression in the specified column.<br>
     * 		   <b>-1</b> if nothing in the column matches
     *         the given regular expression
     */
    public int indexOf(String colName, String regex);

    /**
     * Computes and returns the index of the first occurrence that matches
     * the specified regular expression in the column at the specified index,
     * while starting to search from the given row index to the end of the DataFrame
     * 
     * @param col The index of the column to search 
     * @param startFrom The row index from which to start searching
     * @param regex The regular expression to search for
     * @return The index of the row which matches the given regular
     *         expression in the specified column.<br>
     * 		   <b>-1</b> if nothing in the column matches
     *         the given regular expression
     */
    public int indexOf(int col, int startFrom, String regex);

    /**
     * Computes and returns the index of the first occurrence that matches
     * the specified regular expression in the column with the specified name,
     * while starting to search from the given row index to the end of the DataFrame
     * 
     * @param colName The name of the Column to search
     * @param startFrom The row index from which to start searching
     * @param regex The regular expression to search for
     * @return The index of the row which matches the given regular expression
     *         in the specified column.<br>
     * 		   <b>-1</b> if nothing in the column matches
     *         the given regular expression
     */
    public int indexOf(String colName, int startFrom, String regex);

    /**
     * Computes and returns the indices of all occurrences that match
     * the specified regular expression in the column at the specified index
     * 
     * @param col The index of the column to search 
     * @param regex The regular expression  to search for
     * @return An array containing all indices in proper order of all
     *         occurrences that match the given regular expression.
     *         Returns an empty array if nothing in the column matches
     *         the given regular expression
     */
    public int[] indexOfAll(int col, String regex);

    /**
     * Computes and returns the indices of all occurrences that match
     * the specified regular expression in the column with the specified name
     * 
     * @param colName The name of the Column to search
     * @param regex The regular expression to search for
     * @return An array containing all indices in proper order of
     *         all occurrences that match the given regular expression.
     *         Returns an empty array if nothing in the column matches
     *         the given regular expression
     */
    public int[] indexOfAll(String colName, String regex);

    /**
     * Computes and returns a {@link DataFrame} containing all rows that
     * match the specified regular expression in the column at the specified index.
     * All rows in the returned DataFrame are copies of the original rows, so
     * changing values within the returned DataFrame has no effect on
     * the original DataFrame and vice-versa
     * 
     * @param col The index of the column to search 
     * @param regex The regular expression to search for
     * @return A sub-DataFrame containing all rows that match
     *         the given regular expression.<br>
     *         Returns an empty DataFrame if nothing in
     *         the column matches the given regular expression
     */
    public DataFrame filter(int col, String regex);

    /**
     * Computes and returns a {@link DataFrame} containing all rows
     * that match the specified regular expression in the column with
     * the specified name. All rows in the returned DataFrame are copies of the original
     * rows, so changing values within the returned DataFrame has no effect on the
     * original DataFrame and vice-versa
     * 
     * @param colName The name of the Column to search
     * @param regex The regular expression to search for
     * @return A sub-DataFrame containing all rows that match
     *         the given regular expression.<br>
     *         Returns an empty DataFrame if nothing in
     *         the column matches the given regular expression
     */
    public DataFrame filter(String colName, String regex);

    /**
     * Computes the average of all entries in the specified column.
     * If the underlying DataFrame implementation supports null values, then
     * null values are excluded from the computation and do not contribute to
     * the total number of entries. The average can only be computed for
     * columns containing numbers
     * 
     * @param col The index of the column to compute the average for
     * @return The average of all entries in the specified column
     */
    public double average(int col);

    /**
     * Computes the average of all entries in the specified column. If the
     * underlying DataFrame implementation supports null values, then null
     * values are excluded from the computation and do not contribute to the
     * total number of entries. The average can only be computed for columns
     * containing numbers
     * 
     * @param colName The name of the column to compute the average for
     * @return The average of all entries in the specified column
     */
    public double average(String colName);

    /**
     * Computes the minimum of all entries in the specified column.
     * If the underlying DataFrame implementation supports null values, then null
     * values are excluded from the computation. The minimum can only be computed
     * for columns containing numbers
     * 
     * @param col The index of the column to compute the minimum for
     * @return The minimum of all entries in the specified column
     */
    public double minimum(int col);

    /**
     * Computes the minimum of all entries in the specified column.
     * If the underlying DataFrame implementation supports null values, then
     * null values are excluded from the computation. 
     * The minimum can only be computed for columns containing numbers
     * 
     * @param colName The name of the column to compute the minimum for
     * @return The minimum of all entries in the specified column
     */
    public double minimum(String colName);

    /**
     * Computes the maximum of all entries in the specified column.
     * If the underlying DataFrame implementation supports null values, then null
     * values are excluded from the computation. 
     * The maximum can only be computed for columns containing numbers
     * 
     * @param col The index of the column to compute the maximum for
     * @return The maximum of all entries in the specified column
     */
    public double maximum(int col);

    /**
     * Computes the maximum of all entries in the specified column.
     * If the underlying DataFrame implementation supports null values, then
     * null values are excluded from the computation. 
     * The maximum can only be computed for columns containing numbers
     * 
     * @param colName The name of the column to compute the maximum for
     * @return The maximum of all entries in the specified column
     */
    public double maximum(String colName);

    /**
     * Sorts the entire DataFrame according to the values in the specified column
     * 
     * @param col The index of the column to sort the DataFrame by
     */
    public void sortBy(int col);

    /**
     * Sorts the entire DataFrame according to the values in the specified column
     * 
     * @param colName The name of the column to sort the DataFrame by
     */
    public void sortBy(String colName);

    /**
     * Returns this DataFrame as an array of Objects. The first
     * dimension contains the columns of the DataFrame and the second dimension
     * contains the entries of each column (i.e. rows). The returned array is
     * not backed by the DataFrame, so changing entries in the array has
     * no effect on the DataFrame, and vice-versa
     * 
     * @return An array of Objects representing this DataFrame
     */
    public Object[][] asArray();

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
    public Object clone();

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
     * @param obj The reference DataFrame with which to compare
     * @return True if this DataFrame is equal to the obj argument, false otherwise
     * @see java.lang.Object#equals(Object)
     */
    public boolean equals(Object obj);

    /**
     * Returns an iterator over all Columns in this DataFrame
     * 
     * @return An Iterator
     * @see java.lang.Iterable#iterator()
     * 
     */
    public Iterator<Column> iterator();

    /**
     * Creates and returns a copy of the given {@link DataFrame}
     * 
     * @param df The DataFrame instance to copy
     * @return A copy of the specified DataFrame
     */
    public static DataFrame copyOf(final DataFrame df){
        return DataFrameUtils.copyOf(df);
    }

    /**
     * Merges all given {@link DataFrame} instances into one DataFrame
     * of the same type. All DataFrames must be of equal size. All columns
     * are added to the returned DataFrame in the order of the arguments
     * passed to this method. Only passing one DataFrame to this method will
     * simply return that instance.
     * 
     * <p>All columns of the returned DataFrame are backed by their origin,
     * which means that changes to the original DataFrame are reflected in
     * the merged DataFrame, and vice-versa. If that is not what you want,
     * simply pass a clone (copy) of each DataFrame argument to this method.
     * 
     * <p>Example:<br> 
     * <code>
     * DataFrame merged = DataFrame.merge(DataFrame.copyOf(df1), DataFrame.copyOf(df2));
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
     * {@link NullableDataFrame} or vice-versa.<br>
     * Converting a DefaultDataFrame to a NullableDataFrame will not change any internal
     * values, except that now you can add/insert null values to it.<br>
     * Converting a NullableDataFrame to a DefaultDataFrame will convert all null
     * occurrences to the primitive defaults according to the column they are located.
     * 
     * <p>Example: (if 'myDf' is a DefaultDataFrame)<br>
     * <code>DataFrame df = DataFrame.convert(myDf, NullableDataFrame.class);</code>
     * 
     * @param df The DataFrame instance to convert
     * @param type The type to convert the given DataFrame to
     * @return A DataFrame converted from the type of the argument passed to this 
     * 		   method to the type specified
     */
    public static DataFrame convert(final DataFrame df, final Class<?> type){
        return DataFrameUtils.convert(df, type);
    }
}
