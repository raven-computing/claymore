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
 * A labeled Column to be used in a {@link DataFrame}.<br>
 * Each Column is a container for data of a specific type. Although it can be
 * constructed and initialized independently, a Column is always managed by a
 * DataFrame instance.
 * 
 * <p>A concrete Column can only use data of one specific type.
 * This abstract class defines methods all Columns to be used in DataFrames
 * must implement. Additonal methods may be provided by concrete 
 * implementations.<br>
 * Concrete Columns can be differentiated either by their underlying class
 * or by their unique type code. The type code is exposed as a public constant by
 * each implementing class. Additionally, the <code>typeCode()</code> member
 * method, which must be implemented by all concrete Columns, gives dynamic access
 * to the type code of a Column instance at runtime. For a more human readable
 * indication, the <code>typeName()</code> method returns a string denoting
 * the type of the elements which can be stored by the corresponding Column. The
 * type name is the same for default and nullable columns which work with the
 * same element type while the type code is always unique across all
 * Column classes.
 * 
 * <p>Generally there are two main groups of columns. Those that accept null
 * values, which must also extend {@link NullableColumn}, and those that do not,
 * which may directly extend the <code>Column</code> abstract class.
 * 
 * <p>Each Column can have a distinct label associated with it. That label
 * represents the name of that Column by which it can be referenced and accessed
 * when it is being used inside a DataFrame.
 * 
 * <p>Even though users can get and set values inside Columns directly through
 * the <code>getValue()</code> and <code>setValue()</code> methods defined by
 * this abstract class, it is generally recommended to always perform operations
 * regarding Columns by using the appropriate methods of the DataFrame that
 * the Column is part of. With the exception of Column construction, generally,
 * working with Column instances directly is regarded as more lower-level
 * compared to using the public DataFrame API to manipulate Columns. As a
 * consequence, concrete Column implementations can throw exceptions other
 * than <code>DataFrameExceptions</code>, for example
 * <code>IllegalArgumentExceptions</code> when an invalid argument is passed
 * to a method of a Column.
 * 
 * <p>Columns do not distinguish between their size and their capacity.
 * The row count size is always managed by a DataFrame and any required
 * resizing is also explicitly precipitated by it. Therefore, the value
 * returned by the <code>capacity()</code> method always indicates the
 * true length of the array used internally to store the Column values.
 * However, this may include any buffered space allocated by a DataFrame in
 * order make its operations more efficient. This has to be taken into account
 * when a user works with data in Columns directly.
 * 
 * <p>This class provides various static methods to construct concrete
 * Column instances.
 * 
 * <p>Every Column is {@link Cloneable}
 * 
 * @author Phil Gaiser
 * @see ByteColumn
 * @see ShortColumn
 * @see IntColumn
 * @see LongColumn
 * @see FloatColumn
 * @see DoubleColumn
 * @see StringColumn
 * @see CharColumn
 * @see BooleanColumn
 * @see BinaryColumn
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
public abstract class Column implements Cloneable {

    /** The label of the column **/
    protected String name;

    /**
     * Gets the value at the specified index
     * 
     * @param index The index of the value to get
     * @return The value at the specified index as an Object
     * @throws ArrayIndexOutOfBoundsException If the specified index is out of bounds
     */
    public abstract Object getValue(int index);

    /**
     * Sets the value at the specified index
     * 
     * @param index The index of the value to set
     * @param value The value to set at the specified position
     * @throws ArrayIndexOutOfBoundsException If the specified index is out of bounds
     * @throws ClassCastException If the Object provided cannot be cast to the type 
     * 		   this Column object can hold
     */
    public abstract void setValue(int index, Object value);

    /**
     * Returns the unique type code of this column
     * 
     * @return The type code of this <code>Column</code>
     */
    public abstract byte typeCode();

    /**
     * Returns the standardized name of the element types of this column
     * 
     * @return The type name of this <code>Column</code>
     */
    public abstract String typeName();

    /**
     * Returns the current capacity of this column, i.e. the length of its
     * internal array
     * 
     * @return The capacity of this column
     */
    public abstract int capacity();

    /**
     * Indicates whether this column accepts null values. This method can be used
     * instead of <br>
     * <code>column instanceof NullableColumn</code>.<br>
     * If this method returns true, then the above statement will be true as well
     * 
     * @return True if this <code>Column</code> can work with null values, false if
     *         using null values with this <code>Column</code> will result in
     *         exceptions during runtime
     */
    public abstract boolean isNullable();

    /**
     * Indicates whether this column contains numeric values
     * 
     * @return True if this column uses numeric values, false otherwise
     */
    public abstract boolean isNumeric();

    /**
     * Gets the default value for this Column.<br>
     * For {@link NullableColumn} instances default values are always null
     * 
     * @return The default value for this <code>Column</code>
     */
    public abstract Object getDefaultValue();

    /**
     * Indicates the current memory usage of this Column in bytes. The returned
     * value refers to the minimum amount of memory needed to store all values
     * plus allocated buffered space in the underlying array of this column.
     * 
     * <p>Please note that the memory usage is computed for the raw payload data
     * of the underlying column, comparable to the space needed in an uncompressed
     * serialized form. Other data e.g. column labels, internal representations,
     * encodings etc., are not taken into account. The actual memory required by the
     * underlying <code>Column</code> instance might be considerably higher
     * 
     * @return The current memory usage of this column in bytes
     */
    public abstract int memoryUsage();

    /**
     * Converts this column to a <code>Column</code> instance of the specified
     * type code. The elements of this column are not changed by this operation.
     * The returned column holds a copy of all elements converted to the type of
     * the column with the specified type code.
     * 
     * <p>Please note that any existing buffered space will be included in
     * the converted column
     * 
     * @param typeCode The type code of the <code>Column</code> to convert
     *                 this column to
     * @return A <code>Column</code> instance with the specified type code which
     *         holds all entries of this column converted to the corresponding
     *         element type
     */
    public abstract Column convertTo(byte typeCode);

    /**
     * Gets the label of this column.<br> 
     * That is the name by which this column instance can
     * be referenced when using DataFrame API calls
     * 
     * @return The name of this <code>Column</code>
     */
    public String getName(){
        return this.name;
    }

    /**
     * Returns the type of the data used by this Column.<br>
     * The returned String value denotes the type of data in this Column.
     * It is implementation dependent
     * 
     * @return The type of data used by this <code>Column</code> as a String
     */
    public String getType(){
        return memberClass().getSimpleName();
    }

    /**
     * Returns this Column as a default (non-nullable) Column. If this Column
     * supports null values, then a converted version is returned. If this
     * Column is already non-nullable, then this instance is returned.<br>
     * The element type of this Column is not changed by this operation
     * 
     * @return A <code>Column</code> guaranteed to be non-nullable
     */
    public Column asDefault(){
        if(this.isNullable()){
            if(this.typeCode() <= 18){
                return this.convertTo((byte)(typeCode() - 9));
            }else{//is binary column
                return this.convertTo((byte)(typeCode() - 1));
            }
        }else{
            return this;
        }
    }

    /**
     * Returns this Column as a nullable Column. If this Column
     * does not support null values, then a converted version is returned.
     * If this Column already supports nullable values, then this instance
     * is returned.<br>
     * The element type of this Column is not changed by this operation
     * 
     * @return A <code>Column</code> guaranteed to support null values
     */
    public Column asNullable(){
        if(!this.isNullable()){
            if(this.typeCode() <= 18){
                return this.convertTo((byte)(typeCode() + 9));
            }else{//is binary column
                return this.convertTo((byte)(typeCode() + 1));
            }
        }else{
            return this;
        }
    }

    /**
     * Creates and returns a copy of this Column
     * 
     * @return A copy of this Column
     * @see java.lang.Object#clone()
     */
    @Override
    public abstract Column clone();

    /**
     * Indicates whether this Column is equal to the specified Column.<br>
     * Please note that the capacity of both columns may be taken into account
     * when computing the equality
     *
     * @param obj The reference Column with which to compare
     * @return True if this Column is equal to the specified Column
     *          argument, false otherwise
     * @see Object#equals(Object)
     */
    public abstract boolean equals(Object obj);

    /**
     * Returns a hash code value for this Column.<br>
     * Please note that the capacity of the column may be taken into account
     * when computing the hash code
     *
     * @return A hash code value for this Column
     * @see Object#hashCode()
     */
    public abstract int hashCode();

    /**
     * Inserts the specified value at the given index into the column. Shifts all 
     * entries currently at that position and any subsequent entries down 
     * (adds one to their indices)
     * 
     * @param index The index to insert the value at
     * @param next The index of the next free position
     * @param value The value to insert
     */
    protected abstract void insertValueAt(int index, int next, Object value);

    /**
     * Removes all entries from the first index given, to the second index.
     * Shifts all entries currently next to the last position removed and any 
     * subsequent entries up
     * 
     * @param from The index from which to start removing (inclusive)
     * @param to The index to which to remove to (exclusive)
     * @param next The index of the next free position
     */
    protected abstract void remove(int from, int to, int next);

    /**
     * Returns the class of the entries this Column can hold and operate with
     * 
     * @return The class type of the entries
     */
    protected abstract Class<?> memberClass();

    /**
     * Resizes the internal array holding the column entries according to its 
     * resizing strategy
     */
    protected abstract void resize();

    /**
     * Resizes the internal array to match the given length
     * 
     * @param length The length to resize the column to
     */
    protected abstract void matchLength(int length);

    /**
     * Gets the value at the specified index. The value will be cast to
     * the parameter <code>T</code> before it is returned
     * 
     * @param <T> The type of the object to be returned
     * @param index The index of the value to get
     * @return The value at the specified index as an Object of type <code>T</code>
     * @throws ArrayIndexOutOfBoundsException If the specified index is out of bounds
     */
    protected <T> T getGenericValue(final int index){
        @SuppressWarnings("unchecked")
        final T value = (T) getValue(index);
        return value;
    }

    /**
     * Constructs a new labeled {@link ByteColumn} composed of the content of 
     * the specified byte array
     * 
     * @param name The name of the <code>ByteColumn</code> to construct.
     *             Must not be null or empty
     * @param values The entries of the <code>ByteColumn</code> to be constructed.
     *               Must not be null
     * @return A <code>ByteColumn</code> instance with the specified name and values
     * @see Column#nullable(String, Byte...)
     */
    public static ByteColumn create(final String name, final byte... values){
        return new ByteColumn(name, values);
    }

    /**
     * Constructs a new labeled {@link NullableByteColumn} composed of the content of 
     * the specified Byte array. Individual array entries may be null
     * 
     * @param name The name of the <code>NullableByteColumn</code> to construct.
     *             Must not be null or empty
     * @param values The entries of the <code>NullableByteColumn</code> to be constructed.
     *               Must not be null
     * @return A <code>NullableByteColumn</code> instance with the specified name and values
     * @see Column#create(String, byte...)
     */
    public static NullableByteColumn nullable(final String name, final Byte... values){
        return new NullableByteColumn(name, values);
    }

    /**
     * Constructs a new labeled {@link ShortColumn} composed of the content of 
     * the specified short array
     * 
     * @param name The name of the <code>ShortColumn</code> to construct.
     *             Must not be null or empty
     * @param values The entries of the <code>ShortColumn</code> to be constructed.
     *               Must not be null
     * @return A <code>ShortColumn</code> instance with the specified name and values
     * @see Column#nullable(String, Short...)
     */
    public static ShortColumn create(final String name, final short... values){
        return new ShortColumn(name, values);
    }

    /**
     * Constructs a new labeled {@link NullableShortColumn} composed of the content of 
     * the specified Short array. Individual array entries may be null
     * 
     * @param name The name of the <code>NullableShortColumn</code> to construct.
     *             Must not be null or empty
     * @param values The entries of the <code>NullableShortColumn</code> to be constructed.
     *               Must not be null
     * @return A <code>NullableShortColumn</code> instance with the specified name and values
     * @see Column#create(String, short...)
     */
    public static NullableShortColumn nullable(final String name, final Short... values){
        return new NullableShortColumn(name, values);
    }

    /**
     * Constructs a new labeled {@link IntColumn} composed of the content of 
     * the specified int array
     * 
     * @param name The name of the <code>IntColumn</code> to construct.
     *             Must not be null or empty
     * @param values The entries of the <code>IntColumn</code> to be constructed.
     *               Must not be null
     * @return A <code>IntColumn</code> instance with the specified name and values
     * @see Column#nullable(String, Integer...)
     */
    public static IntColumn create(final String name, final int... values){
        return new IntColumn(name, values);
    }

    /**
     * Constructs a new labeled {@link NullableIntColumn} composed of the content of 
     * the specified Integer array. Individual array entries may be null
     * 
     * @param name The name of the <code>NullableIntColumn</code> to construct.
     *             Must not be null or empty
     * @param values The entries of the <code>NullableIntColumn</code> to be constructed.
     *               Must not be null
     * @return A <code>NullableIntColumn</code> instance with the specified name and values
     * @see Column#create(String, int...)
     */
    public static NullableIntColumn nullable(final String name, final Integer... values){
        return new NullableIntColumn(name, values);
    }

    /**
     * Constructs a new labeled {@link LongColumn} composed of the content of 
     * the specified long array
     * 
     * @param name The name of the <code>LongColumn</code> to construct.
     *             Must not be null or empty
     * @param values The entries of the <code>LongColumn</code> to be constructed.
     *               Must not be null
     * @return A <code>LongColumn</code> instance with the specified name and values
     * @see Column#nullable(String, Long...)
     */
    public static LongColumn create(final String name, final long... values){
        return new LongColumn(name, values);
    }

    /**
     * Constructs a new labeled {@link NullableLongColumn} composed of the content of 
     * the specified Long array. Individual array entries may be null
     * 
     * @param name The name of the <code>NullableLongColumn</code> to construct.
     *             Must not be null or empty
     * @param values The entries of the <code>NullableLongColumn</code> to be constructed.
     *               Must not be null
     * @return A <code>NullableLongColumn</code> instance with the specified name and values
     * @see Column#create(String, long...)
     */
    public static NullableLongColumn nullable(final String name, final Long... values){
        return new NullableLongColumn(name, values);
    }

    /**
     * Constructs a new labeled {@link StringColumn} composed of the content of 
     * the specified String array
     * 
     * @param name The name of the <code>StringColumn</code> to construct.
     *             Must not be null or empty
     * @param values The entries of the <code>StringColumn</code> to be constructed.
     *               Must not be null
     * @return A <code>StringColumn</code> instance with the specified name and values
     * @see Column#nullable(String, String...)
     */
    public static StringColumn create(final String name, final String... values){
        return new StringColumn(name, values);
    }

    /**
     * Constructs a new labeled {@link NullableStringColumn} composed of the content of 
     * the specified String array. Individual array entries may be null or empty
     * 
     * @param name The name of the <code>NullableStringColumn</code> to construct.
     *             Must not be null or empty
     * @param values The entries of the <code>NullableStringColumn</code> to be constructed.
     *               Must not be null
     * @return A <code>NullableStringColumn</code> instance with the specified name and values
     * @see Column#create(String, String...)
     */
    public static NullableStringColumn nullable(final String name, final String... values){
        return new NullableStringColumn(name, values);
    }

    /**
     * Constructs a new labeled {@link FloatColumn} composed of the content of 
     * the specified float array
     * 
     * @param name The name of the <code>FloatColumn</code> to construct.
     *             Must not be null or empty
     * @param values The entries of the <code>FloatColumn</code> to be constructed.
     *               Must not be null
     * @return A <code>FloatColumn</code> instance with the specified name and values
     * @see Column#nullable(String, Float...)
     */
    public static FloatColumn create(final String name, final float... values){
        return new FloatColumn(name, values);
    }

    /**
     * Constructs a new labeled {@link NullableFloatColumn} composed of the content of 
     * the specified Float array. Individual array entries may be null
     * 
     * @param name The name of the <code>NullableFloatColumn</code> to construct.
     *             Must not be null or empty
     * @param values The entries of the <code>NullableFloatColumn</code> to be constructed.
     *               Must not be null
     * @return A <code>NullableFloatColumn</code> instance with the specified name and values
     * @see Column#create(String, float...)
     */
    public static NullableFloatColumn nullable(final String name, final Float... values){
        return new NullableFloatColumn(name, values);
    }

    /**
     * Constructs a new labeled {@link DoubleColumn} composed of the content of 
     * the specified double array
     * 
     * @param name The name of the <code>DoubleColumn</code> to construct.
     *             Must not be null or empty
     * @param values The entries of the <code>DoubleColumn</code> to be constructed.
     *               Must not be null
     * @return A <code>DoubleColumn</code> instance with the specified name and values
     * @see Column#nullable(String, Double...)
     */
    public static DoubleColumn create(final String name, final double... values){
        return new DoubleColumn(name, values);
    }

    /**
     * Constructs a new labeled {@link NullableDoubleColumn} composed of the content of 
     * the specified Double array. Individual array entries may be null
     * 
     * @param name The name of the <code>NullableDoubleColumn</code> to construct.
     *             Must not be null or empty
     * @param values The entries of the <code>NullableDoubleColumn</code> to be constructed.
     *               Must not be null
     * @return A <code>NullableDoubleColumn</code> instance with the specified name and values
     * @see Column#create(String, double...)
     */
    public static NullableDoubleColumn nullable(final String name, final Double... values){
        return new NullableDoubleColumn(name, values);
    }

    /**
     * Constructs a new labeled {@link CharColumn} composed of the content of 
     * the specified char array
     * 
     * @param name The name of the <code>CharColumn</code> to construct.
     *             Must not be null or empty
     * @param values The entries of the <code>CharColumn</code> to be constructed.
     *               Must not be null
     * @return A <code>CharColumn</code> instance with the specified name and values
     * @see Column#nullable(String, Character...)
     */
    public static CharColumn create(final String name, final char... values){
        return new CharColumn(name, values);
    }

    /**
     * Constructs a new labeled {@link NullableCharColumn} composed of the content of 
     * the specified Character array. Individual array entries may be null
     * 
     * @param name The name of the <code>NullableCharColumn</code> to construct.
     *             Must not be null or empty
     * @param values The entries of the <code>NullableCharColumn</code> to be constructed.
     *               Must not be null
     * @return A <code>NullableCharColumn</code> instance with the specified name and values
     * @see Column#create(String, char...)
     */
    public static NullableCharColumn nullable(final String name, final Character... values){
        return new NullableCharColumn(name, values);
    }

    /**
     * Constructs a new labeled {@link BooleanColumn} composed of the content of 
     * the specified boolean array
     * 
     * @param name The name of the <code>BooleanColumn</code> to construct.
     *             Must not be null or empty
     * @param values The entries of the <code>BooleanColumn</code> to be constructed.
     *               Must not be null
     * @return A <code>BooleanColumn</code> instance with the specified name and values
     * @see Column#nullable(String, Boolean...)
     */
    public static BooleanColumn create(final String name, final boolean... values){
        return new BooleanColumn(name, values);
    }

    /**
     * Constructs a new labeled {@link NullableBooleanColumn} composed of the content of 
     * the specified Boolean array. Individual array entries may be null
     * 
     * @param name The name of the <code>NullableBooleanColumn</code> to construct.
     *             Must not be null or empty
     * @param values The entries of the <code>NullableBooleanColumn</code> to be constructed.
     *               Must not be null
     * @return A <code>NullableBooleanColumn</code> instance with the specified name and values
     * @see Column#create(String, boolean...)
     */
    public static NullableBooleanColumn nullable(final String name, final Boolean... values){
        return new NullableBooleanColumn(name, values);
    }

    /**
     * Constructs a new labeled {@link BinaryColumn} composed of the content of 
     * the specified byte array
     * 
     * @param name The name of the <code>BinaryColumn</code> to construct.
     *             Must not be null or empty
     * @param values The entries of the <code>BinaryColumn</code> to be constructed.
     *               Must not be null
     * @return A <code>BinaryColumn</code> instance with the specified name and values
     * @see Column#nullable(String, byte[][])
     */
    public static BinaryColumn create(final String name, final byte[]... values){
        return new BinaryColumn(name, values);
    }

    /**
     * Constructs a new labeled {@link NullableBinaryColumn} composed of the content of 
     * the specified byte array. Individual array entries may be null
     * 
     * @param name The name of the <code>NullableBinaryColumn</code> to construct.
     *             Must not be null or empty
     * @param values The entries of the <code>NullableBinaryColumn</code> to be constructed.
     *               Must not be null
     * @return A <code>NullableBinaryColumn</code> instance with the specified name and values
     * @see Column#create(String, byte[][])
     */
    public static NullableBinaryColumn nullable(final String name, final byte[]... values){
        return new NullableBinaryColumn(name, values);
    }

    /**
     * Creates a new <code>Column</code> instance which has the same type and name as
     * the specified Column but is otherwise empty
     * 
     * @param col The <code>Column</code> to structurally copy
     * @return An empty <code>Column</code> with the type and name of the
     *         specified Column, or null if the specified Column is null
     * @see Column#ofType(byte)
     */
    public static Column like(final Column col){
        return Column.like(col, 0);
    }

    /**
     * Creates a new <code>Column</code> instance which has the same type and name as
     * the specified Column and the specified length. The returned Column will be
     * initialized with default values
     * 
     * @param col The <code>Column</code> to structurally copy
     * @param length The length of the <code>Column</code> to return
     * @return A <code>Column</code> with the type and name of the
     *         specified Column and the specified length, or null
     *         if the specified Column is null
     * @see Column#ofType(byte, int)
     */
    public static Column like(final Column col, final int length){
        if(col == null){
            return null;
        }
        final Column c = Column.ofType(col.typeCode(), length);
        c.name = col.name;
        return c;
    }

    /**
     * Creates a new <code>Column</code> instance with the specified type code.<br>
     * This method can be used to construct an empty column which has the same type
     * as another column but is not a copy of that column's content.<br>
     * <pre>
     * Column col = Column.ofType(someOtherCol.typeCode());
     * </pre>
     * The above statement will construct a column which has the same type as 
     * <i>someOtherCol</i> but is completely empty regardless of the content of 
     * the other column
     * 
     * @param typeCode The unique type code of the <code>Column</code> to create
     * @return An empty <code>Column</code> of the specified type
     * @see Column#ofType(byte, int)
     * @see Column#like(Column)
     */
    public static Column ofType(final byte typeCode){
        return ofType(typeCode, 0);
    }

    /**
     * Creates a new <code>Column</code> instance with the specified type code
     * and initial length.<br>
     * This method can be used to construct a column which has the same type
     * as another column but is not a copy of that column's content and
     * has the specified length. All values in the returned column are initialized
     * with the default values of the respective type<br>
     * <pre>
     * Column col = Column.ofType(someOtherCol.typeCode(), 42);
     * </pre>
     * The above statement will construct a column which has the same type as
     * <i>someOtherCol</i> and a length of 42.
     * 
     * @param typeCode The unique type code of the <code>Column</code> to create
     * @param length The initial length of the <code>Column</code> to create
     * @return A <code>Column</code> of the specified type and length or null
     *         if the specified type code is unknown
     * @see Column#like(Column, int)
     */
    public static Column ofType(final byte typeCode, final int length){
        switch(typeCode){
        case ByteColumn.TYPE_CODE:
            return new ByteColumn(length);
        case ShortColumn.TYPE_CODE:
            return new ShortColumn(length);
        case IntColumn.TYPE_CODE:
            return new IntColumn(length);
        case LongColumn.TYPE_CODE:
            return new LongColumn(length);
        case StringColumn.TYPE_CODE:
            return new StringColumn(length);
        case FloatColumn.TYPE_CODE:
            return new FloatColumn(length);
        case DoubleColumn.TYPE_CODE:
            return new DoubleColumn(length);
        case CharColumn.TYPE_CODE:
            return new CharColumn(length);
        case BooleanColumn.TYPE_CODE:
            return new BooleanColumn(length);
        case BinaryColumn.TYPE_CODE:
            return new BinaryColumn(length);
        case NullableByteColumn.TYPE_CODE:
            return new NullableByteColumn(length);
        case NullableShortColumn.TYPE_CODE:
            return new NullableShortColumn(length);
        case NullableIntColumn.TYPE_CODE:
            return new NullableIntColumn(length);
        case NullableLongColumn.TYPE_CODE:
            return new NullableLongColumn(length);
        case NullableStringColumn.TYPE_CODE:
            return new NullableStringColumn(length);
        case NullableFloatColumn.TYPE_CODE:
            return new NullableFloatColumn(length);
        case NullableDoubleColumn.TYPE_CODE:
            return new NullableDoubleColumn(length);
        case NullableCharColumn.TYPE_CODE:
            return new NullableCharColumn(length);
        case NullableBooleanColumn.TYPE_CODE:
            return new NullableBooleanColumn(length);
        case NullableBinaryColumn.TYPE_CODE:
            return new NullableBinaryColumn(length);
        default:
            return null;
        }
    }
}
