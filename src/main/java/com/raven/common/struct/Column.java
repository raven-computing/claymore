/* 
 * Copyright (C) 2019 Raven Computing
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
 * Each column is a container for data of a specific type. Although it can be
 * constructed and initialized independently, a column is always managed by a
 * DataFrame instance.
 * 
 * <p>A concrete Column can only use data of one specific type.
 * This abstract class defines methods all columns to be used in DataFrames
 * must implement. Additonal methods may be provided by concrete 
 * implementations.<br>
 * Concrete columns can be differentiated either by their underlying class
 * or by their unique type code. The type code is exposed as a public constant by
 * each implementing class. Additionally, the <code>typeCode()</code> member
 * method, which must be implemented by all concrete columns, gives dynamic access
 * to the type code of a Column instance at runtime. 
 * 
 * <p>Generally there are two main groups of columns. Those that accept null
 * values, which must also extend {@link NullableColumn}, and those that do not,
 * which may directly extend the <code>Column</code> abstract class.
 * 
 * <p>Each column can have a distinct label associated with it. That label
 * represents the name of that column by which it can be referenced and accessed
 * when it is being used inside a DataFrame.
 * 
 * <p>Every Column is {@link Cloneable}
 * 
 * @author Phil Gaiser
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
	public abstract Object getValueAt(int index);
	
	/**
	 * Sets the value at the specified index
	 * 
	 * @param index The index of the value to set
	 * @param value The value to set at the specified position
	 * @throws ArrayIndexOutOfBoundsException If the specified index is out of bounds
	 * @throws ClassCastException If the Object provided cannot be cast to the type 
	 * 		   this Column object can hold
	 */
	public abstract void setValueAt(int index, Object value);
	
	/**
	 * Returns the unique type code of this column
	 * 
	 * @return The type code of this <code>Column</code>
	 */
	public abstract byte typeCode();
	
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
	 * Creates and returns a copy of this Column
	 * 
	 * @return A copy of this Column
	 * @see java.lang.Object#clone()
	 */
	@Override
	public abstract Object clone();
	
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
	 * Returns the current capacity of this column, i.e. the length of its
	 * internal array
	 * 
	 * @return The capacity of this column
	 */
	protected abstract int capacity();
	
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
	 */
	public static final Column ofType(final byte typeCode){
		switch(typeCode){
		case ByteColumn.TYPE_CODE:
			return new ByteColumn();
		case ShortColumn.TYPE_CODE:
			return new ShortColumn();
		case IntColumn.TYPE_CODE:
			return new IntColumn();
		case LongColumn.TYPE_CODE:
			return new LongColumn();
		case StringColumn.TYPE_CODE:
			return new StringColumn();
		case FloatColumn.TYPE_CODE:
			return new FloatColumn();
		case DoubleColumn.TYPE_CODE:
			return new DoubleColumn();
		case CharColumn.TYPE_CODE:
			return new CharColumn();
		case BooleanColumn.TYPE_CODE:
			return new BooleanColumn();
		case NullableByteColumn.TYPE_CODE:
			return new NullableByteColumn();
		case NullableShortColumn.TYPE_CODE:
			return new NullableShortColumn();
		case NullableIntColumn.TYPE_CODE:
			return new NullableIntColumn();
		case NullableLongColumn.TYPE_CODE:
			return new NullableLongColumn();
		case NullableStringColumn.TYPE_CODE:
			return new NullableStringColumn();
		case NullableFloatColumn.TYPE_CODE:
			return new NullableFloatColumn();
		case NullableDoubleColumn.TYPE_CODE:
			return new NullableDoubleColumn();
		case NullableCharColumn.TYPE_CODE:
			return new NullableCharColumn();
		case NullableBooleanColumn.TYPE_CODE:
			return new NullableBooleanColumn();
		default:
			return null;
		}
	}

}
