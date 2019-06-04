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

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * DataFrame implementation using primitive wrapper objects as the underlying 
 * data structure.<br>This implementation does permit null values.<br>
 * Columns which support the use of null values must be used with this implementation.
 * 
 * <p>As described in the {@link DataFrame} interface, most methods of this class can
 * throw a {@link DataFrameException} at runtime if any argument passed to it is invalid,
 * for example an out of bounds index, or if that operation would result in an 
 * incoherent/invalid state of that DataFrame.
 * 
 * <p>A NullableDataFrame is {@link Cloneable}, {@link Iterable}
 * 
 * <p>This implementation is NOT thread-safe.
 * 
 * @author Phil Gaiser
 * @see DefaultDataFrame
 * @since 1.0.0
 *
 */
public class NullableDataFrame implements DataFrame {
	
	private Column[] columns;
	private Map<String, Integer> names;
	private int next;

	/**
	 * Constructs an empty <code>NullableDataFrame</code> without any columns set.
	 */
	public NullableDataFrame(){
		this.next = -1;
	}
	
	/**
	 * Constructs a new <code>NullableDataFrame</code> with the specified columns.
	 * 
	 * <p>If a column was labeled during its construction, that column will be
	 * referenceable by that name. All columns which have not been labeled during their
	 * construction will have no name assigned to them.<br>
	 * The order of the columns within the constructed DataFrame is defined by the order
	 * of the arguments passed to this constructor. All columns must have the same size.
	 * <p>This implementation must use {@link Column} instances which permit null values
	 * 
	 * @param columns The Column instances comprising the constructed DataFrame 
	 */
	public NullableDataFrame(final Column... columns){
		assignColumns(columns);
	}
	
	/**
	 * Constructs a new <code>NullableDataFrame</code> with the specified columns and assigns
	 * them the specified names. The number of columns must be equal to the number of 
	 * names.
	 * 
	 * <p>If a column was labeled during its construction, that label is overridden
	 * by the corresponding label of the <i>names</i> argument.
	 * The order of the columns within the constructed DataFrame is defined by the order
	 * of the arguments passed to this constructor. The index of the name in the array
	 * determines to which column that name will be assigned to.<br>All columns must have
	 * the same size.
	 * 
	 * <p>This implementation must use {@link Column} instances which permit null values
	 * 
	 * @param names The names of all columns
	 * @param columns The Column instances comprising the constructed DataFrame 
	 */
	public NullableDataFrame(final String[] names, final Column... columns){
		if(names.length != columns.length){
			throw new DataFrameException("Args must have equal length");
		}
		//override any set names with the provided argument strings
		for(int i=0; i<columns.length; ++i){
			columns[i].name = names[i];
		}
		assignColumns(columns);
	}
	
	/**
	 * Constructs a new empty <code>NullableDataFrame</code> from the annotated fields in 
	 * the specified class.<br>
	 * The provided class must implement the {@link Row} interface. The type of each field 
	 * annotated with {@link RowItem} will be used to determine the type of the column for
	 * that row item. If the annotation does not specify a column name, then the identifying
	 * name of the field will be used as the name for that column.<br>
	 * Fields not carrying the <code>RowItem</code> annotation are ignored when creating the
	 * column structure.<br>
	 * Please note that the order of the constructed columns within the returned DataFrame is
	 * not necessarily the order in which the fields in the provided class are declared
	 * 
	 * @param structure The class defining a row in the NullableDataFrame to be constructed, 
	 *                  which is used to infer the column structure.
	 *                  Must implement <code>Row</code>
	 */
	public NullableDataFrame(final Class<? extends Row> structure){
		final Field[] fields = structure.getDeclaredFields();
		if(fields.length == 0){
			throw new DataFrameException(structure.getSimpleName()
					+ " class does not declare any fields");
		}
		String[] declaredNames = new String[fields.length];
		Column[] cols = new Column[fields.length];
		int i = 0;
		for(final Field field : fields){
			final RowItem item = field.getAnnotation(RowItem.class);
			if(item != null){
				String name = item.value();
				if((name == null || name.isEmpty())){
					name = field.getName();
				}
				cols[i] = inferColumnFromType(field.getType());
				declaredNames[i] = name;
				++i;
			}
		}
		if(i == 0){
			throw new DataFrameException(structure.getSimpleName()
					+ " class does not declare any annotated fields");
		}
		this.columns = new Column[i];
		for(int j=0; j<i; ++j){
			this.columns[j] = cols[j];
		}
		this.names = new HashMap<String, Integer>(16);
		for(int j=0; j<i; ++j){
			this.names.put(declaredNames[j], j);
			this.columns[j].name = declaredNames[j];
		}
		this.next = 0;
	}
	
	public Byte getByte(final int col, final int row){
		if((next == -1) || (col < 0) || (col >= columns.length)){
			throw new DataFrameException("Invalid column index: "+col);
		}
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(columns[col].typeCode() != NullableByteColumn.TYPE_CODE){
			throw new DataFrameException("Is not NullableByteColumn");
		}
		return ((NullableByteColumn)columns[col]).get(row);
	}

	public Byte getByte(final String colName, final int row){
		final int col = enforceName(colName);
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(columns[col].typeCode() != NullableByteColumn.TYPE_CODE){
			throw new DataFrameException("Is not NullableByteColumn");
		}
		return ((NullableByteColumn)columns[col]).get(row);
	}

	public Short getShort(final int col, final int row){
		if((next == -1) || (col < 0) || (col >= columns.length)){
			throw new DataFrameException("Invalid column index: "+col);
		}
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(columns[col].typeCode() != NullableShortColumn.TYPE_CODE){
			throw new DataFrameException("Is not NullableShortColumn");
		}
		return ((NullableShortColumn)columns[col]).get(row);
	}

	public Short getShort(final String colName, final int row){
		final int col = enforceName(colName);
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(columns[col].typeCode() != NullableShortColumn.TYPE_CODE){
			throw new DataFrameException("Is not NullableShortColumn");
		}
		return ((NullableShortColumn)columns[col]).get(row);
	}

	public Integer getInt(final int col, final int row){
		if((next == -1) || (col < 0) || (col >= columns.length)){
			throw new DataFrameException("Invalid column index: "+col);
		}
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(columns[col].typeCode() != NullableIntColumn.TYPE_CODE){
			throw new DataFrameException("Is not NullableIntColumn");
		}
		return ((NullableIntColumn)columns[col]).get(row);
	}

	public Integer getInt(final String colName, final int row){
		final int col = enforceName(colName);
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(columns[col].typeCode() != NullableIntColumn.TYPE_CODE){
			throw new DataFrameException("Is not NullableIntColumn");
		}
		return ((NullableIntColumn)columns[col]).get(row);
	}
	
	public Long getLong(final int col, final int row){
		if((next == -1) || (col < 0) || (col >= columns.length)){
			throw new DataFrameException("Invalid column index: "+col);
		}
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(columns[col].typeCode() != NullableLongColumn.TYPE_CODE){
			throw new DataFrameException("Is not NullableLongColumn");
		}
		return ((NullableLongColumn)columns[col]).get(row);
	}

	public Long getLong(final String colName, final int row){
		final int col = enforceName(colName);
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(columns[col].typeCode() != NullableLongColumn.TYPE_CODE){
			throw new DataFrameException("Is not NullableLongColumn");
		}
		return ((NullableLongColumn)columns[col]).get(row);
	}

	public String getString(final int col, final int row){
		if((next == -1) || (col < 0) || (col >= columns.length)){
			throw new DataFrameException("Invalid column index: "+col);
		}
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(columns[col].typeCode() != NullableStringColumn.TYPE_CODE){
			throw new DataFrameException("Is not NullableStringColumn");
		}
		return ((NullableStringColumn)columns[col]).get(row);
	}

	public String getString(final String colName, final int row){
		final int col = enforceName(colName);
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(columns[col].typeCode() != NullableStringColumn.TYPE_CODE){
			throw new DataFrameException("Is not NullableStringColumn");
		}
		return ((NullableStringColumn)columns[col]).get(row);
	}
	
	public Float getFloat(final int col, final int row){
		if((next == -1) || (col < 0) || (col >= columns.length)){
			throw new DataFrameException("Invalid column index: "+col);
		}
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(columns[col].typeCode() != NullableFloatColumn.TYPE_CODE){
			throw new DataFrameException("Is not NullableFloatColumn");
		}
		return ((NullableFloatColumn)columns[col]).get(row);
	}

	public Float getFloat(final String colName, final int row){
		final int col = enforceName(colName);
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(columns[col].typeCode() != NullableFloatColumn.TYPE_CODE){
			throw new DataFrameException("Is not NullableFloatColumn");
		}
		return ((NullableFloatColumn)columns[col]).get(row);
	}

	public Double getDouble(final int col, final int row){
		if((next == -1) || (col < 0) || (col >= columns.length)){
			throw new DataFrameException("Invalid column index: "+col);
		}
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(columns[col].typeCode() != NullableDoubleColumn.TYPE_CODE){
			throw new DataFrameException("Is not NullableDoubleColumn");
		}
		return ((NullableDoubleColumn)columns[col]).get(row);
	}

	public Double getDouble(final String colName, final int row){
		final int col = enforceName(colName);
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(columns[col].typeCode() != NullableDoubleColumn.TYPE_CODE){
			throw new DataFrameException("Is not NullableDoubleColumn");
		}
		return ((NullableDoubleColumn)columns[col]).get(row);
	}

	public Character getChar(final int col, final int row){
		if((next == -1) || (col < 0) || (col >= columns.length)){
			throw new DataFrameException("Invalid column index: "+col);
		}
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(columns[col].typeCode() != NullableCharColumn.TYPE_CODE){
			throw new DataFrameException("Is not NullableCharColumn");
		}
		return ((NullableCharColumn)columns[col]).get(row);
	}

	public Character getChar(final String colName, final int row){
		final int col = enforceName(colName);
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(columns[col].typeCode() != NullableCharColumn.TYPE_CODE){
			throw new DataFrameException("Is not NullableCharColumn");
		}
		return ((NullableCharColumn)columns[col]).get(row);
	}

	public Boolean getBoolean(final int col, final int row){
		if((next == -1) || (col < 0) || (col >= columns.length)){
			throw new DataFrameException("Invalid column index: "+col);
		}
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(columns[col].typeCode() != NullableBooleanColumn.TYPE_CODE){
			throw new DataFrameException("Is not NullableBooleanColumn");
		}
		return ((NullableBooleanColumn)columns[col]).get(row);
	}

	public Boolean getBoolean(final String colName, final int row){
		final int col = enforceName(colName);
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(columns[col].typeCode() != NullableBooleanColumn.TYPE_CODE){
			throw new DataFrameException("Is not NullableBooleanColumn");
		}
		return ((NullableBooleanColumn)columns[col]).get(row);
	}
	
	public void setByte(final int col, final int row, final Byte value){
		if((next == -1) || (col < 0) || (col >= columns.length)){
			throw new DataFrameException("Invalid column index: "+col);
		}
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(columns[col].typeCode() != NullableByteColumn.TYPE_CODE){
			throw new DataFrameException("Is not NullableByteColumn");
		}
		((NullableByteColumn)columns[col]).set(row, value);
	}

	public void setByte(final String colName, final int row, final Byte value){
		final int col = enforceName(colName);
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(columns[col].typeCode() != NullableByteColumn.TYPE_CODE){
			throw new DataFrameException("Is not NullableByteColumn");
		}
		((NullableByteColumn)columns[col]).set(row, value);
	}

	public void setShort(final int col, final int row, final Short value){
		if((next == -1) || (col < 0) || (col >= columns.length)){
			throw new DataFrameException("Invalid column index: "+col);
		}
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(columns[col].typeCode() != NullableShortColumn.TYPE_CODE){
			throw new DataFrameException("Is not NullableShortColumn");
		}
		((NullableShortColumn)columns[col]).set(row, value);
	}

	public void setShort(final String colName, final int row, final Short value){
		final int col = enforceName(colName);
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(columns[col].typeCode() != NullableShortColumn.TYPE_CODE){
			throw new DataFrameException("Is not NullableShortColumn");
		}
		((NullableShortColumn)columns[col]).set(row, value);
	}

	public void setInt(final int col, final int row, final Integer value){
		if((next == -1) || (col < 0) || (col >= columns.length)){
			throw new DataFrameException("Invalid column index: "+col);
		}
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(columns[col].typeCode() != NullableIntColumn.TYPE_CODE){
			throw new DataFrameException("Is not NullableIntColumn");
		}
		((NullableIntColumn)columns[col]).set(row, value);
	}

	public void setInt(final String colName, final int row, final Integer value){
		final int col = enforceName(colName);
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(columns[col].typeCode() != NullableIntColumn.TYPE_CODE){
			throw new DataFrameException("Is not NullableIntColumn");
		}
		((NullableIntColumn)columns[col]).set(row, value);
	}
	
	public void setLong(final int col, final int row, final Long value){
		if((next == -1) || (col < 0) || (col >= columns.length)){
			throw new DataFrameException("Invalid column index: "+col);
		}
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(columns[col].typeCode() != NullableLongColumn.TYPE_CODE){
			throw new DataFrameException("Is not NullableLongColumn");
		}
		((NullableLongColumn)columns[col]).set(row, value);
	}

	public void setLong(final String colName, final int row, final Long value){
		final int col = enforceName(colName);
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(columns[col].typeCode() != NullableLongColumn.TYPE_CODE){
			throw new DataFrameException("Is not NullableLongColumn");
		}
		((NullableLongColumn)columns[col]).set(row, value);
	}

	public void setString(final int col, final int row, final String value){
		if((next == -1) || (col < 0) || (col >= columns.length)){
			throw new DataFrameException("Invalid column index: "+col);
		}
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(columns[col].typeCode() != NullableStringColumn.TYPE_CODE){
			throw new DataFrameException("Is not NullableStringColumn");
		}
		((NullableStringColumn)columns[col]).set(row, value);
	}

	public void setString(final String colName, final int row, final String value){
		final int col = enforceName(colName);
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(columns[col].typeCode() != NullableStringColumn.TYPE_CODE){
			throw new DataFrameException("Is not NullableStringColumn");
		}
		((NullableStringColumn)columns[col]).set(row, value);
	}
	
	public void setFloat(final int col, final int row, final Float value){
		if((next == -1) || (col < 0) || (col >= columns.length)){
			throw new DataFrameException("Invalid column index: "+col);
		}
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(columns[col].typeCode() != NullableFloatColumn.TYPE_CODE){
			throw new DataFrameException("Is not NullableFloatColumn");
		}
		((NullableFloatColumn)columns[col]).set(row, value);
	}

	public void setFloat(final String colName, final int row, final Float value){
		final int col = enforceName(colName);
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(columns[col].typeCode() != NullableFloatColumn.TYPE_CODE){
			throw new DataFrameException("Is not NullableFloatColumn");
		}
		((NullableFloatColumn)columns[col]).set(row, value);
	}

	public void setDouble(final int col, final int row, final Double value){
		if((next == -1) || (col < 0) || (col >= columns.length)){
			throw new DataFrameException("Invalid column index: "+col);
		}
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(columns[col].typeCode() != NullableDoubleColumn.TYPE_CODE){
			throw new DataFrameException("Is not NullableDoubleColumn");
		}
		((NullableDoubleColumn)columns[col]).set(row, value);
	}

	public void setDouble(final String colName, final int row, final Double value){
		final int col = enforceName(colName);
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(columns[col].typeCode() != NullableDoubleColumn.TYPE_CODE){
			throw new DataFrameException("Is not NullableDoubleColumn");
		}
		((NullableDoubleColumn)columns[col]).set(row, value);
	}

	public void setChar(final int col, final int row, final Character value){
		if((next == -1) || (col < 0) || (col >= columns.length)){
			throw new DataFrameException("Invalid column index: "+col);
		}
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(columns[col].typeCode() != NullableCharColumn.TYPE_CODE){
			throw new DataFrameException("Is not NullableCharColumn");
		}
		((NullableCharColumn)columns[col]).set(row, value);
	}

	public void setChar(final String colName, final int row, final Character value){
		final int col = enforceName(colName);
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(columns[col].typeCode() != NullableCharColumn.TYPE_CODE){
			throw new DataFrameException("Is not NullableCharColumn");
		}
		((NullableCharColumn)columns[col]).set(row, value);
	}

	public void setBoolean(final int col, final int row, final Boolean value){
		if((next == -1) || (col < 0) || (col >= columns.length)){
			throw new DataFrameException("Invalid column index: "+col);
		}
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(columns[col].typeCode() != NullableBooleanColumn.TYPE_CODE){
			throw new DataFrameException("Is not NullableBooleanColumn");
		}
		((NullableBooleanColumn)columns[col]).set(row, value);
	}

	public void setBoolean(final String colName, final int row, final Boolean value){
		final int col = enforceName(colName);
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(columns[col].typeCode() != NullableBooleanColumn.TYPE_CODE){
			throw new DataFrameException("Is not NullableBooleanColumn");
		}
		((NullableBooleanColumn)columns[col]).set(row, value);
	}

	public String[] getColumnNames(){
		if(names != null){
			final String[] names = new String[columns.length];
			for(int i=0; i<columns.length; ++i){
				final String s = this.columns[i].name;
				names[i] = ((s == null) ? String.valueOf(i) : s);
			}
			return names;
		}
		return null;
	}

	public String getColumnName(final int col){
		if((next == -1) || (col < 0) || (col >= columns.length)){
			throw new DataFrameException("Invalid column index: "+col);
		}
		if(names != null){
			return this.columns[col].name;
		}
		return null;
	}

	public int getColumnIndex(final String colName){
		return enforceName(colName);
	}

	public void setColumnNames(String... names){
		if((names == null) || (names.length == 0)){
			throw new DataFrameException("Arg must not be null or empty");
		}
		if((next == -1) || (names.length != columns.length)){
			throw new DataFrameException("Length does not match number of columns: "
					+names.length);
		}
		this.names = new HashMap<String, Integer>(16);
		for(int i=0; i<names.length; ++i){
			if((names[i] == null) || (names[i].isEmpty())){
				throw new DataFrameException("Column name must not be null or empty");
			}
			this.names.put(names[i], i);
			this.columns[i].name = names[i];
		}
	}

	public boolean setColumnName(final int col, final String name){
		if((next == -1) || (col < 0) || (col >= columns.length)){
			throw new DataFrameException("Invalid column index: "+col);
		}
		if((name == null) || (name.isEmpty())){
			throw new DataFrameException("Column name must not be null or empty");
		}
		if(names == null){
			this.names = new HashMap<String, Integer>(16);
		}
		boolean overridden = false;
		final String current = this.columns[col].name;
		Integer index = null;
		if(current != null){
			index = this.names.get(current);	
		}
		if((index != null) && (index == col)){
			this.names.remove(current);
			overridden = true;
		}
		this.names.put(name, col);
		this.columns[col].name = name;
		return overridden;
	}

	public void removeColumnNames(){
		this.names = null;
		for(int i=0; i<columns.length; ++i){
			this.columns[i].name = null;
		}
	}

	public boolean hasColumnNames(){
		return (this.names != null);
	}

	public Object[] getRowAt(final int index){
		if((index >= next) || (index < 0)){
			throw new DataFrameException("Invalid row index: "+index);
		}
		Object[] row = new Object[columns.length];
		for(int i=0; i<columns.length; ++i){
			row[i] = columns[i].getValueAt(index);
		}
		return row;
	}
	
	public <T extends Row> T getRowAt(final int index, final Class<T> classOfT){
		if(!hasColumnNames()){
			throw new DataFrameException("Columns must be labeled in order "
					+ "to use row annotation feature");
		}
		if((index >= next) || (index < 0)){
			throw new DataFrameException("Invalid row index: "+index);
		}
		T row = null;
		try{
			row = classOfT.newInstance();
			for(final Field field : classOfT.getDeclaredFields()){
				final RowItem item = field.getAnnotation(RowItem.class);
				if(item != null){
					String name = item.value();
					if((name == null || name.isEmpty())){
						name = field.getName();
					}
					final int i = enforceName(name);
					field.setAccessible(true);
					field.set(row, columns[i].getValueAt(index));
				}
			}
		}catch(InstantiationException ex){
			throw new DataFrameException(classOfT.getSimpleName() 
					+ " does not declare a default no-args constructor");
		}catch(IllegalAccessException ex){
			throw new DataFrameException(ex.getMessage());
		}
		return row;
	}

	public void setRowAt(final int index, final Object[] row){
		if((index >= next) || (index < 0)){
			throw new DataFrameException("Invalid row index: "+index);
		}
		enforceTypes(row);
		for(int i=0; i<columns.length; ++i){
			columns[i].setValueAt(index, row[i]);
		}
	}
	
	public void setRowAt(final int index, final Row row){
		if(!hasColumnNames()){
			throw new DataFrameException("Columns must be labeled in order "
					+ "to use row annotation feature");
		}
		if((index >= next) || (index < 0)){
			throw new DataFrameException("Invalid row index: "+index);
		}
		final Object[] items = itemsByAnnotations(row);
		for(int i=0; i<items.length; ++i){
			columns[i].setValueAt(index, items[i]);
		}
	}

	public void addRow(final Object[] row){
		enforceTypes(row);
		if(next >= columns[0].capacity()){
			resize();
		}
		for(int i=0; i<columns.length; ++i){
			columns[i].setValueAt(next, row[i]);
		}
		++next;
	}
	
	public void addRow(final Row row){
		if(!hasColumnNames()){
			throw new DataFrameException("Columns must be labeled in order "
					+ "to use row annotation feature");
		}
		if(next >= columns[0].capacity()){
			resize();
		}
		final Object[] items = itemsByAnnotations(row);
		for(int i=0; i<items.length; ++i){
			columns[i].setValueAt(next, items[i]);
		}
		++next;
	}

	public void insertRowAt(final int index, final Object[] row){
		if((index > next) || (index < 0)){
			throw new DataFrameException("Invalid row index: "+index);
		}
		if(index == next){
			addRow(row);
			return;
		}
		enforceTypes(row);
		if(next >= columns[0].capacity()){
			resize();
		}
		for(int i=0; i<columns.length; ++i){
			columns[i].insertValueAt(index, next, row[i]);
		}
		++next;
	}
	
	public void insertRowAt(final int index, final Row row){
		if(!hasColumnNames()){
			throw new DataFrameException("Columns must be labeled in order "
					+ "to use row annotation feature");
		}
		if((index > next) || (index < 0)){
			throw new DataFrameException("Invalid row index: "+index);
		}
		if(index == next){
			addRow(row);
			return;
		}
		final Object[] items = itemsByAnnotations(row);
		if(next >= columns[0].capacity()){
			resize();
		}
		for(int i=0; i<items.length; ++i){
			columns[i].insertValueAt(index, next, items[i]);
		}
		++next;
	}

	public void removeRow(final int index){
		if((index >= next) || (index < 0)){
			throw new DataFrameException("Invalid row index: "+index);
		}
		for(final Column col : columns){
			col.remove(index, index+1, next);
		}
		--next;
		if((next*3) < columns[0].capacity()){
			flushAll(4);
		}
	}

	public void removeRows(final int from, final int to){
		if(from >= to){
			throw new DataFrameException("'to' must be greater than 'from'");
		}
		if((from < 0) || (to < 0) || (from >= next) || (to > next)){
			throw new DataFrameException("Invalid row index: "
					+((from < 0) || (from >= next) ? from : to));
		}
		for(final Column col : columns){
			col.remove(from, to, next);
		}
		next-=(to-from);
		if((next*3) < columns[0].capacity()){
			flushAll(4);
		}
	}

	public void addColumn(final Column col){
		if(col == null){
			throw new DataFrameException("Arg must not be null");
		}
		if(!col.isNullable()){
			throw new DataFrameException("NullableDataFrame must use NullableColumn instance");
		}
		if(next == -1){
			this.columns = new Column[1];
			this.columns[0] = col;
			this.next = col.capacity();
			if((col.name != null) && !(col.name.isEmpty())){
				this.names = new HashMap<String, Integer>(16);
				this.names.put(col.name, 0);
			}
		}else{
			if(col.capacity() > next){
				final int diff = (col.capacity() - next);
				for(int i=0; i<diff; ++i){
					addRow(new Object[columns.length]);
				}
			}
			col.matchLength(capacity());
			final Column[] tmp = new Column[columns.length+1];
			for(int i=0; i<columns.length; ++i){
				tmp[i] = columns[i];
			}
			tmp[columns.length] = col;
			if((col.name != null) && !(col.name.isEmpty())){
				if(names == null){
					this.names = new HashMap<String, Integer>(16);
				}
				this.names.put(col.name, columns.length);
			}
			this.columns = tmp;
		}
	}

	public void addColumn(final String colName, final Column col){
		if((colName == null) || (colName.isEmpty()) || (col == null)){
			throw new DataFrameException("Arg must not be null or empty");
		}
		if(!col.isNullable()){
			throw new DataFrameException("NullableDataFrame must use NullableColumn instance");
		}
		if(next == -1){
			this.columns = new Column[1];
			this.columns[0] = col;
			this.next = col.capacity();
			this.names = new HashMap<String, Integer>(16);
			this.names.put(colName, 0);
			col.name = colName;
		}else{
			if(col.capacity() > next){
				final int diff = (col.capacity() - next);
				for(int i=0; i<diff; ++i){
					addRow(new Object[columns.length]);
				}
			}
			col.matchLength(capacity());
			final Column[] tmp = new Column[columns.length+1];
			for(int i=0; i<columns.length; ++i){
				tmp[i] = columns[i];
			}
			tmp[columns.length] = col;
			this.columns = tmp;
			if(this.names == null){
				this.names = new HashMap<String, Integer>(16);
			}
			this.names.put(colName, columns.length-1);
			col.name = colName;
		}
	}

	public void removeColumn(final int col){
		if((next == -1) || (col < 0) || (col >= columns.length)){
			throw new DataFrameException("Invalid column index: "+col);
		}
		final Column[] tmp = new Column[columns.length-1];
		int idx = 0;
		for(int i=0; i<columns.length; ++i){
			if(i != col){
				tmp[idx++] = columns[i];
			}
		}
		if(names != null){
			final String name = this.columns[col].name;
			if(name != null){
				this.names.remove(name);
			}
			Iterator<Map.Entry<String, Integer>> iter = names.entrySet().iterator();
			while(iter.hasNext()){
				final Map.Entry<String, Integer> entry = iter.next();
				if(entry.getValue()>=col){
					entry.setValue(entry.getValue()-1);
				}
			}
		}
		this.columns = tmp;
	}

	public void removeColumn(final String colName){
		removeColumn(enforceName(colName));
	}

	public void insertColumnAt(final int index, final Column col){
		if(col == null){
			throw new DataFrameException("Arg must not be null");
		}
		if(!col.isNullable()){
			throw new DataFrameException("NullableDataFrame must use NullableColumn instance");
		}
		if(next == -1){
			if(index != 0){
				throw new DataFrameException("Invalid column index: "+index);
			}
			this.columns = new Column[1];
			this.columns[0] = col;
			this.next = col.capacity();
			if((col.name != null) && !(col.name.isEmpty())){
				this.names = new HashMap<String, Integer>(16);
				this.names.put(col.name, 0);
			}
		}else{
			if((index < 0) || (index > columns.length)){
				throw new DataFrameException("Invalid column index: "+index);
			}
			if(col.capacity() > next){
				final int diff = (col.capacity() - next);
				for(int i=0; i<diff; ++i){
					addRow(new Object[columns.length]);
				}
			}
			col.matchLength(capacity());
			final Column[] tmp = new Column[columns.length+1];
			for(int i=tmp.length-1; i>index; --i){
				tmp[i] = columns[i-1];
			}
			tmp[index] = col;
			for(int i=0; i<index; ++i){
				tmp[i] = columns[i];
			}
			this.columns = tmp;
			if(names != null){
				Iterator<Map.Entry<String, Integer>> iter = names.entrySet().iterator();
				while(iter.hasNext()){
					final Map.Entry<String, Integer> entry = iter.next();
					if(entry.getValue()>=index){
						entry.setValue(entry.getValue()+1);
					}
				}
			}
			if((col.name != null) && !(col.name.isEmpty())){
				if(names == null){
					this.names = new HashMap<String, Integer>(16);
				}
				this.names.put(col.name, index);
			}
		}
	}

	public void insertColumnAt(final int index, final String colName, final Column col){
		if((col == null) || (colName == null) || (colName.isEmpty())){
			throw new DataFrameException("Arg must not be null or empty");
		}
		col.name = colName;
		insertColumnAt(index, col);
	}
	
	public int columns(){
		return (columns != null ? columns.length : 0);
	}

	public int capacity(){
		return (columns != null ? columns[0].capacity() : 0);
	}

	public int rows(){
		return (columns != null ? next : 0);
	}

	public boolean isEmpty(){
		return (next <= 0);
	}

	public boolean isNullable(){
		return true;
	}
	
	public void clear(){
		for(final Column col : columns){
			col.remove(0, next, next);
		}
		this.next = 0;
		flushAll(2);
	}
	
	public void flush(){
		if((next != -1) && (next != columns[0].capacity())){
			flushAll(0);
		}
	}

	public Column getColumnAt(final int col){
		if((next == -1) || (col < 0) || (col >= columns.length)){
			throw new DataFrameException("Invalid column index: "+col);
		}
		return columns[col];
	}

	public Column getColumn(final String colName){
		return getColumnAt(enforceName(colName));
	}

	public void setColumnAt(final int index, final Column col){
		if(col == null){
			throw new DataFrameException("Arg must not be null");
		}
		if(!col.isNullable()){
			throw new DataFrameException("NullableDataFrame must use NullableColumn instance");
		}
		if((next == -1) || (index < 0) || (index >= columns.length)){
			throw new DataFrameException("Invalid column index: "+index);
		}
		if(col.capacity() != next){
			throw new DataFrameException("Invalid column length. Must be of length "+next);
		}
		col.matchLength(capacity());
		final String oldName = columns[index].name;
		columns[index] = col;
		if((col.name != null) && !(col.name.isEmpty())){
			if((names != null) && (oldName != null)){
				this.names.remove(oldName);
			}
			if(names == null){
				this.names = new HashMap<String, Integer>(16);
			}
			this.names.put(col.name, index);
		}else{
			col.name = oldName;
		}
	}

	public int indexOf(final int col, final String regex){
		if((next == -1) || (col < 0) || (col >= columns.length)){
			throw new DataFrameException("Invalid column index: "+col);
		}
		if((regex == null) || (regex.isEmpty())){
			throw new DataFrameException("Arg must not be null");
		}
		final Column c = columns[col];
		final Pattern p = Pattern.compile(regex);//cache
		for(int i=0; i<next; ++i){
			if(p.matcher(String.valueOf(c.getValueAt(i))).matches()){
				return i;
			}
		}
		return -1;
	}

	public int indexOf(final String colName, final String regex){
		return indexOf(enforceName(colName), regex);
	}
	
	public int indexOf(final int col, final int startFrom, final String regex){
		if((next == -1) || (col < 0) || (col >= columns.length)){
			throw new DataFrameException("Invalid column index: "+col);
		}
		if((regex == null) || (regex.isEmpty())){
			throw new DataFrameException("Arg must not be null");
		}
		if((startFrom < 0) || (startFrom >= next)){
			throw new DataFrameException("Invalid start argument: "+startFrom);
		}
		final Column c = columns[col];
		final Pattern p = Pattern.compile(regex);//cache
		for(int i=startFrom; i<next; ++i){
			if(p.matcher(String.valueOf(c.getValueAt(i))).matches()){
				return i;
			}
		}
		return -1;
	}
	
	public int indexOf(final String colName, final int startFrom, final String regex){
		return indexOf(enforceName(colName), startFrom, regex);
	}

	public int[] indexOfAll(final int col, final String regex){
		if((next == -1) || (col < 0) || (col >= columns.length)){
			throw new DataFrameException("Invalid column index: "+col);
		}
		if((regex == null) || (regex.isEmpty())){
			throw new DataFrameException("Arg must not be null or empty");
		}
		final Column c = columns[col];
		final Pattern p = Pattern.compile(regex);//cache
		int[] res = new int[16];
		int hits = 0;
		for(int i=0; i<next; ++i){
			if(p.matcher(String.valueOf(c.getValueAt(i))).matches()){
				if(hits>=res.length){//resize
					final int[] tmp = new int[res.length*2];
					for(int j=0; j<hits; ++j){
						tmp[j] = res[j];
					}
					res = tmp;
				}
				res[hits++] = i;
			}
		}
		if(res.length != hits){//trim
			final int[] tmp = new int[hits];
			for(int j=0; j<hits; ++j){
				tmp[j] = res[j];
			}
			res = tmp;
		}
		return (hits == 0 ? null : res);
	}
	
	public int[] indexOfAll(final String colName, final String regex){
		return indexOfAll(enforceName(colName), regex);
	}
	
	public DataFrame filter(final int col, final String regex){
		if((next == -1) || (col < 0) || (col >= columns.length)){
			throw new DataFrameException("Invalid column index: "+col);
		}
		if((regex == null) || (regex.isEmpty())){
			throw new DataFrameException("Arg must not be null or empty");
		}
		final int[] indices = indexOfAll(col, regex);
		final DataFrame df = new NullableDataFrame();
		for(final Column c : columns){
			df.addColumn(Column.ofType(c.typeCode()));
		}
		if(indices != null){
			for(int i=0; i<indices.length; ++i){
				df.addRow(getRowAt(indices[i]));
			}
		}
		if(names != null){
			df.setColumnNames(getColumnNames());
		}
		return df;
	}
	
	public DataFrame filter(final String colName, final String regex){
		return filter(enforceName(colName), regex);
	}
	
	public double average(final int col){
		if((next == -1) || (col < 0) || (col >= columns.length)){
			throw new DataFrameException("Invalid column index: "+col);
		}
		final Column c = columns[col];
		if(isNaN(c) || (next == 0)){
			throw new DataFrameException("Unable to compute average. Column consists of NaNs");
		}
		double avg = 0;
		int total = 0;
		switch(c.typeCode()){
		case NullableFloatColumn.TYPE_CODE:
			final NullableFloatColumn columnFloat = ((NullableFloatColumn)c);
			for(int i=0; i<next; ++i){
				if(columnFloat.get(i) != null){
					avg+=columnFloat.get(i);
					++total;
				}
			}
			break;
		case NullableDoubleColumn.TYPE_CODE:
			final NullableDoubleColumn columnDouble = ((NullableDoubleColumn)c);
			for(int i=0; i<next; ++i){
				if(columnDouble.get(i) != null){
					avg+=columnDouble.get(i);
					++total;
				}
			}
			break;
		case NullableByteColumn.TYPE_CODE:
			final NullableByteColumn columnByte = ((NullableByteColumn)c);
			for(int i=0; i<next; ++i){
				if(columnByte.get(i) != null){
					avg+=columnByte.get(i);
					++total;
				}
			}
			break;
		case NullableShortColumn.TYPE_CODE:
			final NullableShortColumn columnShort = ((NullableShortColumn)c);
			for(int i=0; i<next; ++i){
				if(columnShort.get(i) != null){
					avg+=columnShort.get(i);
					++total;
				}
			}
			break;
		case NullableIntColumn.TYPE_CODE:
			final NullableIntColumn columnInt = ((NullableIntColumn)c);
			for(int i=0; i<next; ++i){
				if(columnInt.get(i) != null){
					avg+=columnInt.get(i);
					++total;
				}
			}
			break;
		case NullableLongColumn.TYPE_CODE:
			final NullableLongColumn columnLong = ((NullableLongColumn)c);
			for(int i=0; i<next; ++i){
				if(columnLong.get(i) != null){
					avg+=columnLong.get(i);
					++total;
				}
			}
			break;
		default:
			throw new DataFrameException("Unrecognized column type");
		}
		return (avg/total);
	}
	
	public double average(final String colName){
		return average(enforceName(colName));
	}
	
	public double minimum(final int col){
		if((next == -1) || (col < 0) || (col >= columns.length)){
			throw new DataFrameException("Invalid column index: "+col);
		}
		final Column c = columns[col];
		if(isNaN(c) || (next == 0)){
			throw new DataFrameException("Unable to compute minimum. Column consists of NaNs");
		}
		Double min = 0.0;
		switch(c.typeCode()){
		case NullableFloatColumn.TYPE_CODE:
			float minFloat = Float.MAX_VALUE;
			final NullableFloatColumn columnFloat = ((NullableFloatColumn)c);
			for(int i=0; i<next; ++i){
				if((columnFloat.get(i) != null) && (columnFloat.get(i)<minFloat)){
					minFloat = columnFloat.get(i);
				}
			}
			min = (double)minFloat;
			break;
		case NullableDoubleColumn.TYPE_CODE:
			double minDouble = Double.MAX_VALUE;
			final NullableDoubleColumn columnDouble = ((NullableDoubleColumn)c);
			for(int i=0; i<next; ++i){
				if((columnDouble.get(i) != null) && (columnDouble.get(i)<minDouble)){
					minDouble = columnDouble.get(i);
				}
			}
			min = minDouble;
			break;
		case NullableByteColumn.TYPE_CODE:
			byte minByte = Byte.MAX_VALUE;
			final NullableByteColumn columnByte = ((NullableByteColumn)c);
			for(int i=0; i<next; ++i){
				if((columnByte.get(i) != null) && (columnByte.get(i)<minByte)){
					minByte = columnByte.get(i);
				}
			}
			min = (double)minByte;
			break;
		case NullableShortColumn.TYPE_CODE:
			short minShort = Short.MAX_VALUE;
			final NullableShortColumn columnShort = ((NullableShortColumn)c);
			for(int i=0; i<next; ++i){
				if((columnShort.get(i) != null) && (columnShort.get(i)<minShort)){
					minShort = columnShort.get(i);
				}
			}
			min = (double)minShort;
			break;
		case NullableIntColumn.TYPE_CODE:
			int minInt = Integer.MAX_VALUE;
			final NullableIntColumn columnInt = ((NullableIntColumn)c);
			for(int i=0; i<next; ++i){
				if((columnInt.get(i) != null) && (columnInt.get(i)<minInt)){
					minInt = columnInt.get(i);
				}
			}
			min = (double)minInt;
			break;
		case NullableLongColumn.TYPE_CODE:
			long minLong = Long.MAX_VALUE;
			final NullableLongColumn columnLong = ((NullableLongColumn)c);
			for(int i=0; i<next; ++i){
				if((columnLong.get(i) != null) && (columnLong.get(i)<minLong)){
					minLong = columnLong.get(i);
				}
			}
			min = (double)minLong;
			break;
		default:
			throw new DataFrameException("Unrecognized column type");
		}
		return min;
	}
	
	public double minimum(final String colName){
		return minimum(enforceName(colName));
	}
	
	public double maximum(final int col){
		if((next == -1) || (col < 0) || (col >= columns.length)){
			throw new DataFrameException("Invalid column index: "+col);
		}
		final Column c = columns[col];
		if(isNaN(c) || (next == 0)){
			throw new DataFrameException("Unable to compute maximum. Column consists of NaNs");
		}
		Double max = 0.0;
		switch(c.typeCode()){
		case NullableFloatColumn.TYPE_CODE:
			float maxFloat = Float.MIN_VALUE;
			final NullableFloatColumn columnFloat = ((NullableFloatColumn)c);
			for(int i=0; i<next; ++i){
				if((columnFloat.get(i) != null) && (columnFloat.get(i)>maxFloat)){
					maxFloat = columnFloat.get(i);
				}
			}
			max = (double)maxFloat;
			break;
		case NullableDoubleColumn.TYPE_CODE:
			double maxDouble = Double.MIN_VALUE;
			final NullableDoubleColumn columnDouble = ((NullableDoubleColumn)c);
			for(int i=0; i<next; ++i){
				if((columnDouble.get(i) != null) && (columnDouble.get(i)>maxDouble)){
					maxDouble = columnDouble.get(i);
				}
			}
			max = maxDouble;
			break;
		case NullableByteColumn.TYPE_CODE:
			byte maxByte = Byte.MIN_VALUE;
			final NullableByteColumn columnByte = ((NullableByteColumn)c);
			for(int i=0; i<next; ++i){
				if((columnByte.get(i) != null) && (columnByte.get(i)>maxByte)){
					maxByte = columnByte.get(i);
				}
			}
			max = (double)maxByte;
			break;
		case NullableShortColumn.TYPE_CODE:
			short maxShort = Short.MIN_VALUE;
			final NullableShortColumn columnShort = ((NullableShortColumn)c);
			for(int i=0; i<next; ++i){
				if((columnShort.get(i) != null) && (columnShort.get(i)>maxShort)){
					maxShort = columnShort.get(i);
				}
			}
			max = (double)maxShort;
			break;
		case NullableIntColumn.TYPE_CODE:
			int maxInt = Integer.MIN_VALUE;
			final NullableIntColumn columnInt = ((NullableIntColumn)c);
			for(int i=0; i<next; ++i){
				if((columnInt.get(i) != null) && (columnInt.get(i)>maxInt)){
					maxInt = columnInt.get(i);
				}
			}
			max = (double)maxInt;
			break;
		case NullableLongColumn.TYPE_CODE:
			long maxLong = Long.MIN_VALUE;
			final NullableLongColumn columnLong = ((NullableLongColumn)c);
			for(int i=0; i<next; ++i){
				if((columnLong.get(i) != null) && (columnLong.get(i)>maxLong)){
					maxLong = columnLong.get(i);
				}
			}
			max = (double)maxLong;
			break;
		default:
			throw new DataFrameException("Unrecognized column type");
		}
		return max;
	}
	
	public double maximum(final String colName){
		return maximum(enforceName(colName));
	}
	
	public void sortBy(final int col){
		if((next == -1) || (col < 0) || (col >= columns.length)){
			throw new DataFrameException("Invalid column index: "+col);
		}
		NullableDataFrame.QuickSort.sort(columns[col], columns, next);
	}
	
	public void sortBy(final String colName){
		final int col = enforceName(colName);
		NullableDataFrame.QuickSort.sort(columns[col], columns, next);
	}
	
	public Object[][] asArray(){
		if(next == -1){
			return null;
		}
		final Object[][] a = new Object[columns.length][next];
		for(int i=0; i<columns.length; ++i){
			final Column c = (Column)getColumnAt(i).clone();
			for(int j=0; j<next; ++j){
				a[i][j] = c.getValueAt(j);
			}
		}
		return a;
	}
	
	@Override
	public String toString(){
		if(columns == null){
			return "uninitialized DataFrame instance";
		}
		final String nl = System.lineSeparator();
		int[] max = new int[columns.length];
		int maxIdx = String.valueOf(next-1).length();
		for(int i=0; i<columns.length; ++i){
			int k = 0;
			for(int j=0; j<next; ++j){
				if(String.valueOf(columns[i].getValueAt(j)).length() > k){
					k = String.valueOf(columns[i].getValueAt(j)).length();
				}
			}
			max[i] = k;
		}
		String[] n = new String[columns.length];
		if(names != null){
			final Set<Map.Entry<String, Integer>> set = names.entrySet();
			for(int i=0; i<columns.length; ++i){
				String s = null;
				for(final Map.Entry<String, Integer> e : set){
					if(e.getValue() == i){
						s = e.getKey();
						break;
					}
				}
				n[i] = (s != null ? s : String.valueOf(i));
			}
		}else{
			for(int i=0; i<columns.length; ++i){
				n[i] = (i+" ");
			}
		}
		for(int i=0; i<columns.length; ++i){
			max[i] = (max[i]>=n[i].length() ? max[i] : n[i].length());
		}
		final StringBuilder sb = new StringBuilder();
		for(int i=0; i<maxIdx; ++i){
			sb.append("_");
		}
		sb.append("|");
		for(int i=0; i<columns.length; ++i){
			sb.append(" ");
			sb.append(n[i]);
			for(int j=(max[i]-n[i].length()); j>0; --j){
				sb.append(" ");
			}
		}
		sb.append(nl);
		for(int i=0; i<next; ++i){
			sb.append(i);
			for(int ii=0; ii<(maxIdx-String.valueOf(i).length()); ++ii){
				sb.append(" ");
			}
			sb.append("| ");
			for(int j=0; j<columns.length; ++j){
				final Object val = columns[j].getValueAt(i);
				final String s = (val != null ? val.toString() : "null");	
				sb.append(s);
				for(int k=(max[j]-s.length()); k>=0; --k){
					sb.append(" ");
				}
			}
			sb.append(nl);
		}
		return sb.toString();
	}
	
	@Override
	public Object clone(){
		return DataFrame.copyOf(this);
	}
	
	@Override
	public int hashCode(){
		int hash = 0;
		final String[] n = this.getColumnNames();
		for(int i=0; i<columns.length; ++i){
			hash += n[i].hashCode();
			hash += columns[i].typeCode();
		}
		for(final Column col : this){
			for(int i=0; i<this.rows(); ++i){
				final Object val =col.getValueAt(i);
				if(val != null){
					hash += val.hashCode();
				}else{
					hash += 3;
				}
			}
		}
		return hash;
	}
	
	@Override
	public boolean equals(Object obj){
		if(!(obj instanceof NullableDataFrame)){
			return false;
		}
		final DataFrame df = (NullableDataFrame) obj;
		if((this.rows() != df.rows()) || (this.columns() != df.columns())){
			return false;
		}
		final String[] n1 = this.getColumnNames();
		final String[] n2 = df.getColumnNames();
		for(int i=0; i<df.columns(); ++i){
			//compare column names
			if(!n1[i].equals(n2[i])){
				return false;
			}
			//compare column types
			if(this.getColumnAt(i).typeCode() != df.getColumnAt(i).typeCode()){
				return false;
			}
		}
		//compare data
		int idx = 0;
		for(final Column col2 : df){
			final Column col1 = getColumnAt(idx++);
			for(int i=0; i<df.rows(); ++i){
				final Object val1 = col1.getValueAt(i);
				final Object val2 = col2.getValueAt(i);
				//both entries are non-null
				if((val1 != null) && (val2 != null)){
					if(!val1.equals(val2)){
						return false;
					}
					continue;
				}
				//both entries are null
				if((val1 == null) && (val2 == null)){
					continue;
				}
				//one entry is null and the other isn't
				return false;
			}
		}
		return true;
	}
	
	@Override
	public Iterator<Column> iterator(){
		return new ColumnIterator(this);
	}
	
	/**
	 * Initialization method for assigning this DataFrame the specified columns and their names
	 * 
	 * @param columns The columns to use in this DataFrame instance
	 */
	private void assignColumns(final Column[] columns){
		if((columns == null) || (columns.length == 0)){
			throw new DataFrameException("Arg must not be null or empty");
		}
		int colSize = columns[0].capacity();
		for(int i=1; i<columns.length; ++i){
			if(columns[i].capacity() != colSize){
				throw new DataFrameException("Columns have deviating sizes");
			}
		}
		this.columns = new Column[columns.length];
		for(int i=0; i<columns.length; ++i){
			final Column col = columns[i];
			if(!col.isNullable()){
				throw new DataFrameException("NullableDataFrame must use NullableColumn instance");
			}
			this.columns[i] = col;
			if((col.name != null) && !(col.name.isEmpty())){
				if(names == null){
					this.names = new HashMap<String, Integer>(16);
				}
				this.names.put(col.name, i);
			}
		}
		this.next = colSize;
	}
	
	/**
	 * Resizes all columns sequentially
	 */
	private void resize(){
		for(final Column col : columns){
			col.resize();
		}
	}
	
	/**
	 * Enforces that all entries in the given row adhere to the column types in this DataFrame
	 * 
	 * @param row The row to check against type missmatches
	 */
	private void enforceTypes(final Object[] row){
		if((next == -1) || (row.length != columns.length)){
			throw new DataFrameException("Length does not match number of columns: "+row.length);
		}
		for(int i=0; i<columns.length; ++i){
			if(row[i] != null){
				if(!(columns[i].memberClass().equals(row[i].getClass()))){
					throw new DataFrameException(String.format(
							"Type missmatch at column %s. Expected %s but found %s",
							i, columns[i].memberClass().getSimpleName(),
							row[i].getClass().getSimpleName()));

				}
			}
		}
	}
	
	/**
	 * Enforces that all requirements are met in order to access a column by its name.
	 * Throws an exception in the case of failure or returns the index of the column in
	 * the case of success
	 * 
	 * @param colName The name to check
	 * @return The index of the column with the specified name 
	 */
	private int enforceName(final String colName){
		if((colName == null) || (colName.isEmpty())){
			throw new DataFrameException("Arg must not be null or empty");
		}
		if(names == null){
			throw new DataFrameException("Column names not set");
		}
		final Integer col = names.get(colName);
		if(col == null){
			throw new DataFrameException("Invalid column name: "+colName);
		}
		return col;
	}
	
	/**
	 * Indicates whether a given Column contains NaN values
	 * 
	 * @param col The Column instance to check
	 * @return True if the given column contains NaNs, false otherwise
	 */
	private boolean isNaN(final Column col){
		final byte typeCode = col.typeCode();
		return (typeCode == NullableStringColumn.TYPE_CODE
				|| typeCode == NullableCharColumn.TYPE_CODE
				|| typeCode == NullableBooleanColumn.TYPE_CODE);
	}
	
	/**
	 * Sequentially performs a flush operation on all columns. A buffer can be set to keep
	 * some extra space between the current entries and the column capacity 
	 * 
	 * @param buffer A buffer applied to each column. Using 0 (zero) will apply no buffer
	 * 	   	  at all and will shrink each column to its minimum required length
	 */
	private void flushAll(final int buffer){
		for(final Column col : columns){
			col.matchLength(next+buffer);
		}
	}
	
	/**
	 * Collects all annotated items from a <code>Row</code> object and returns them in an
	 * array at the correct index for further processing
	 * 
	 * @param row The row to get the items from
	 * @return An array holding the row items of the specified Row object
	 */
	private Object[] itemsByAnnotations(final Row row){
		final Object[] items = new Object[columns.length];
		for(final Field field : row.getClass().getDeclaredFields()){
			final RowItem item = field.getAnnotation(RowItem.class);
			if(item != null){
				String name = item.value();
				if((name == null || name.isEmpty())){
					name = field.getName();
				}
				final int i = enforceName(name);
				field.setAccessible(true);
				Object value = null;
				try{
					value = field.get(row);
				}catch(IllegalArgumentException | IllegalAccessException ex){
					throw new DataFrameException(ex.getMessage());
				}
				if((value != null) && !value.getClass().equals(columns[i].memberClass())){
					throw new DataFrameException(
                            String.format("Row item %s uses an incorrect type. " 
                            + "Expected %s but found %s", name,
                            columns[i].memberClass().getSimpleName(), 
                            value.getClass().getSimpleName()));
				}
				items[i] = value;
			}
		}
		return items;
	}
	
	/**
	 * Returns an instance of the correct <code>Column</code> for the specified 
	 * class type
	 * 
	 * @param classOfField The class of the field to get a column for
	 * @return A <code>Column</code> that matches the type of the provided class
	 */
	private Column inferColumnFromType(final Class<?> classOfField){
		switch(classOfField.getSimpleName()){
		case "String":
			return new NullableStringColumn();
		case "Byte":
			return new NullableByteColumn();
		case "Short":
			return new NullableShortColumn();
		case "Integer":
			return new NullableIntColumn();
		case "Long":
			return new NullableLongColumn();
		case "Float":
			return new NullableFloatColumn();
		case "Double":
			return new NullableDoubleColumn();
		case "Character":
			return new NullableCharColumn();
		case "Boolean":
			return new NullableBooleanColumn();
		default:
			throw new DataFrameException(classOfField.isPrimitive() 
					? "NullableDataFrame does not support primitive types for row items: " 
					    + classOfField.getSimpleName() 
					: "Unsupported type for row item: " 
                        + classOfField.getSimpleName());

		}
	}
	
	/**
	 * Internal Quicksort implementation for sorting NullableDataFrame instances.
	 * Presorts the column by putting all null values at the end and then only sorting
	 * the remaining part.
	 *
	 */
	private static class QuickSort {

		private static void sort(Column col, Column[] cols, int next){
			
			switch(col.typeCode()){
			case NullableByteColumn.TYPE_CODE:
				sort(((NullableByteColumn)col).asArray(), cols, 0, 
						presort(((NullableByteColumn)col).asArray(), cols, next));
				break;
			case NullableShortColumn.TYPE_CODE:
				sort(((NullableShortColumn)col).asArray(), cols, 0,
						presort(((NullableShortColumn)col).asArray(), cols, next));
				break;
			case NullableIntColumn.TYPE_CODE:
				sort(((NullableIntColumn)col).asArray(), cols, 0, 
						presort(((NullableIntColumn)col).asArray(), cols, next));
				break;
			case NullableLongColumn.TYPE_CODE:
				sort(((NullableLongColumn)col).asArray(), cols, 0, 
						presort(((NullableLongColumn)col).asArray(), cols, next));
				break;
			case NullableStringColumn.TYPE_CODE:
				sort(((NullableStringColumn)col).asArray(), cols, 0, 
						presort(((NullableStringColumn)col).asArray(), cols, next));
				break;
			case NullableFloatColumn.TYPE_CODE:
				sort(((NullableFloatColumn)col).asArray(), cols, 0, 
						presort(((NullableFloatColumn)col).asArray(), cols, next));
				break;
			case NullableDoubleColumn.TYPE_CODE:
				sort(((NullableDoubleColumn)col).asArray(), cols, 0, 
						presort(((NullableDoubleColumn)col).asArray(), cols, next));
				break;
			case NullableCharColumn.TYPE_CODE:
				sort(((NullableCharColumn)col).asArray(), cols, 0, 
						presort(((NullableCharColumn)col).asArray(), cols, next));
				break;
			case NullableBooleanColumn.TYPE_CODE:
				sort(((NullableBooleanColumn)col).asArray(), cols, 0, 
						presort(((NullableBooleanColumn)col).asArray(), cols, next));
				break;
			default:
				//undefined
			}
		}	
		
	    private static void sort(Byte[] list, Column[] cols, int left, int right){
	    	if(right <= -1){
	    		return;
	    	}
	        final byte MID = list[(left+right)/2];
	        int l = left;
	        int r = right;
	        while(l < r){
	            while(list[l] < MID){ ++l; }
	            while(list[r] > MID){ --r; }
	            if(l <= r){
	                swap(cols, l++, r--);
	            }
	        }
	        if(left < r){
	            sort(list, cols, left, r );
	        }
	        if(right > l){
	            sort(list, cols, l, right);
	        }
	    }
	    
	    private static void sort(Short[] list, Column[] cols, int left, int right){
	    	if(right <= -1){
	    		return;
	    	}
	        final short MID = list[(left+right)/2];
	        int l = left;
	        int r = right;
	        while(l < r){
	            while(list[l] < MID){ ++l; }
	            while(list[r] > MID){ --r; }
	            if(l <= r){
	                swap(cols, l++, r--);
	            }
	        }
	        if(left < r){
	            sort(list, cols, left, r );
	        }
	        if(right > l){
	            sort(list, cols, l, right);
	        }
	    }
		
	    private static void sort(Integer[] list, Column[] cols, int left, int right){
	    	if(right <= -1){
	    		return;
	    	}
	        final int MID = list[(left+right)/2];
	        int l = left;
	        int r = right;
	        while(l < r){
	            while(list[l] < MID){ ++l; }
	            while(list[r] > MID){ --r; }
	            if(l <= r){
	                swap(cols, l++, r--);
	            }
	        }
	        if(left < r){
	            sort(list, cols, left, r );
	        }
	        if(right > l){
	            sort(list, cols, l, right);
	        }
	    }
	    
	    private static void sort(Long[] list, Column[] cols, int left, int right){
	    	if(right <= -1){
	    		return;
	    	}
	        final long MID = list[(left+right)/2];
	        int l = left;
	        int r = right;
	        while(l < r){
	            while(list[l] < MID){ ++l; }
	            while(list[r] > MID){ --r; }
	            if(l <= r){
	                swap(cols, l++, r--);
	            }
	        }
	        if(left < r){
	            sort(list, cols, left, r );
	        }
	        if(right > l){
	            sort(list, cols, l, right);
	        }
	    }
	    
	    private static void sort(String[] list, Column[] cols, int left, int right){
	    	if(right <= -1){
	    		return;
	    	}
	        final String MID = list[(left+right)/2];
	        int l = left;
	        int r = right;
	        while(l < r){
	            while(list[l].compareTo(MID)<0){ ++l; }
	            while(list[r].compareTo(MID)>0){ --r; }
	            if(l <= r){
	                swap(cols, l++, r--);
	            }
	        }
	        if(left < r){
	            sort(list, cols, left, r );
	        }
	        if(right > l){
	            sort(list, cols, l, right);
	        }
	    }
	    
	    private static void sort(Float[] list, Column[] cols, int left, int right){
	    	if(right <= -1){
	    		return;
	    	}
	        final float MID = list[(left+right)/2];
	        int l = left;
	        int r = right;
	        while(l < r){
	            while(list[l] < MID){ ++l; }
	            while(list[r] > MID){ --r; }
	            if(l <= r){
	                swap(cols, l++, r--);
	            }
	        }
	        if(left < r){
	            sort(list, cols, left, r );
	        }
	        if(right > l){
	            sort(list, cols, l, right);
	        }
	    }
	    
	    private static void sort(Double[] list, Column[] cols, int left, int right){
	    	if(right <= -1){
	    		return;
	    	}
	        final double MID = list[(left+right)/2];
	        int l = left;
	        int r = right;
	        while(l < r){
	            while(list[l] < MID){ ++l; }
	            while(list[r] > MID){ --r; }
	            if(l <= r){
	                swap(cols, l++, r--);
	            }
	        }
	        if(left < r){
	            sort(list, cols, left, r );
	        }
	        if(right > l){
	            sort(list, cols, l, right);
	        }
	    }
	    
	    private static void sort(Character[] list, Column[] cols, int left, int right){
	    	if(right <= -1){
	    		return;
	    	}
	        final char MID = list[(left+right)/2];
	        int l = left;
	        int r = right;
	        while(l < r){
	            while(list[l] < MID){ ++l; }
	            while(list[r] > MID){ --r; }
	            if(l <= r){
	                swap(cols, l++, r--);
	            }
	        }
	        if(left < r){
	            sort(list, cols, left, r );
	        }
	        if(right > l){
	            sort(list, cols, l, right);
	        }
	    }
	    
	    private static void sort(Boolean[] list, Column[] cols, int left, int right){
	    	if(right <= -1){
	    		return;
	    	}
	        final Boolean MID = list[(left+right)/2];
	        int l = left;
	        int r = right;
	        while(l < r){
	            while(new Boolean(list[l]).compareTo(MID)<0){ ++l; }
	            while(new Boolean(list[r]).compareTo(MID)>0){ --r; }
	            if(l <= r){
	                swap(cols, l++, r--);
	            }
	        }
	        if(left < r){
	            sort(list, cols, left, r );
	        }
	        if(right > l){
	            sort(list, cols, l, right);
	        }
	    }
	    
	    private static void swap(Column[] cols, int i, int j){
	    	for(final Column c : cols){
		        final Object cache = c.getValueAt(i);
		        c.setValueAt(i, c.getValueAt(j));
		        c.setValueAt(j, cache);
	    	}
	    }
	    
	    private static int presort(Object[] list, Column[] cols, int next){
	    	int ptr = next-1;
	    	for(int i=0; i<ptr; ++i){
	    		while(list[i] == null){
	    			if(i == ptr){
	    				break;
	    			}
	    			swap(cols, i, ptr--);
	    		}
	    	}
	    	return (list[ptr] != null ? ptr : ptr-1);
	    }
	}

}
