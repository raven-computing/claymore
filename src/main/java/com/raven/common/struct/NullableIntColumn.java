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

import java.util.Iterator;
import java.util.List;

/**
 * Column holding nullable integer values.<br>
 * Any values not explicitly set are considered null. This class uses the primitive 
 * wrapper object as the underlying data structure.
 * 
 * @see IntColumn
 *
 */
public class NullableIntColumn extends NullableColumn {
	
	/**
	 * The unique type code of all <code>NullableIntColumns</code>
	 */
	public static final byte TYPE_CODE = (byte)12;
	
	private Integer[] entries;
	
	/**
	 * 	Constructs an empty <code>NullableIntColumn</code>.
	 */
	public NullableIntColumn(){
		this.entries = new Integer[0];
	}
	
	/**
	 * Constructs an empty <code>NullableIntColumn</code> with the specified label.
	 * 
	 * @param name The name of the column to construct. Must not be null or empty
	 */
	public NullableIntColumn(final String name){
		this();
		if((name == null) || (name.isEmpty())){
			throw new IllegalArgumentException("Column name must not be null or empty");
		}
		this.name = name;
	}

	/**
	 * Constructs a new <code>NullableIntColumn</code> composed of the content of 
	 * the specified int array 
	 * 
	 * @param column The entries of the column to be constructed. Must not be null
	 */
	public NullableIntColumn(final int[] column){
		if(column == null){
			throw new IllegalArgumentException("Arg must not be null");
		}
		Integer[] obj = new Integer[column.length];
		for(int i=0; i<column.length; ++i){
			obj[i] = column[i];
		}
		this.entries = obj;
	}
	
	/**
	 * Constructs a new labeled <code>NullableIntColumn</code> composed of the
	 * content of the specified int array
	 * 
	 * @param name The name of the column to construct. Must not be null or empty
	 * @param column The entries of the column to be constructed. Must not be null
	 */
	public NullableIntColumn(final String name, final int[] column){
		this(name);
		if(column == null){
			throw new IllegalArgumentException("Arg must not be null");
		}
		Integer[] obj = new Integer[column.length];
		for(int i=0; i<column.length; ++i){
			obj[i] = column[i];
		}
		this.entries = obj;
	}
	
	/**
	 * Constructs a new <code>NullableIntColumn</code> composed of the content of 
	 * the specified Integer array. Individual entries may be null
	 * 
	 * @param column The entries of the column to be constructed. Must not be null
	 */
	public NullableIntColumn(final Integer[] column){
		if(column == null){
			throw new IllegalArgumentException("Arg must not be null");
		}
		this.entries = column;
	}
	
	/**
	 * Constructs a new labeled <code>NullableIntColumn</code> composed of the
	 * content of the specified Integer array. Individual entries may be null
	 * 
	 * @param name The name of the column to construct. Must not be null or empty
	 * @param column The entries of the column to be constructed. Must not be null
	 */
	public NullableIntColumn(final String name, final Integer[] column){
		this(name);
		if(column == null){
			throw new IllegalArgumentException("Arg must not be null");
		}
		this.entries = column;
	}
	
	/**
	 * Constructs a new <code>NullableIntColumn</code> composed of the content of 
	 * the specified List. Individual items may be null
	 * 
	 * @param list The entries of the column to be constructed. Must not be null or empty
	 */
	public NullableIntColumn(final List<Integer> list){
		fillFrom(list);
	}
	
	/**
	 * Constructs a new labeled <code>NullableIntColumn</code> composed of the content of 
	 * the specified list
	 * 
	 * @param name The name of the column to construct. Must not be null or empty
	 * @param list The list representing the entries of the column to be constructed.
	 *             Must not be null or empty
	 */
	public NullableIntColumn(final String name, final List<Integer> list){
		this(name);
		fillFrom(list);
	}
	
	/**
	 * Gets the entry of this column at the specified index
	 * 
	 * @param index The index of the entry to get
	 * @return The Integer value at the specified index. May be null
	 */
	public Integer get(final int index){
		return entries[index];
	}
	
	/**
	 * Sets the entry of this column at the specified index
	 * to the given value
	 * 
	 * @param index The index of the entry to set
	 * @param value The Integer value to set the entry to. May be null
	 */
	public void set(final int index, final Integer value){
		entries[index] = value;
	}
	
	/**
	 * Returns a reference to the internal array of this column
	 * 
	 * @return The internal Integer array
	 */
	public Integer[] asArray(){
		return this.entries;
	}
	
	public Object clone(){
		final Integer[] clone = new Integer[entries.length];
		for(int i=0; i<entries.length; ++i){
			clone[i] = (entries[i] != null ? new Integer(entries[i]) : null);
		}
		return new NullableIntColumn(clone);
	}

	public Object getValueAt(int index){
		return entries[index];
	}

	public void setValueAt(int index, Object value){
		entries[index] = (Integer)value;
	}
	
	public byte typeCode(){
		return TYPE_CODE;
	}
	
	protected int capacity(){
		return entries.length;
	}
	
	protected void insertValueAt(int index, int next, Object value){
		for(int i=next; i>index; --i){
			entries[i] = entries[i-1];
		}
		entries[index] = (Integer)value;
	}

	protected Class<?> memberClass(){
		return Integer.class;
	}

	protected void resize(){
		Integer[] newEntries = new Integer[(entries.length > 0 ? entries.length*2 : 2)];
		for(int i=0; i<entries.length; ++i){
			newEntries[i] = entries[i];
		}
		this.entries = newEntries;
	}
	
	protected void remove(int from, int to, int next){
		for(int i=from, j=0; j<(next-to); ++i, ++j){
			entries[i] = entries[(to-from)+i];
		}
		for(int i=next-1, j=0; j<(to-from); --i, ++j){
			entries[i] = null;
		}
	}

	protected void matchLength(int length){
		if(length != entries.length){
			final Integer[] tmp = new Integer[length];
			for(int i=0; i<length; ++i){
				if(i < entries.length){
					tmp[i] = entries[i];
				}else{
					break;
				}
			}
			this.entries = tmp;
		}
	}
	
	private void fillFrom(final List<Integer> list){
		if((list == null) || (list.isEmpty())){
			throw new IllegalArgumentException("Arg must not be null or empty");
		}
		Integer[] tmp = new Integer[list.size()];
		Iterator<Integer> iter = list.iterator();
		int i=0;
		while(iter.hasNext()){
			tmp[i++] = iter.next();
		}
		this.entries = tmp;
	}
}
