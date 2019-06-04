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
 * Column holding boolean values.<br>
 * This implementation <b>DOES NOT</b> support null values.
 * 
 * @see NullableBooleanColumn
 *
 */
public class BooleanColumn extends Column {
	
	/**
	 * The unique type code of all <code>BooleanColumns</code>
	 */
	public static final byte TYPE_CODE = (byte)9;
	
	private boolean[] entries;
	
	/**
	 * Constructs an empty <code>BooleanColumn</code>.
	 */
	public BooleanColumn(){
		this.entries = new boolean[0];
	}
	
	/**
	 * Constructs an empty <code>BooleanColumn</code> with the specified label.
	 * 
	 * @param name The name of the column to construct. Must not be null or empty
	 */
	public BooleanColumn(final String name){
		this();
		if((name == null) || (name.isEmpty())){
			throw new IllegalArgumentException("Column name must not be null or empty");
		}
		this.name = name;
	}

	/**
	 * Constructs a new <code>BooleanColumn</code> composed of the content of 
	 * the specified boolean array 
	 * 
	 * @param column The entries of the column to be constructed. Must not be null
	 */
	public BooleanColumn(final boolean[] column){
		if(column == null){
			throw new IllegalArgumentException("Arg must not be null");
		}
		this.entries = column;
	}
	
	/**
	 * Constructs a new labeled <code>BooleanColumn</code> composed of the content of 
	 * the specified boolean array
	 * 
	 * @param name The name of the column to construct. Must not be null or empty
	 * @param column The entries of the column to be constructed. Must not be null
	 */
	public BooleanColumn(final String name, final boolean[] column){
		this(name);
		if(column == null){
			throw new IllegalArgumentException("Arg must not be null");
		}
		this.entries = column;
	}
	
	/**
	 * Constructs a new <code>BooleanColumn</code> composed of the content of 
	 * the specified list
	 * 
	 * @param list The list representing the entries of the column to be constructed
	 */
	public BooleanColumn(final List<Boolean> list){
		fillFrom(list);
	}
	
	/**
	 * Constructs a new labeled <code>BooleanColumn</code> composed of the content of 
	 * the specified list
	 * 
	 * @param name The name of the column to construct. Must not be null or empty
	 * @param list The list representing the entries of the column to be constructed.
	 *             Must not be null or empty
	 */
	public BooleanColumn(final String name, final List<Boolean> list){
		this(name);
		fillFrom(list);
	}

	/**
	 * Gets the entry of this column at the specified index
	 * 
	 * @param index The index of the entry to get
	 * @return The boolean value at the specified index
	 */
	public boolean get(final int index){
		return entries[index];
	}
	
	/**
	 * Sets the entry of this column at the specified index
	 * to the given value
	 * 
	 * @param index The index of the entry to set
	 * @param value The boolean value to set the entry to
	 */
	public void set(final int index, final boolean value){
		entries[index] = value;
	}
	
	/**
	 * Returns a reference to the internal array of this column
	 * 
	 * @return The internal boolean array
	 */
	public boolean[] asArray(){
		return this.entries;
	}
	
	public Object clone(){
		final boolean[] clone = new boolean[entries.length];
		for(int i=0; i<entries.length; ++i){
			clone[i] = entries[i];
		}
		return new BooleanColumn(clone);
	}

	public Object getValueAt(int index){
		return entries[index];
	}

	public void setValueAt(int index, Object value){
		entries[index] = (Boolean)value;
	}
	
	public byte typeCode(){
		return TYPE_CODE;
	}
	
	public boolean isNullable(){
		return false;
	}
	
	protected int capacity(){
		return entries.length;
	}
	
	protected void insertValueAt(int index, int next, Object value){
		for(int i=next; i>index; --i){
			entries[i] = entries[i-1];
		}
		entries[index] = (Boolean)value;
	}

	protected Class<?> memberClass(){
		return Boolean.class;
	}

	protected void resize(){
		boolean[] newEntries = new boolean[(entries.length > 0 ? entries.length*2 : 2)];
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
			entries[i] = false;
		}
	}

	protected void matchLength(int length){
		if(length != entries.length){
			final boolean[] tmp = new boolean[length];
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
	
	private void fillFrom(final List<Boolean> list){
		if((list == null) || (list.isEmpty())){
			throw new IllegalArgumentException("Arg must not be null or empty");
		}
		boolean[] tmp = new boolean[list.size()];
		Iterator<Boolean> iter = list.iterator();
		int i=0;
		while(iter.hasNext()){
			tmp[i++] = iter.next();
		}
		this.entries = tmp;
	}
}