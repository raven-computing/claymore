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
 * Column holding String values.<br>
 * This implementation <b>DOES NOT</b> support null values or empty strings.
 * 
 * @see NullableStringColumn
 *
 */
public class StringColumn extends Column {
	
	/**
	 * The unique type code of all <code>StringColumns</code>
	 */
	public static final byte TYPE_CODE = (byte)5;
	
	/**
	 * The placeholder used when trying to add or insert null or 
	 * empty strings into any StringColumn
	 */
	public static final String PLACEHOLDER_EMPTY = "n/a";
	
	private String[] entries;
	
	/**
	 * Constructs an empty <code>StringColumn</code>.
	 */
	public StringColumn(){
		this.entries = new String[0];
	}
	
	/**
	 * Constructs an empty <code>StringColumn</code> with the specified label.
	 * 
	 * @param name The name of the column to construct. Must not be null or empty
	 */
	public StringColumn(final String name){
		this();
		if((name == null) || (name.isEmpty())){
			throw new IllegalArgumentException("Column name must not be null or empty");
		}
		this.name = name;
	}
	
	/**
	 * Constructs a new <code>StringColumn</code> composed of the content of 
	 * the specified string array 
	 * 
	 * @param column The entries of the column to be constructed. Must not be null
	 */
	public StringColumn(final String[] column){
		if(column == null){
			throw new IllegalArgumentException("Arg must not be null");
		}
		for(int i=0; i<column.length; ++i){
			if((column[i] == null) || (column[i].isEmpty())){
				column[i] = PLACEHOLDER_EMPTY;
			}
		}
		this.entries = column;
	}
	
	/**
	 * Constructs a new labeled <code>StringColumn</code> composed of the content of 
	 * the specified string array
	 * 
	 * @param name The name of the column to construct. Must not be null or empty
	 * @param column The entries of the column to be constructed. Must not be null
	 */
	public StringColumn(final String name, final String[] column){
		this(name);
		if(column == null){
			throw new IllegalArgumentException("Arg must not be null");
		}
		this.entries = column;
	}
	
	/**
	 * Constructs a new <code>StringColumn</code> composed of the content of 
	 * the specified list
	 * 
	 * @param list The list representing the entries of the column to be constructed
	 */
	public StringColumn(final List<String> list){
		fillFrom(list);
	}
	
	/**
	 * Constructs a new labeled <code>StringColumn</code> composed of the content of 
	 * the specified list
	 * 
	 * @param name The name of the column to construct. Must not be null or empty
	 * @param list The list representing the entries of the column to be constructed.
	 *             Must not be null or empty
	 */
	public StringColumn(final String name, final List<String> list){
		this(name);
		fillFrom(list);
	}
	
	/**
	 * Gets the entry of this column at the specified index
	 * 
	 * @param index The index of the entry to get
	 * @return The string value at the specified index
	 */
	public String get(final int index){
		return entries[index];
	}
	
	/**
	 * Sets the entry of this column at the specified index
	 * to the given value
	 * 
	 * @param index The index of the entry to set
	 * @param value The string value to set the entry to
	 */
	public void set(final int index, final String value){
		entries[index] = (((value == null) || (value.isEmpty())) 
                ? PLACEHOLDER_EMPTY 
                : value);
	}
	
	/**
	 * Returns a reference to the internal array of this column
	 * 
	 * @return The internal string array
	 */
	public String[] asArray(){
		return this.entries;
	}
	
	public Object clone(){
		final String[] clone = new String[entries.length];
		for(int i=0; i<entries.length; ++i){
			clone[i] = entries[i];
		}
		return new StringColumn(clone);
	}

	public Object getValueAt(int index){
		return entries[index];
	}

	public void setValueAt(int index, Object value){
		entries[index] = (((value == null) || ((String)value).isEmpty()) 
                ? PLACEHOLDER_EMPTY 
                : (String)value);
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
		entries[index] = (((value == null) || ((String)value).isEmpty()) 
                ? PLACEHOLDER_EMPTY 
                : (String)value);
	}

	protected Class<?> memberClass(){
		return String.class;
	}

	protected void resize(){
		String[] newEntries = new String[(entries.length > 0 ? entries.length*2 : 2)];
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
			final String[] tmp = new String[length];
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
	
	private void fillFrom(final List<String> list){
		if((list == null) || (list.isEmpty())){
			throw new IllegalArgumentException("Arg must not be null or empty");
		}
		String[] tmp = new String[list.size()];
		Iterator<String> iter = list.iterator();
		int i=0;
		while(iter.hasNext()){
			final String s = iter.next();
			tmp[i++] = (((s == null) || (s.isEmpty())) 
                    ? PLACEHOLDER_EMPTY 
                    : s);
		}
		this.entries = tmp;
	}
}
