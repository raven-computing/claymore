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

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Column holding byte values.<br>
 * This implementation <b>DOES NOT</b> support null values.
 * 
 * @see NullableByteColumn
 *
 */
public class ByteColumn extends Column {

    /**
     * The unique type code of all <code>ByteColumns</code>
     */
    public static final byte TYPE_CODE = (byte)1;

    private byte[] entries;

    /**
     * Constructs an empty <code>ByteColumn</code>.
     */
    public ByteColumn(){
        this.entries = new byte[0];
    }

    /**
     * Constructs an empty <code>ByteColumn</code> with the specified label.
     * 
     * @param name The name of the column to construct. Must not be null or empty
     */
    public ByteColumn(final String name){
        this();
        if((name == null) || (name.isEmpty())){
            throw new IllegalArgumentException("Column name must not be null or empty");
        }
        this.name = name;
    }

    /**
     * Constructs a new <code>ByteColumn</code> composed of the content of 
     * the specified byte array
     * 
     * @param column The entries of the column to be constructed. Must not be null
     */
    public ByteColumn(final byte[] column){
        if(column == null){
            throw new IllegalArgumentException("Arg must not be null");
        }
        this.entries = column;
    }

    /**
     * Constructs a new labeled <code>ByteColumn</code> composed of the content of 
     * the specified byte array
     * 
     * @param name The name of the column to construct. Must not be null or empty
     * @param column The entries of the column to be constructed. Must not be null
     */
    public ByteColumn(final String name, final byte[] column){
        this(name);
        if(column == null){
            throw new IllegalArgumentException("Arg must not be null");
        }
        this.entries = column;
    }

    /**
     * Constructs a new <code>ByteColumn</code> composed of the content of 
     * the specified list
     * 
     * @param list The list representing the entries of the column to be constructed
     */
    public ByteColumn(final List<Byte> list){
        fillFrom(list);
    }

    /**
     * Constructs a new labeled <code>ByteColumn</code> composed of the content of 
     * the specified list
     * 
     * @param name The name of the column to construct. Must not be null or empty
     * @param list The list representing the entries of the column to be constructed.
     *             Must not be null or empty
     */
    public ByteColumn(final String name, final List<Byte> list){
        this(name);
        fillFrom(list);
    }

    /**
     * Gets the entry of this column at the specified index
     * 
     * @param index The index of the entry to get
     * @return The byte value at the specified index
     */
    public byte get(final int index){
        return entries[index];
    }

    /**
     * Sets the entry of this column at the specified index
     * to the given value
     * 
     * @param index The index of the entry to set
     * @param value The byte value to set the entry to
     */
    public void set(final int index, final byte value){
        entries[index] = value;
    }

    /**
     * Returns a reference to the internal array of this column
     * 
     * @return The internal byte array
     */
    public byte[] asArray(){
        return this.entries;
    }

    @Override
    public Column clone(){
        final byte[] clone = new byte[entries.length];
        for(int i=0; i<entries.length; ++i){
            clone[i] = entries[i];
        }
        return new ByteColumn(clone);
    }

    @Override
    public boolean equals(Object obj){
        if(this == obj){
            return true;
        }
        if(!(obj instanceof ByteColumn)){
            return false;
        }
        final ByteColumn col = (ByteColumn)obj;
        if((this.name == null) ^ (col.name == null)){
            return false;
        }
        if((this.name != null) && (col.name != null)){
            if(!this.name.equals(col.name)){
                return false;
            }
        }
        return Arrays.equals(entries, col.entries);
    }

    @Override
    public int hashCode(){
        return (name != null)
                ? Arrays.hashCode(entries) + name.hashCode() 
                : Arrays.hashCode(entries);
    }

    @Override
    public Object getValueAt(int index){
        return entries[index];
    }

    @Override
    public void setValueAt(int index, Object value){
        entries[index] = (Byte)value;
    }

    @Override
    public byte typeCode(){
        return TYPE_CODE;
    }

    @Override
    public boolean isNullable(){
        return false;
    }

    @Override
    public boolean isNumeric(){
        return true;
    }

    @Override
    protected int capacity(){
        return entries.length;
    }

    @Override
    protected void insertValueAt(int index, int next, Object value){
        for(int i=next; i>index; --i){
            entries[i] = entries[i-1];
        }
        entries[index] = (Byte)value;
    }

    @Override
    protected Class<?> memberClass(){
        return Byte.class;
    }

    @Override
    protected void resize(){
        byte[] newEntries = new byte[(entries.length > 0 ? entries.length*2 : 2)];
        for(int i=0; i<entries.length; ++i){
            newEntries[i] = entries[i];
        }
        this.entries = newEntries;
    }

    @Override
    protected void remove(int from, int to, int next){
        for(int i=from, j=0; j<(next-to); ++i, ++j){
            entries[i] = entries[(to-from)+i];
        }
        for(int i=next-1, j=0; j<(to-from); --i, ++j){
            entries[i] = 0;
        }
    }

    @Override
    protected void matchLength(int length){
        if(length != entries.length){
            final byte[] tmp = new byte[length];
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

    private void fillFrom(final List<Byte> list){
        if((list == null) || (list.isEmpty())){
            throw new IllegalArgumentException("Arg must not be null or empty");
        }
        byte[] tmp = new byte[list.size()];
        Iterator<Byte> iter = list.iterator();
        int i=0;
        while(iter.hasNext()){
            tmp[i++] = iter.next();
        }
        this.entries = tmp;
    }
}
