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

/**
 * Column holding nullable binary data of arbitrary length.<br>
 * Any values not explicitly set are considered null.
 * 
 * @see BinaryColumn
 *
 */
public class NullableBinaryColumn extends NullableColumn {

    /**
     * The unique type code of all <code>NullableBinaryColumns</code>
     */
    public static final byte TYPE_CODE = (byte)20;

    private byte[][] entries;

    /**
     * Constructs an empty <code>NullableBinaryColumn</code>.
     */
    public NullableBinaryColumn(){
        this.entries = new byte[0][0];
    }

    /**
     * Constructs an empty <code>NullableBinaryColumn</code> with the specified label.
     * 
     * @param name The name of the column to construct. Must not be null or empty
     */
    public NullableBinaryColumn(final String name){
        this();
        if((name == null) || (name.isEmpty())){
            throw new IllegalArgumentException("Column name must not be null or empty");
        }
        this.name = name;
    }

    /**
     * Constructs a new <code>NullableBinaryColumn</code> composed of the content of 
     * the specified byte array
     * 
     * @param column The entries of the column to be constructed. Must not be null
     */
    public NullableBinaryColumn(final byte[][] column){
        if(column == null){
            throw new IllegalArgumentException("Arg must not be null");
        }
        checkNonEmptyContent(column);
        this.entries = column;
    }

    /**
     * Constructs a new labeled <code>NullableBinaryColumn</code> composed of the content of 
     * the specified byte array
     * 
     * @param name The name of the column to construct. Must not be null or empty
     * @param column The entries of the column to be constructed. Must not be null
     */
    public NullableBinaryColumn(final String name, final byte[][] column){
        this(name);
        if(column == null){
            throw new IllegalArgumentException("Arg must not be null");
        }
        checkNonEmptyContent(column);
        this.entries = column;
    }

    /**
     * Gets the entry of this column at the specified index
     * 
     * @param index The index of the entry to get
     * @return The byte array at the specified index
     */
    public byte[] get(final int index){
        return entries[index];
    }

    /**
     * Sets the entry of this column at the specified index
     * to the given value
     * 
     * @param index The index of the entry to set
     * @param value The byte array to set the entry to
     */
    public void set(final int index, final byte[] value){
        if((value != null) && (value.length == 0)){
            throw new IllegalArgumentException(
                    "NullableBinaryColumn cannot use empty values");

        }
        entries[index] = value;
    }

    /**
     * Returns a reference to the internal array of this column
     * 
     * @return The internal byte array
     */
    public byte[][] asArray(){
        return this.entries;
    }

    @Override
    public Column clone(){
        final byte[][] clone = new byte[entries.length][0];
        for(int i=0; i<entries.length; ++i){
            if(entries[i] != null){
                final byte[] data = new byte[entries[i].length];
                for(int j=0; j<data.length; ++j){
                    data[j] = entries[i][j];
                }
                clone[i] = data;
            }else{
                clone[i] = null;
            }
        }
        return new NullableBinaryColumn(clone);
    }

    @Override
    public boolean equals(Object obj){
        if(this == obj){
            return true;
        }
        if(!(obj instanceof NullableBinaryColumn)){
            return false;
        }
        final NullableBinaryColumn col = (NullableBinaryColumn)obj;
        if((this.name == null) ^ (col.name == null)){
            return false;
        }
        if((this.name != null) && (col.name != null)){
            if(!this.name.equals(col.name)){
                return false;
            }
        }
        return Arrays.deepEquals(entries, col.entries);
    }

    @Override
    public int hashCode(){
        return (name != null)
                ? Arrays.deepHashCode(entries) + name.hashCode() 
                : Arrays.deepHashCode(entries);
    }

    @Override
    public Object getValueAt(int index){
        return entries[index];
    }

    @Override
    public void setValueAt(int index, Object value){
        final byte[] data = (byte[])value; 
        if((value != null) && (data.length == 0)){
            throw new IllegalArgumentException(
                    "NullableBinaryColumn cannot use empty values");

        }
        entries[index] = data;
    }

    @Override
    public byte typeCode(){
        return TYPE_CODE;
    }

    @Override
    public boolean isNumeric(){
        return false;
    }

    @Override
    protected int capacity(){
        return entries.length;
    }

    @Override
    protected void insertValueAt(int index, int next, Object value){
        final byte[] data = (byte[])value;
        if((value != null) && (data.length == 0)){
            throw new IllegalArgumentException(
                    "NullableBinaryColumn cannot use empty values");

        }
        for(int i=next; i>index; --i){
            entries[i] = entries[i-1];
        }
        entries[index] = data;
    }

    @Override
    protected Class<?> memberClass(){
        return byte[].class;
    }

    @Override
    protected void resize(){
        byte[][] newEntries = new byte[(entries.length > 0 ? entries.length*2 : 2)][0];
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
            entries[i] = null;
        }
    }

    @Override
    protected void matchLength(int length){
        if(length != entries.length){
            final byte[][] tmp = new byte[length][0];
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

    private void checkNonEmptyContent(final byte[][] column){
        for(int i=0; i<column.length; ++i){
            if((column[i] != null) && (column[i].length == 0)){
                throw new IllegalArgumentException(
                        "NullableBinaryColumn cannot use empty values (at index "
                                + i + ")");

            }
        }
    }
}