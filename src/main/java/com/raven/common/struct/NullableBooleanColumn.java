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
 * A Column holding nullable boolean values.<br>
 * Any values not explicitly set are considered null. This class uses the primitive 
 * wrapper object as the underlying data structure.
 * 
 * @see BooleanColumn
 *
 */
public final class NullableBooleanColumn extends NullableColumn {

    /**
     * The unique type code of all <code>NullableBooleanColumns</code>
     */
    public static final byte TYPE_CODE = (byte)18;

    private Boolean[] entries;

    /**
     * 	Constructs an empty <code>NullableBooleanColumn</code>.
     */
    public NullableBooleanColumn(){
        this(0);
    }

    /**
     * Constructs a <code>NullableBooleanColumn</code> with the specified length.<br>
     * All column entries are set to null
     * 
     * @param length The initial length of the column to construct
     */
    public NullableBooleanColumn(final int length){
        this.entries = new Boolean[length];
    }

    /**
     * Constructs an empty <code>NullableBooleanColumn</code> with the specified label.
     * 
     * @param name The name of the column to construct. Must not be null or empty
     */
    public NullableBooleanColumn(final String name){
        this();
        if((name == null) || (name.isEmpty())){
            throw new IllegalArgumentException("Column name must not be null or empty");
        }
        this.name = name;
    }

    /**
     * Constructs a <code>NullableBooleanColumn</code> with the specified label
     * and the specified length.<br>
     * All column entries are set to null
     * 
     * @param name The name of the column to construct. Must not be null or empty
     * @param length The initial length of the column to construct
     */
    public NullableBooleanColumn(final String name, final int length){
        this(length);
        if((name == null) || (name.isEmpty())){
            throw new IllegalArgumentException("Column name must not be null or empty");
        }
        this.name = name;
    }

    /**
     * Constructs a new <code>NullableBooleanColumn</code> composed of the content of 
     * the specified boolean array 
     * 
     * @param column The entries of the column to be constructed. Must not be null
     */
    public NullableBooleanColumn(final boolean[] column){
        if(column == null){
            throw new IllegalArgumentException("Arg must not be null");
        }
        Boolean[] obj = new Boolean[column.length];
        for(int i=0; i<column.length; ++i){
            obj[i] = column[i];
        }
        this.entries = obj;
    }

    /**
     * Constructs a new labeled <code>NullableBooleanColumn</code> composed of the
     * content of the specified boolean array
     * 
     * @param name The name of the column to construct. Must not be null or empty
     * @param column The entries of the column to be constructed. Must not be null
     */
    public NullableBooleanColumn(final String name, final boolean[] column){
        this(name);
        if(column == null){
            throw new IllegalArgumentException("Arg must not be null");
        }
        Boolean[] obj = new Boolean[column.length];
        for(int i=0; i<column.length; ++i){
            obj[i] = column[i];
        }
        this.entries = obj;
    }

    /**
     * Constructs a new <code>NullableBooleanColumn</code> composed of the content of 
     * the specified Boolean array. Individual entries may be null
     * 
     * @param column The entries of the column to be constructed. Must not be null
     */
    public NullableBooleanColumn(final Boolean[] column){
        if(column == null){
            throw new IllegalArgumentException("Arg must not be null");
        }
        this.entries = column;
    }

    /**
     * Constructs a new labeled <code>NullableBooleanColumn</code> composed of the
     * content of the specified Boolean array. Individual entries may be null
     * 
     * @param name The name of the column to construct. Must not be null or empty
     * @param column The entries of the column to be constructed. Must not be null
     */
    public NullableBooleanColumn(final String name, final Boolean[] column){
        this(name);
        if(column == null){
            throw new IllegalArgumentException("Arg must not be null");
        }
        this.entries = column;
    }

    /**
     * Constructs a new <code>NullableBooleanColumn</code> composed of the content of 
     * the specified List. Individual items may be null
     * 
     * @param list The entries of the column to be constructed. Must not be null or empty
     */
    public NullableBooleanColumn(final List<Boolean> list){
        fillFrom(list);
    }

    /**
     * Constructs a new labeled <code>NullableBooleanColumn</code> composed of the content of 
     * the specified list
     * 
     * @param name The name of the column to construct. Must not be null or empty
     * @param list The list representing the entries of the column to be constructed.
     *             Must not be null or empty
     */
    public NullableBooleanColumn(final String name, final List<Boolean> list){
        this(name);
        fillFrom(list);
    }

    /**
     * Gets the entry of this column at the specified index
     * 
     * @param index The index of the entry to get
     * @return The Boolean value at the specified index. May be null
     */
    public Boolean get(final int index){
        return entries[index];
    }

    /**
     * Sets the entry of this column at the specified index
     * to the given value
     * 
     * @param index The index of the entry to set
     * @param value The Boolean value to set the entry to. May be null
     */
    public void set(final int index, final Boolean value){
        entries[index] = value;
    }

    /**
     * Returns a reference to the internal array of this column
     * 
     * @return The internal Boolean array
     */
    public Boolean[] asArray(){
        return this.entries;
    }

    @Override
    public Column clone(){
        final Boolean[] clone = new Boolean[entries.length];
        for(int i=0; i<entries.length; ++i){
            clone[i] = (entries[i] != null ? Boolean.valueOf(entries[i]) : null);
        }
        return ((name != null) && !name.isEmpty())
                ? new NullableBooleanColumn(name, clone)
                : new NullableBooleanColumn(clone);
    }

    @Override
    public boolean equals(Object obj){
        if(this == obj){
            return true;
        }
        if(!(obj instanceof NullableBooleanColumn)){
            return false;
        }
        final NullableBooleanColumn col = (NullableBooleanColumn)obj;
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
    public Object getValue(int index){
        return entries[index];
    }

    @Override
    public void setValue(int index, Object value){
        entries[index] = (Boolean)value;
    }

    @Override
    public byte typeCode(){
        return TYPE_CODE;
    }

    @Override
    public String typeName(){
        return "boolean";
    }

    @Override
    public int capacity(){
        return entries.length;
    }

    @Override
    public boolean isNumeric(){
        return false;
    }

    @Override
    public int memoryUsage(){
        return (entries.length / 8) + ((entries.length % 8 != 0) ? 1 : 0);
    }

    @Override
    public Column convertTo(byte typeCode){
        Column converted = null;
        switch(typeCode){
        case ByteColumn.TYPE_CODE:
            final byte[] bytes = new byte[entries.length];
            for(int i=0; i<entries.length; ++i){
                if(entries[i] != null){
                    bytes[i] = (byte) (entries[i] ? 1 : 0);
                }else{
                    bytes[i] = 0;
                }
            }
            converted = new ByteColumn(bytes);
            break;
        case ShortColumn.TYPE_CODE:
            final short[] shorts = new short[entries.length];
            for(int i=0; i<entries.length; ++i){
                if(entries[i] != null){
                    shorts[i] = (short) (entries[i] ? 1 : 0);
                }else{
                    shorts[i] = 0;
                }
            }
            converted = new ShortColumn(shorts);
            break;
        case IntColumn.TYPE_CODE:
            final int[] ints = new int[entries.length];
            for(int i=0; i<entries.length; ++i){
                if(entries[i] != null){
                    ints[i] = (entries[i] ? 1 : 0);
                }else{
                    ints[i] = 0;
                }
            }
            converted = new IntColumn(ints);
            break;
        case LongColumn.TYPE_CODE:
            final long[] longs = new long[entries.length];
            for(int i=0; i<entries.length; ++i){
                if(entries[i] != null){
                    longs[i] = (entries[i] ? 1l : 0l);
                }else{
                    longs[i] = 0;
                }
            }
            converted = new LongColumn(longs);
            break;
        case StringColumn.TYPE_CODE:
            final String[] strings = new String[entries.length];
            for(int i=0; i<entries.length; ++i){
                if(entries[i] != null){
                    strings[i] = String.valueOf(entries[i]);
                }else{
                    strings[i] = StringColumn.DEFAULT_VALUE;
                }
            }
            converted = new StringColumn(strings);
            break;
        case FloatColumn.TYPE_CODE:
            final float[] floats = new float[entries.length];
            for(int i=0; i<entries.length; ++i){
                if(entries[i] != null){
                    floats[i] = (entries[i] ? 1.0f : 0.0f);
                }else{
                    floats[i] = 0.0f;
                }
            }
            converted = new FloatColumn(floats);
            break;
        case DoubleColumn.TYPE_CODE:
            final double[] doubles = new double[entries.length];
            for(int i=0; i<entries.length; ++i){
                if(entries[i] != null){
                    doubles[i] = (entries[i] ? 1.0 : 0.0);
                }else{
                    doubles[i] = 0.0;
                }
            }
            converted = new DoubleColumn(doubles);
            break;
        case CharColumn.TYPE_CODE:
            final char[] chars = new char[entries.length];
            for(int i=0; i<entries.length; ++i){
                if(entries[i] != null){
                    chars[i] = (entries[i] ? '1' : '0');
                }else{
                    chars[i] = CharColumn.DEFAULT_VALUE;
                }
            }
            converted = new CharColumn(chars);
            break;
        case BooleanColumn.TYPE_CODE:
            final boolean[] bools = new boolean[entries.length];
            for(int i=0; i<entries.length; ++i){
                if(entries[i] != null){
                    bools[i] = entries[i];
                }else{
                    bools[i] = false;
                }
            }
            converted = new BooleanColumn(bools);
            break;
        case BinaryColumn.TYPE_CODE:
            final byte[][] bins = new byte[entries.length][];
            for(int i=0; i<entries.length; ++i){
                if(entries[i] != null){
                    bins[i] = (entries[i] ? new byte[]{(byte)1} : new byte[]{(byte)0});
                }else{
                    bins[i] = new byte[]{(byte)0};
                }
            }
            converted = new BinaryColumn(bins);
            break;
        case NullableByteColumn.TYPE_CODE:
            final Byte[] bytesn = new Byte[entries.length];
            for(int i=0; i<entries.length; ++i){
                if(entries[i] != null){
                    bytesn[i] = (byte) (entries[i] ? 1 : 0);
                }else{
                    bytesn[i] = null;
                }
            }
            converted = new NullableByteColumn(bytesn);
            break;
        case NullableShortColumn.TYPE_CODE:
            final Short[] shortsn = new Short[entries.length];
            for(int i=0; i<entries.length; ++i){
                if(entries[i] != null){
                    shortsn[i] = (short) (entries[i] ? 1 : 0);
                }else{
                    shortsn[i] = null;
                }
            }
            converted = new NullableShortColumn(shortsn);
            break;
        case NullableIntColumn.TYPE_CODE:
            final Integer[] intsn = new Integer[entries.length];
            for(int i=0; i<entries.length; ++i){
                if(entries[i] != null){
                    intsn[i] = (entries[i] ? 1 : 0);
                }else{
                    intsn[i] = null;
                }
            }
            converted = new NullableIntColumn(intsn);
            break;
        case NullableLongColumn.TYPE_CODE:
            final Long[] longsn = new Long[entries.length];
            for(int i=0; i<entries.length; ++i){
                if(entries[i] != null){
                    longsn[i] = (entries[i] ? 1l : 0l);
                }else{
                    longsn[i] = null;
                }
            }
            converted = new NullableLongColumn(longsn);
            break;
        case NullableStringColumn.TYPE_CODE:
            final String[] stringsn = new String[entries.length];
            for(int i=0; i<entries.length; ++i){
                if(entries[i] != null){
                    stringsn[i] = String.valueOf(entries[i]);
                }else{
                    stringsn[i] = null;
                }
            }
            converted = new NullableStringColumn(stringsn);
            break;
        case NullableFloatColumn.TYPE_CODE:
            final Float[] floatsn = new Float[entries.length];
            for(int i=0; i<entries.length; ++i){
                if(entries[i] != null){
                    floatsn[i] = (entries[i] ? 1.0f : 0.0f);
                }else{
                    floatsn[i] = null;
                }
            }
            converted = new NullableFloatColumn(floatsn);
            break;
        case NullableDoubleColumn.TYPE_CODE:
            final Double[] doublesn = new Double[entries.length];
            for(int i=0; i<entries.length; ++i){
                if(entries[i] != null){
                    doublesn[i] = (entries[i] ? 1.0 : 0.0);
                }else{
                    doublesn[i] = null;
                }
            }
            converted = new NullableDoubleColumn(doublesn);
            break;
        case NullableCharColumn.TYPE_CODE:
            final Character[] charsn = new Character[entries.length];
            for(int i=0; i<entries.length; ++i){
                if(entries[i] != null){
                    charsn[i] = (entries[i] ? '1' : '0');
                }else{
                    charsn[i] = null;
                }
            }
            converted = new NullableCharColumn(charsn);
            break;
        case NullableBooleanColumn.TYPE_CODE:
            converted = this.clone();
            break;
        case NullableBinaryColumn.TYPE_CODE:
            final byte[][] binsn = new byte[entries.length][];
            for(int i=0; i<entries.length; ++i){
                if(entries[i] != null){
                    binsn[i] = (entries[i] ? new byte[]{(byte)1} : new byte[]{(byte)0});
                }else{
                    binsn[i] = null;
                }
            }
            converted = new NullableBinaryColumn(binsn);
            break;
        default:
            throw new DataFrameException("Unknown column type code: " + typeCode);
        }
        converted.name = this.name;
        return converted;
    }

    @Override
    protected void insertValueAt(int index, int next, Object value){
        for(int i=next; i>index; --i){
            entries[i] = entries[i-1];
        }
        entries[index] = (Boolean)value;
    }

    @Override
    protected Class<?> memberClass(){
        return Boolean.class;
    }

    @Override
    protected void resize(){
        Boolean[] newEntries = new Boolean[(entries.length > 0 ? entries.length*2 : 2)];
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
            final Boolean[] tmp = new Boolean[length];
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
        Boolean[] tmp = new Boolean[list.size()];
        Iterator<Boolean> iter = list.iterator();
        int i=0;
        while(iter.hasNext()){
            tmp[i++] = iter.next();
        }
        this.entries = tmp;
    }
}
