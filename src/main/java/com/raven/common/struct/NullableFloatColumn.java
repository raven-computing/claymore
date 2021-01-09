/* 
 * Copyright (C) 2021 Raven Computing
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
 * A Column holding nullable float values.<br>
 * Any values not explicitly set are considered null. This class uses the primitive 
 * wrapper object as the underlying data structure.
 * 
 * @see FloatColumn
 *
 */
public final class NullableFloatColumn extends NullableColumn {

    /**
     * The unique type code of all <code>NullableFloatColumns</code>
     */
    public static final byte TYPE_CODE = (byte)15;

    private Float[] entries;

    /**
     * 	Constructs an empty <code>NullableFloatColumn</code>.
     */
    public NullableFloatColumn(){
        this(0);
    }

    /**
     * Constructs a <code>NullableFloatColumn</code> with the specified length.<br>
     * All column entries are set to null
     * 
     * @param length The initial length of the column to construct
     */
    public NullableFloatColumn(final int length){
        this.entries = new Float[length];
    }

    /**
     * Constructs an empty <code>NullableFloatColumn</code> with the specified label.
     * 
     * @param name The name of the column to construct. Must not be null or empty
     */
    public NullableFloatColumn(final String name){
        this();
        if((name == null) || (name.isEmpty())){
            throw new IllegalArgumentException("Column name must not be null or empty");
        }
        this.name = name;
    }

    /**
     * Constructs a <code>NullableFloatColumn</code> with the specified label
     * and the specified length.<br>
     * All column entries are set to null
     * 
     * @param name The name of the column to construct. Must not be null or empty
     * @param length The initial length of the column to construct
     */
    public NullableFloatColumn(final String name, final int length){
        this(length);
        if((name == null) || (name.isEmpty())){
            throw new IllegalArgumentException("Column name must not be null or empty");
        }
        this.name = name;
    }

    /**
     * Constructs a new <code>NullableFloatColumn</code> composed of the content of 
     * the specified float array 
     *
     * @param column The entries of the column to be constructed. Must not be null
     */
    public NullableFloatColumn(final float[] column){
        if(column == null){
            throw new IllegalArgumentException("Arg must not be null");
        }
        Float[] obj = new Float[column.length];
        for(int i=0; i<column.length; ++i){
            obj[i] = column[i];
        }
        this.entries = obj;
    }

    /**
     * Constructs a new labeled <code>NullableFloatColumn</code> composed of the
     * content of the specified float array
     * 
     * @param name The name of the column to construct. Must not be null or empty
     * @param column The entries of the column to be constructed. Must not be null
     */
    public NullableFloatColumn(final String name, final float[] column){
        this(name);
        if(column == null){
            throw new IllegalArgumentException("Arg must not be null");
        }
        Float[] obj = new Float[column.length];
        for(int i=0; i<column.length; ++i){
            obj[i] = column[i];
        }
        this.entries = obj;
    }

    /**
     * Constructs a new <code>NullableFloatColumn</code> composed of the content of 
     * the specified Float array. Individual entries may be null
     * 
     * @param column The entries of the column to be constructed. Must not be null
     */
    public NullableFloatColumn(final Float[] column){
        if(column == null){
            throw new IllegalArgumentException("Arg must not be null");
        }
        this.entries = column;
    }

    /**
     * Constructs a new labeled <code>NullableFloatColumn</code> composed of the
     * content of the specified Float array. Individual entries may be null
     * 
     * @param name The name of the column to construct. Must not be null or empty
     * @param column The entries of the column to be constructed. Must not be null
     */
    public NullableFloatColumn(final String name, final Float[] column){
        this(name);
        if(column == null){
            throw new IllegalArgumentException("Arg must not be null");
        }
        this.entries = column;
    }

    /**
     * Constructs a new <code>NullableFloatColumn</code> composed of the content of 
     * the specified List. Individual items may be null
     * 
     * @param list The entries of the column to be constructed. Must not be null or empty
     */
    public NullableFloatColumn(final List<Float> list){
        fillFrom(list);
    }

    /**
     * Constructs a new labeled <code>NullableFloatColumn</code> composed of the content of 
     * the specified list
     * 
     * @param name The name of the column to construct. Must not be null or empty
     * @param list The list representing the entries of the column to be constructed.
     *             Must not be null or empty
     */
    public NullableFloatColumn(final String name, final List<Float> list){
        this(name);
        fillFrom(list);
    }

    /**
     * Gets the entry of this column at the specified index
     * 
     * @param index The index of the entry to get
     * @return The Float value at the specified index. May be null
     */
    public Float get(final int index){
        return entries[index];
    }

    /**
     * Sets the entry of this column at the specified index
     * to the given value
     * 
     * @param index The index of the entry to set
     * @param value The Float value to set the entry to. May be null
     */
    public void set(final int index, final Float value){
        entries[index] = value;
    }

    /**
     * Returns a reference to the internal array of this column
     * 
     * @return The internal Float array
     */
    public Float[] asArray(){
        return this.entries;
    }

    @Override
    public Column clone(){
        final Float[] clone = new Float[entries.length];
        for(int i=0; i<entries.length; ++i){
            clone[i] = (entries[i] != null ? new Float(entries[i]) : null);
        }
        return ((name != null) && !name.isEmpty())
                ? new NullableFloatColumn(name, clone)
                : new NullableFloatColumn(clone);
    }

    @Override
    public boolean equals(Object obj){
        if(this == obj){
            return true;
        }
        if(!(obj instanceof NullableFloatColumn)){
            return false;
        }
        final NullableFloatColumn col = (NullableFloatColumn)obj;
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
        entries[index] = (Float)value;
    }

    @Override
    public byte typeCode(){
        return TYPE_CODE;
    }

    @Override
    public String typeName(){
        return "float";
    }

    @Override
    public int capacity(){
        return entries.length;
    }

    @Override
    public boolean isNumeric(){
        return true;
    }

    @Override
    public int memoryUsage(){
        return entries.length * 4;
    }

    @Override
    public Column convertTo(byte typeCode){
        Column converted = null;
        switch(typeCode){
        case ByteColumn.TYPE_CODE:
            final byte[] bytes = new byte[entries.length];
            for(int i=0; i<entries.length; ++i){
                bytes[i] = (byte) ((entries[i] != null) ? entries[i] : 0);
            }
            converted = new ByteColumn(bytes);
            break;
        case ShortColumn.TYPE_CODE:
            final short[] shorts = new short[entries.length];
            for(int i=0; i<entries.length; ++i){
                shorts[i] = (short) ((entries[i] != null) ? entries[i] : 0);
            }
            converted = new ShortColumn(shorts);
            break;
        case IntColumn.TYPE_CODE:
            final int[] ints = new int[entries.length];
            for(int i=0; i<entries.length; ++i){
                ints[i] = (int) ((entries[i] != null) ? entries[i] : 0);
            }
            converted = new IntColumn(ints);
            break;
        case LongColumn.TYPE_CODE:
            final long[] longs = new long[entries.length];
            for(int i=0; i<entries.length; ++i){
                longs[i] = (long) ((entries[i] != null) ? entries[i] : 0l);
            }
            converted = new LongColumn(longs);
            break;
        case StringColumn.TYPE_CODE:
            final String[] strings = new String[entries.length];
            for(int i=0; i<entries.length; ++i){
                strings[i] = String.valueOf((entries[i] != null)
                        ? entries[i]
                        : StringColumn.DEFAULT_VALUE);
            }
            converted = new StringColumn(strings);
            break;
        case FloatColumn.TYPE_CODE:
            final float[] floats = new float[entries.length];
            for(int i=0; i<entries.length; ++i){
                floats[i] = (entries[i] != null) ? entries[i] : 0.0f;
            }
            converted = new FloatColumn(floats);
            break;
        case DoubleColumn.TYPE_CODE:
            final double[] doubles = new double[entries.length];
            for(int i=0; i<entries.length; ++i){
                doubles[i] = (entries[i] != null) ? entries[i] : 0.0;
            }
            converted = new DoubleColumn(doubles);
            break;
        case CharColumn.TYPE_CODE:
            final char[] chars = new char[entries.length];
            for(int i=0; i<entries.length; ++i){
                chars[i] = (entries[i] != null)
                        ? String.valueOf(entries[i]).charAt(0)
                        : CharColumn.DEFAULT_VALUE;
            }
            converted = new CharColumn(chars);
            break;
        case BooleanColumn.TYPE_CODE:
            final boolean[] bools = new boolean[entries.length];
            for(int i=0; i<entries.length; ++i){
                bools[i] = (entries[i] != null) ? (entries[i] != 0.0f) : false;
            }
            converted = new BooleanColumn(bools);
            break;
        case BinaryColumn.TYPE_CODE:
            final byte[][] bins = new byte[entries.length][];
            for(int i=0; i<entries.length; ++i){
                if(entries[i] != null){
                    final int fval = Float.floatToIntBits(entries[i]);
                    bins[i] = new byte[4];
                    bins[i][0] = (byte) ((fval & 0xff000000) >> 24);
                    bins[i][1] = (byte) ((fval & 0xff0000) >> 16);
                    bins[i][2] = (byte) ((fval & 0xff00) >> 8);
                    bins[i][3] = (byte)  (fval & 0xff);
                }else{
                    bins[i] = new byte[]{0};
                }
            }
            converted = new BinaryColumn(bins);
            break;
        case NullableByteColumn.TYPE_CODE:
            final Byte[] bytesn = new Byte[entries.length];
            for(int i=0; i<entries.length; ++i){
                bytesn[i] = (entries[i] != null) ? (byte) ((float)entries[i]) : null;
            }
            converted = new NullableByteColumn(bytesn);
            break;
        case NullableShortColumn.TYPE_CODE:
            final Short[] shortsn = new Short[entries.length];
            for(int i=0; i<entries.length; ++i){
                shortsn[i] = (entries[i] != null) ? (short) ((float)entries[i]) : null;
            }
            converted = new NullableShortColumn(shortsn);
            break;
        case NullableIntColumn.TYPE_CODE:
            final Integer[] intsn = new Integer[entries.length];
            for(int i=0; i<entries.length; ++i){
                intsn[i] = (entries[i] != null) ? (int) ((float)entries[i]) : null;
            }
            converted = new NullableIntColumn(intsn);
            break;
        case NullableLongColumn.TYPE_CODE:
            final Long[] longsn = new Long[entries.length];
            for(int i=0; i<entries.length; ++i){
                longsn[i] = (entries[i] != null) ? (long) ((float)entries[i]) : null;
            }
            converted = new NullableLongColumn(longsn);
            break;
        case NullableStringColumn.TYPE_CODE:
            final String[] stringsn = new String[entries.length];
            for(int i=0; i<entries.length; ++i){
                stringsn[i] = (entries[i] != null)
                           ? String.valueOf(entries[i])
                           : null;
            }
            converted = new NullableStringColumn(stringsn);
            break;
        case NullableFloatColumn.TYPE_CODE:
            converted = this.clone();
            break;
        case NullableDoubleColumn.TYPE_CODE:
            final Double[] doublesn = new Double[entries.length];
            for(int i=0; i<entries.length; ++i){
                doublesn[i] = (entries[i] != null) ? (double) entries[i] : null;
            }
            converted = new NullableDoubleColumn(doublesn);
            break;
        case NullableCharColumn.TYPE_CODE:
            final Character[] charsn = new Character[entries.length];
            for(int i=0; i<entries.length; ++i){
                charsn[i] = (entries[i] != null)
                         ? String.valueOf(entries[i]).charAt(0)
                         : null;
            }
            converted = new NullableCharColumn(charsn);
            break;
        case NullableBooleanColumn.TYPE_CODE:
            final Boolean[] boolsn = new Boolean[entries.length];
            for(int i=0; i<entries.length; ++i){
                boolsn[i] = (entries[i] != null) ? entries[i] != 0.0f : null;
            }
            converted = new NullableBooleanColumn(boolsn);
            break;
        case NullableBinaryColumn.TYPE_CODE:
            final byte[][] binsn = new byte[entries.length][];
            for(int i=0; i<entries.length; ++i){
                if(entries[i] != null){
                    final int fval = Float.floatToIntBits(entries[i]);
                    binsn[i] = new byte[4];
                    binsn[i][0] = (byte) ((fval & 0xff000000) >> 24);
                    binsn[i][1] = (byte) ((fval & 0xff0000) >> 16);
                    binsn[i][2] = (byte) ((fval & 0xff00) >> 8);
                    binsn[i][3] = (byte)  (fval & 0xff);
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
        entries[index] = (Float)value;
    }

    @Override
    protected Class<?> memberClass(){
        return Float.class;
    }

    @Override
    protected void resize(){
        Float[] newEntries = new Float[(entries.length > 0 ? entries.length*2 : 2)];
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
            final Float[] tmp = new Float[length];
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

    private void fillFrom(final List<Float> list){
        if((list == null) || (list.isEmpty())){
            throw new IllegalArgumentException("Arg must not be null or empty");
        }
        Float[] tmp = new Float[list.size()];
        Iterator<Float> iter = list.iterator();
        int i=0;
        while(iter.hasNext()){
            tmp[i++] = iter.next();
        }
        this.entries = tmp;
    }
}
