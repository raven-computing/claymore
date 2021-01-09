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
 * A Column holding nullable double values.<br>
 * Any values not explicitly set are considered null. This class uses the primitive 
 * wrapper object as the underlying data structure.
 * 
 * @see DoubleColumn
 *
 */
public final class NullableDoubleColumn extends NullableColumn {

    /**
     * The unique type code of all <code>NullableDoubleColumns</code>
     */
    public static final byte TYPE_CODE = (byte)16;

    private Double[] entries;

    /**
     * 	Constructs an empty <code>NullableDoubleColumn</code>.
     */
    public NullableDoubleColumn(){
        this(0);
    }

    /**
     * Constructs a <code>NullableDoubleColumn</code> with the specified length.<br>
     * All column entries are set to null
     * 
     * @param length The initial length of the column to construct
     */
    public NullableDoubleColumn(final int length){
        this.entries = new Double[length];
    }

    /**
     * Constructs an empty <code>NullableDoubleColumn</code> with the specified label.
     * 
     * @param name The name of the column to construct. Must not be null or empty
     */
    public NullableDoubleColumn(final String name){
        this();
        if((name == null) || (name.isEmpty())){
            throw new IllegalArgumentException("Column name must not be null or empty");
        }
        this.name = name;
    }

    /**
     * Constructs a <code>NullableDoubleColumn</code> with the specified label
     * and the specified length.<br>
     * All column entries are set to null
     * 
     * @param name The name of the column to construct. Must not be null or empty
     * @param length The initial length of the column to construct
     */
    public NullableDoubleColumn(final String name, final int length){
        this(length);
        if((name == null) || (name.isEmpty())){
            throw new IllegalArgumentException("Column name must not be null or empty");
        }
        this.name = name;
    }

    /**
     * Constructs a new <code>NullableDoubleColumn</code> composed of the content of 
     * the specified double array 
     * 
     * @param column The entries of the column to be constructed. Must not be null
     */
    public NullableDoubleColumn(final double[] column){
        if(column == null){
            throw new IllegalArgumentException("Arg must not be null");
        }
        Double[] obj = new Double[column.length];
        for(int i=0; i<column.length; ++i){
            obj[i] = column[i];
        }
        this.entries = obj;
    }

    /**
     * Constructs a new labeled <code>NullableDoubleColumn</code> composed of the
     * content of the specified double array
     * 
     * @param name The name of the column to construct. Must not be null or empty
     * @param column The entries of the column to be constructed. Must not be null
     */
    public NullableDoubleColumn(final String name, final double[] column){
        this(name);
        if(column == null){
            throw new IllegalArgumentException("Arg must not be null");
        }
        Double[] obj = new Double[column.length];
        for(int i=0; i<column.length; ++i){
            obj[i] = column[i];
        }
        this.entries = obj;
    }

    /**
     * Constructs a new <code>NullableDoubleColumn</code> composed of the content of 
     * the specified Double array. Individual entries may be null
     * 
     * @param column The entries of the column to be constructed. Must not be null
     */
    public NullableDoubleColumn(final Double[] column){
        if(column == null){
            throw new IllegalArgumentException("Arg must not be null");
        }
        this.entries = column;
    }

    /**
     * Constructs a new labeled <code>NullableDoubleColumn</code> composed of the
     * content of the specified Double array. Individual entries may be null
     * 
     * @param name The name of the column to construct. Must not be null or empty
     * @param column The entries of the column to be constructed. Must not be null
     */
    public NullableDoubleColumn(final String name, final Double[] column){
        this(name);
        if(column == null){
            throw new IllegalArgumentException("Arg must not be null");
        }
        this.entries = column;
    }

    /**
     * Constructs a new <code>NullableDoubleColumn</code> composed of the content of 
     * the specified List. Individual items may be null
     * 
     * @param list The entries of the column to be constructed. Must not be null or empty
     */
    public NullableDoubleColumn(final List<Double> list){
        fillFrom(list);
    }

    /**
     * Constructs a new labeled <code>NullableDoubleColumn</code> composed of the content of 
     * the specified list
     * 
     * @param name The name of the column to construct. Must not be null or empty
     * @param list The list representing the entries of the column to be constructed.
     *             Must not be null or empty
     */
    public NullableDoubleColumn(final String name, final List<Double> list){
        this(name);
        fillFrom(list);
    }

    /**
     * Gets the entry of this column at the specified index
     * 
     * @param index The index of the entry to get
     * @return The Double value at the specified index. May be null
     */
    public Double get(final int index){
        return entries[index];
    }

    /**
     * Sets the entry of this column at the specified index
     * to the given value
     * 
     * @param index The index of the entry to set
     * @param value The Double value to set the entry to. May be null
     */
    public void set(final int index, final Double value){
        entries[index] = value;
    }

    /**
     * Returns a reference to the internal array of this column
     * 
     * @return The internal Double array
     */
    public Double[] asArray(){
        return this.entries;
    }

    @Override
    public Column clone(){
        final Double[] clone = new Double[entries.length];
        for(int i=0; i<entries.length; ++i){
            clone[i] = (entries[i] != null ? new Double(entries[i]) : null);
        }
        return ((name != null) && !name.isEmpty())
                ? new NullableDoubleColumn(name, clone)
                : new NullableDoubleColumn(clone);
    }

    @Override
    public boolean equals(Object obj){
        if(this == obj){
            return true;
        }
        if(!(obj instanceof NullableDoubleColumn)){
            return false;
        }
        final NullableDoubleColumn col = (NullableDoubleColumn)obj;
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
        entries[index] = (Double)value;
    }

    @Override
    public byte typeCode(){
        return TYPE_CODE;
    }

    @Override
    public String typeName(){
        return "double";
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
        return entries.length * 8;
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
                floats[i] = (float) ((entries[i] != null) ? entries[i] : 0.0f);
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
                bools[i] = (entries[i] != null) ? (entries[i] != 0.0) : false;
            }
            converted = new BooleanColumn(bools);
            break;
        case BinaryColumn.TYPE_CODE:
            final byte[][] bins = new byte[entries.length][];
            for(int i=0; i<entries.length; ++i){
                if(entries[i] != null){
                    final long dval = Double.doubleToLongBits(entries[i]);
                    bins[i] = new byte[8];
                    bins[i][0] = (byte) ((dval & 0xff00000000000000L) >> 56);
                    bins[i][1] = (byte) ((dval & 0xff000000000000L) >> 48);
                    bins[i][2] = (byte) ((dval & 0xff0000000000L) >> 40);
                    bins[i][3] = (byte) ((dval & 0xff00000000L) >> 32);
                    bins[i][4] = (byte) ((dval & 0xff000000L) >> 24);
                    bins[i][5] = (byte) ((dval & 0xff0000L) >> 16);
                    bins[i][6] = (byte) ((dval & 0xff00L) >> 8);
                    bins[i][7] = (byte)  (dval & 0xffL);
                }else{
                    bins[i] = new byte[]{0};
                }
            }
            converted = new BinaryColumn(bins);
            break;
        case NullableByteColumn.TYPE_CODE:
            final Byte[] bytesn = new Byte[entries.length];
            for(int i=0; i<entries.length; ++i){
                bytesn[i] = (entries[i] != null) ? (byte) ((double)entries[i]) : null;
            }
            converted = new NullableByteColumn(bytesn);
            break;
        case NullableShortColumn.TYPE_CODE:
            final Short[] shortsn = new Short[entries.length];
            for(int i=0; i<entries.length; ++i){
                shortsn[i] = (entries[i] != null) ? (short) ((double)entries[i]) : null;
            }
            converted = new NullableShortColumn(shortsn);
            break;
        case NullableIntColumn.TYPE_CODE:
            final Integer[] intsn = new Integer[entries.length];
            for(int i=0; i<entries.length; ++i){
                intsn[i] = (entries[i] != null) ? (int) ((double)entries[i]) : null;
            }
            converted = new NullableIntColumn(intsn);
            break;
        case NullableLongColumn.TYPE_CODE:
            final Long[] longsn = new Long[entries.length];
            for(int i=0; i<entries.length; ++i){
                longsn[i] = (entries[i] != null) ? (long) ((double)entries[i]) : null;
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
            final Float[] floatsn = new Float[entries.length];
            for(int i=0; i<entries.length; ++i){
                floatsn[i] = (entries[i] != null) ? (float) ((double)entries[i]) : null;
            }
            converted = new NullableFloatColumn(floatsn);
            break;
        case NullableDoubleColumn.TYPE_CODE:
            converted = this.clone();
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
                boolsn[i] = (entries[i] != null) ? entries[i] != 0.0 : null;
            }
            converted = new NullableBooleanColumn(boolsn);
            break;
        case NullableBinaryColumn.TYPE_CODE:
            final byte[][] binsn = new byte[entries.length][];
            for(int i=0; i<entries.length; ++i){
                if(entries[i] != null){
                    final long dval = Double.doubleToLongBits(entries[i]);
                    binsn[i] = new byte[8];
                    binsn[i][0] = (byte) ((dval & 0xff00000000000000L) >> 56);
                    binsn[i][1] = (byte) ((dval & 0xff000000000000L) >> 48);
                    binsn[i][2] = (byte) ((dval & 0xff0000000000L) >> 40);
                    binsn[i][3] = (byte) ((dval & 0xff00000000L) >> 32);
                    binsn[i][4] = (byte) ((dval & 0xff000000L) >> 24);
                    binsn[i][5] = (byte) ((dval & 0xff0000L) >> 16);
                    binsn[i][6] = (byte) ((dval & 0xff00L) >> 8);
                    binsn[i][7] = (byte)  (dval & 0xffL);
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
        entries[index] = (Double)value;
    }

    @Override
    protected Class<?> memberClass(){
        return Double.class;
    }

    @Override
    protected void resize(){
        Double[] newEntries = new Double[(entries.length > 0 ? entries.length*2 : 2)];
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
            final Double[] tmp = new Double[length];
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

    private void fillFrom(final List<Double> list){
        if((list == null) || (list.isEmpty())){
            throw new IllegalArgumentException("Arg must not be null or empty");
        }
        Double[] tmp = new Double[list.size()];
        Iterator<Double> iter = list.iterator();
        int i=0;
        while(iter.hasNext()){
            tmp[i++] = iter.next();
        }
        this.entries = tmp;
    }
}
