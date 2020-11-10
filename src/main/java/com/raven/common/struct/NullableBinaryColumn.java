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
 * A Column holding nullable binary data of arbitrary length.<br>
 * Any values not explicitly set are considered null.
 * 
 * @see BinaryColumn
 *
 */
public final class NullableBinaryColumn extends NullableColumn {

    /**
     * The unique type code of all <code>NullableBinaryColumns</code>
     */
    public static final byte TYPE_CODE = (byte)20;

    private byte[][] entries;

    /**
     * Constructs an empty <code>NullableBinaryColumn</code>.
     */
    public NullableBinaryColumn(){
        this(0);
    }

    /**
     * Constructs a <code>NullableBinaryColumn</code> with the specified length.<br>
     * All column entries are set to null
     * 
     * @param length The initial length of the column to construct
     */
    public NullableBinaryColumn(final int length){
        this.entries = new byte[length][];
        for(int i=0; i<length; ++i){
            this.entries[i] = null;
        }
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
     * Constructs a <code>NullableBinaryColumn</code> with the specified label
     * and the specified length.<br>
     * All column entries are set to null
     * 
     * @param name The name of the column to construct. Must not be null or empty
     * @param length The initial length of the column to construct
     */
    public NullableBinaryColumn(final String name, final int length){
        this(length);
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
                final byte[] tmp = entries[i];//cache
                final byte[] data = new byte[tmp.length];
                for(int j=0; j<data.length; ++j){
                    data[j] = tmp[j];
                }
                clone[i] = data;
            }else{
                clone[i] = null;
            }
        }
        return ((name != null) && !name.isEmpty())
                ? new NullableBinaryColumn(name, clone)
                : new NullableBinaryColumn(clone);
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
    public Object getValue(int index){
        return entries[index];
    }

    @Override
    public void setValue(int index, Object value){
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
    public String typeName(){
        return "binary";
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
        int size = 0;
        for(int i=0; i<entries.length; ++i){
            if((entries[i] == null) || entries[i].length == 0){
                size += 1;
            }else{
                size += entries[i].length;
            }
        }
        return size;
    }

    @Override
    public Column convertTo(byte typeCode){
        Column converted = null;
        switch(typeCode){
        case ByteColumn.TYPE_CODE:
            final byte[] bytes = new byte[entries.length];
            for(int i=0; i<entries.length; ++i){
                if((entries[i] != null) && (entries[i].length > 0)){
                    bytes[i] = entries[i][0];
                }else{
                    bytes[i] = 0;
                }
            }
            converted = new ByteColumn(bytes);
            break;
        case ShortColumn.TYPE_CODE:
            final short[] shorts = new short[entries.length];
            for(int i=0; i<entries.length; ++i){
                if((entries[i] != null) && (entries[i].length >= 2)){
                    shorts[i] = (short) (((entries[i][0] & 0xff) << 8)
                                        | (entries[i][1] & 0xff));
                }else{
                    shorts[i] = 0;
                }
            }
            converted = new ShortColumn(shorts);
            break;
        case IntColumn.TYPE_CODE:
            final int[] ints = new int[entries.length];
            for(int i=0; i<entries.length; ++i){
                if((entries[i] != null) && (entries[i].length >= 4)){
                    ints[i] = (((entries[i][0] & 0xff) << 24)
                             | ((entries[i][1] & 0xff) << 16)
                             | ((entries[i][2] & 0xff) << 8)
                             |  (entries[i][3] & 0xff));
                }else{
                    ints[i] = 0;
                }
            }
            converted = new IntColumn(ints);
            break;
        case LongColumn.TYPE_CODE:
            final long[] longs = new long[entries.length];
            for(int i=0; i<entries.length; ++i){
                if((entries[i] != null) && (entries[i].length >= 8)){
                    longs[i] = (((entries[i][0] & 0xffL) << 56)
                              | ((entries[i][1] & 0xffL) << 48)
                              | ((entries[i][2] & 0xffL) << 40)
                              | ((entries[i][3] & 0xffL) << 32)
                              | ((entries[i][4] & 0xffL) << 24)
                              | ((entries[i][5] & 0xffL) << 16)
                              | ((entries[i][6] & 0xffL) << 8)
                              |  (entries[i][7] & 0xffL));
                }else{
                    longs[i] = 0l;
                }
            }
            converted = new LongColumn(longs);
            break;
        case StringColumn.TYPE_CODE:
            final String[] strings = new String[entries.length];
            for(int i=0; i<entries.length; ++i){
                if((entries[i] != null) && (entries[i].length > 0)){
                    strings[i] = BitVector.wrap(entries[i]).toHexString();
                }else{
                    strings[i] = StringColumn.DEFAULT_VALUE;
                }
            }
            converted = new StringColumn(strings);
            break;
        case FloatColumn.TYPE_CODE:
            final float[] floats = new float[entries.length];
            for(int i=0; i<entries.length; ++i){
                if((entries[i] != null) && (entries[i].length >= 4)){
                    floats[i] = Float.intBitsToFloat(
                            ((entries[i][0] & 0xff) << 24)
                          | ((entries[i][1] & 0xff) << 16)
                          | ((entries[i][2] & 0xff) << 8)
                          |  (entries[i][3] & 0xff));
                }else{
                    floats[i] = 0.0f;
                }
            }
            converted = new FloatColumn(floats);
            break;
        case DoubleColumn.TYPE_CODE:
            final double[] doubles = new double[entries.length];
            for(int i=0; i<entries.length; ++i){
                if((entries[i] != null) && (entries[i].length >= 8)){
                    doubles[i] = Double.longBitsToDouble(
                            ((entries[i][0] & 0xffL) << 56)
                          | ((entries[i][1] & 0xffL) << 48)
                          | ((entries[i][2] & 0xffL) << 40)
                          | ((entries[i][3] & 0xffL) << 32)
                          | ((entries[i][4] & 0xffL) << 24)
                          | ((entries[i][5] & 0xffL) << 16)
                          | ((entries[i][6] & 0xffL) << 8)
                          |  (entries[i][7] & 0xffL));
                }else{
                    doubles[i] = 0.0;
                }
            }
            converted = new DoubleColumn(doubles);
            break;
        case CharColumn.TYPE_CODE:
            final char[] chars = new char[entries.length];
            for(int i=0; i<entries.length; ++i){
                if((entries[i] != null) && (entries[i].length > 0)){
                    chars[i] = (char) entries[i][0];
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
                    boolean isZero = true;
                    for(int j=0; j<entries[i].length; ++j){
                        if(entries[i][j] != 0){
                            isZero = false;
                            break;
                        }
                        bools[i] = !isZero;
                    }
                }else{
                    bools[i] = false;
                }
            }
            converted = new BooleanColumn(bools);
            break;
        case BinaryColumn.TYPE_CODE:
            final byte[][] bins = new byte[entries.length][];
            for(int i=0; i<entries.length; ++i){
                if((entries[i] != null) && (entries[i].length > 0)){
                    final byte[] bval = new byte[entries[i].length];
                    for(int j=0; j<entries[i].length; ++j){
                        bval[j] = entries[i][j];
                    }
                    bins[i] = bval;
                }else{
                    bins[i] = new byte[]{0};
                }
            }
            converted = new BinaryColumn(bins);
            break;
        case NullableByteColumn.TYPE_CODE:
            final Byte[] bytesn = new Byte[entries.length];
            for(int i=0; i<entries.length; ++i){
                if((entries[i] != null) && (entries[i].length > 0)){
                    bytesn[i] = entries[i][0];
                }else{
                    bytesn[i] = null;
                }
            }
            converted = new NullableByteColumn(bytesn);
            break;
        case NullableShortColumn.TYPE_CODE:
            final Short[] shortsn = new Short[entries.length];
            for(int i=0; i<entries.length; ++i){
                if((entries[i] != null) && (entries[i].length >= 2)){
                    shortsn[i] = (short) (((entries[i][0] & 0xff) << 8)
                                         | (entries[i][1] & 0xff));
                }else{
                    shortsn[i] = null;
                }
            }
            converted = new NullableShortColumn(shortsn);
            break;
        case NullableIntColumn.TYPE_CODE:
            final Integer[] intsn = new Integer[entries.length];
            for(int i=0; i<entries.length; ++i){
                if((entries[i] != null) && (entries[i].length >= 4)){
                    intsn[i] = (((entries[i][0] & 0xff) << 24)
                              | ((entries[i][1] & 0xff) << 16)
                              | ((entries[i][2] & 0xff) << 8)
                              |  (entries[i][3] & 0xff));
                }else{
                    intsn[i] = null;
                }
            }
            converted = new NullableIntColumn(intsn);
            break;
        case NullableLongColumn.TYPE_CODE:
            final Long[] longsn = new Long[entries.length];
            for(int i=0; i<entries.length; ++i){
                if((entries[i] != null) && (entries[i].length >= 8)){
                    longsn[i] = (((entries[i][0] & 0xffL) << 56)
                               | ((entries[i][1] & 0xffL) << 48)
                               | ((entries[i][2] & 0xffL) << 40)
                               | ((entries[i][3] & 0xffL) << 32)
                               | ((entries[i][4] & 0xffL) << 24)
                               | ((entries[i][5] & 0xffL) << 16)
                               | ((entries[i][6] & 0xffL) << 8)
                               |  (entries[i][7] & 0xffL));
                }else{
                    longsn[i] = null;
                }
            }
            converted = new NullableLongColumn(longsn);
            break;
        case NullableStringColumn.TYPE_CODE:
            final String[] stringsn = new String[entries.length];
            for(int i=0; i<entries.length; ++i){
                if((entries[i] != null) && (entries[i].length > 0)){
                    stringsn[i] = BitVector.wrap(entries[i]).toHexString();
                }else{
                    stringsn[i] = null;
                }
            }
            converted = new NullableStringColumn(stringsn);
            break;
        case NullableFloatColumn.TYPE_CODE:
            final Float[] floatsn = new Float[entries.length];
            for(int i=0; i<entries.length; ++i){
                if((entries[i] != null) && (entries[i].length >= 4)){
                    floatsn[i] = Float.intBitsToFloat(
                            ((entries[i][0] & 0xff) << 24)
                          | ((entries[i][1] & 0xff) << 16)
                          | ((entries[i][2] & 0xff) << 8)
                          |  (entries[i][3] & 0xff));
                }else{
                    floatsn[i] = null;
                }
            }
            converted = new NullableFloatColumn(floatsn);
            break;
        case NullableDoubleColumn.TYPE_CODE:
            final Double[] doublesn = new Double[entries.length];
            for(int i=0; i<entries.length; ++i){
                if((entries[i] != null) && (entries[i].length >= 8)){
                    doublesn[i] = Double.longBitsToDouble(
                            ((entries[i][0] & 0xffL) << 56)
                          | ((entries[i][1] & 0xffL) << 48)
                          | ((entries[i][2] & 0xffL) << 40)
                          | ((entries[i][3] & 0xffL) << 32)
                          | ((entries[i][4] & 0xffL) << 24)
                          | ((entries[i][5] & 0xffL) << 16)
                          | ((entries[i][6] & 0xffL) << 8)
                          |  (entries[i][7] & 0xffL));
                }else{
                    doublesn[i] = null;
                }
            }
            converted = new NullableDoubleColumn(doublesn);
            break;
        case NullableCharColumn.TYPE_CODE:
            final Character[] charsn = new Character[entries.length];
            for(int i=0; i<entries.length; ++i){
                if((entries[i] != null) && (entries[i].length > 0)){
                    charsn[i] = (char) entries[i][0];
                }else{
                    charsn[i] = null;
                }
            }
            converted = new NullableCharColumn(charsn);
            break;
        case NullableBooleanColumn.TYPE_CODE:
            final Boolean[] boolsn = new Boolean[entries.length];
            for(int i=0; i<entries.length; ++i){
                if((entries[i] != null) && (entries[i].length > 0)){
                    boolean isZero = true;
                    for(int j=0; j<entries[i].length; ++j){
                        if(entries[i][j] != 0){
                            isZero = false;
                            break;
                        }
                        boolsn[i] = !isZero;
                    }
                }else{
                    boolsn[i] = null;
                }
            }
            converted = new NullableBooleanColumn(boolsn);
            break;
        case NullableBinaryColumn.TYPE_CODE:
            converted = this.clone();
            break;
        default:
            throw new DataFrameException("Unknown column type code: " + typeCode);
        }
        converted.name = this.name;
        return converted;
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
