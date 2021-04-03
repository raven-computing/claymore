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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * A Column holding nullable string values.<br>
 * Any values not explicitly set are considered null.
 * 
 * @see StringColumn
 *
 */
public final class NullableStringColumn extends NullableColumn {

    /**
     * The unique type code of all <code>NullableStringColumns</code>
     */
    public static final byte TYPE_CODE = (byte)14;

    private String[] entries;

    /**
     * 	Constructs an empty <code>NullableStringColumn</code>.
     */
    public NullableStringColumn(){
        this(0);
    }

    /**
     * Constructs a <code>NullableStringColumn</code> with the specified length.<br>
     * All column entries are set to null
     * 
     * @param length The initial length of the column to construct
     */
    public NullableStringColumn(final int length){
        this.entries = new String[length];
    }

    /**
     * Constructs an empty <code>NullableStringColumn</code> with the specified label.
     * 
     * @param name The name of the column to construct. Must not be null or empty
     */
    public NullableStringColumn(final String name){
        this();
        if((name == null) || (name.isEmpty())){
            throw new IllegalArgumentException("Column name must not be null or empty");
        }
        this.name = name;
    }

    /**
     * Constructs a <code>NullableStringColumn</code> with the specified label
     * and the specified length.<br>
     * All column entries are set to null
     * 
     * @param name The name of the column to construct. Must not be null or empty
     * @param length The initial length of the column to construct
     */
    public NullableStringColumn(final String name, final int length){
        this(length);
        if((name == null) || (name.isEmpty())){
            throw new IllegalArgumentException("Column name must not be null or empty");
        }
        this.name = name;
    }

    /**
     * Constructs a new <code>NullableStringColumn</code> composed of the content of 
     * the specified string array. Individual entries may be null or empty
     * 
     * @param column The entries of the column to be constructed. Must not be null
     */
    public NullableStringColumn(final String[] column){
        if(column == null){
            throw new IllegalArgumentException("Arg must not be null");
        }
        this.entries = column;
    }

    /**
     * Constructs a new labeled <code>NullableStringColumn</code> composed of the
     * content of the specified string array
     * 
     * @param name The name of the column to construct. Must not be null or empty
     * @param column The entries of the column to be constructed. Must not be null
     */
    public NullableStringColumn(final String name, final String[] column){
        this(name);
        if(column == null){
            throw new IllegalArgumentException("Arg must not be null");
        }
        this.entries = column;
    }

    /**
     * Constructs a new <code>NullableStringColumn</code> composed of the content of 
     * the specified List. Individual items may be null or empty
     * 
     * @param list The entries of the column to be constructed. Must not be null or empty
     */
    public NullableStringColumn(final List<String> list){
        fillFrom(list);
    }

    /**
     * Constructs a new labeled <code>NullableStringColumn</code> composed of the content of 
     * the specified list
     * 
     * @param name The name of the column to construct. Must not be null or empty
     * @param list The list representing the entries of the column to be constructed.
     *             Must not be null or empty
     */
    public NullableStringColumn(final String name, final List<String> list){
        this(name);
        fillFrom(list);
    }

    /**
     * Gets the entry of this column at the specified index
     * 
     * @param index The index of the entry to get
     * @return The String value at the specified index. May be null
     */
    public String get(final int index){
        return entries[index];
    }

    /**
     * Sets the entry of this column at the specified index
     * to the given value
     * 
     * @param index The index of the entry to set
     * @param value The String value to set the entry to. May be null
     */
    public void set(final int index, final String value){
        entries[index] = value;
    }

    /**
     * Returns a reference to the internal array of this column
     * 
     * @return The internal String array
     */
    public String[] asArray(){
        return this.entries;
    }

    @Override
    public Column clone(){
        final String[] clone = new String[entries.length];
        for(int i=0; i<entries.length; ++i){
            clone[i] = entries[i];
        }
        return ((name != null) && !name.isEmpty())
                ? new NullableStringColumn(name, clone)
                : new NullableStringColumn(clone);
    }

    @Override
    public boolean equals(Object obj){
        if(this == obj){
            return true;
        }
        if(!(obj instanceof NullableStringColumn)){
            return false;
        }
        final NullableStringColumn col = (NullableStringColumn)obj;
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
        entries[index] = (String)value;
    }

    @Override
    public byte typeCode(){
        return TYPE_CODE;
    }

    @Override
    public String typeName(){
        return "string";
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
            if((entries[i] == null) || entries[i].isEmpty()){
                size += 1;
            }else{
                size += entries[i].getBytes(StandardCharsets.UTF_8).length + 1;
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
                if(entries[i] != null){
                    bytes[i] = Byte.valueOf(entries[i]);
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
                    shorts[i] = Short.valueOf(entries[i]);
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
                    ints[i] = Integer.valueOf(entries[i]);
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
                    longs[i] = Long.valueOf(entries[i]);
                }else{
                    longs[i] = 0l;
                }
            }
            converted = new LongColumn(longs);
            break;
        case StringColumn.TYPE_CODE:
            final String[] strings = new String[entries.length];
            for(int i=0; i<entries.length; ++i){
                if((entries[i] != null) && !entries[i].isEmpty()){
                    strings[i] = entries[i];
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
                    floats[i] = Float.valueOf(entries[i]);
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
                    doubles[i] = Double.valueOf(entries[i]);
                }else{
                    doubles[i] = 0.0;
                }
            }
            converted = new DoubleColumn(doubles);
            break;
        case CharColumn.TYPE_CODE:
            final char[] chars = new char[entries.length];
            for(int i=0; i<entries.length; ++i){
                if((entries[i] != null) && !entries[i].isEmpty()){
                    chars[i] = entries[i].charAt(0);
                }else{
                    chars[i] = CharColumn.DEFAULT_VALUE;
                }
            }
            converted = new CharColumn(chars);
            break;
        case BooleanColumn.TYPE_CODE:
            final Set<String> valuesTrue = new HashSet<>(
                    Arrays.asList("true","t","1","yes","y","on"));

            final Set<String> valuesFalse = new HashSet<>(
                    Arrays.asList("false","f","0","no","n","off"));

            final boolean[] bools = new boolean[entries.length];
            for(int i=0; i<entries.length; ++i){
                if((entries[i] != null) && !entries[i].isEmpty()){
                    final String s = entries[i].toLowerCase();
                    final boolean isTrue = valuesTrue.contains(s);
                    final boolean isFalse = valuesFalse.contains(s);
                    if(!isTrue && !isFalse){
                        throw new DataFrameException("Invalid boolean string: "
                                + entries[i]);
                    }
                    bools[i] = isTrue;
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
                    bins[i] = BinaryColumn.bytesFromHex(entries[i]);
                }else{
                    bins[i] = new byte[]{0};
                }
            }
            converted = new BinaryColumn(bins);
            break;
        case NullableByteColumn.TYPE_CODE:
            final Byte[] bytesn = new Byte[entries.length];
            for(int i=0; i<entries.length; ++i){
                if(entries[i] != null){
                    bytesn[i] = Byte.valueOf(entries[i]);
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
                    shortsn[i] = Short.valueOf(entries[i]);
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
                    intsn[i] = Integer.valueOf(entries[i]);
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
                    longsn[i] = Long.valueOf(entries[i]);
                }else{
                    longsn[i] = null;
                }
            }
            converted = new NullableLongColumn(longsn);
            break;
        case NullableStringColumn.TYPE_CODE:
            converted = this.clone();
            break;
        case NullableFloatColumn.TYPE_CODE:
            final Float[] floatsn = new Float[entries.length];
            for(int i=0; i<entries.length; ++i){
                if(entries[i] != null){
                    floatsn[i] = Float.valueOf(entries[i]);
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
                    doublesn[i] = Double.valueOf(entries[i]);
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
                    charsn[i] = entries[i].charAt(0);
                }else{
                    charsn[i] = null;
                }
            }
            converted = new NullableCharColumn(charsn);
            break;
        case NullableBooleanColumn.TYPE_CODE:
            final Set<String> valuesnTrue = new HashSet<>(
                    Arrays.asList("true","t","1","yes","y","on"));

            final Set<String> valuesnFalse = new HashSet<>(
                    Arrays.asList("false","f","0","no","n","off"));

            final Boolean[] boolsn = new Boolean[entries.length];
            for(int i=0; i<entries.length; ++i){
                if(entries[i] != null){
                    final String s = entries[i].toLowerCase();
                    final boolean isTrue = valuesnTrue.contains(s);
                    final boolean isFalse = valuesnFalse.contains(s);
                    if(!isTrue && !isFalse){
                        throw new DataFrameException("Invalid boolean string: "
                                + entries[i]);
                    }
                    boolsn[i] = isTrue;
                }else{
                    boolsn[i] = null;
                }
            }
            converted = new NullableBooleanColumn(boolsn);
            break;
        case NullableBinaryColumn.TYPE_CODE:
            final byte[][] binsn = new byte[entries.length][];
            for(int i=0; i<entries.length; ++i){
                if(entries[i] != null){
                    binsn[i] = BinaryColumn.bytesFromHex(entries[i]);
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
        entries[index] = (String)value;
    }

    @Override
    protected Class<?> memberClass(){
        return String.class;
    }

    @Override
    protected void resize(){
        String[] newEntries = new String[(entries.length > 0 ? entries.length*2 : 2)];
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
            tmp[i++] = iter.next();
        }
        this.entries = tmp;
    }
}
