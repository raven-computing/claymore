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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * A Column holding char values.<br>
 * This implementation <b>DOES NOT</b> support null values.
 * 
 * @see NullableCharColumn
 *
 */
public final class CharColumn extends Column {

    /**
     * The unique type code of all <code>CharColumns</code>
     */
    public static final byte TYPE_CODE = (byte)8;

    /**
     * The default placeholder character used in CharColumns
     */
    public static final char DEFAULT_VALUE = '?';

    private char[] entries;

    /**
     * Constructs an empty <code>CharColumn</code>.
     */
    public CharColumn(){
        this(0);
    }

    /**
     * Constructs a <code>CharColumn</code> with the specified length.<br>
     * All column entries are set to default char values
     * 
     * @param length The initial length of the column to construct
     */
    public CharColumn(final int length){
        this.entries = new char[length];
        for(int i=0; i<length; ++i){
            this.entries[i] = DEFAULT_VALUE;
        }
    }

    /**
     * Constructs an empty <code>CharColumn</code> with the specified label.
     * 
     * @param name The name of the column to construct. Must not be null or empty
     */
    public CharColumn(final String name){
        this();
        if((name == null) || (name.isEmpty())){
            throw new IllegalArgumentException("Column name must not be null or empty");
        }
        this.name = name;
    }

    /**
     * Constructs a <code>CharColumn</code> with the specified label
     * and the specified length.<br>
     * All column entries are set to default char values
     * 
     * @param name The name of the column to construct. Must not be null or empty
     * @param length The initial length of the column to construct
     */
    public CharColumn(final String name, final int length){
        this(length);
        if((name == null) || (name.isEmpty())){
            throw new IllegalArgumentException("Column name must not be null or empty");
        }
        this.name = name;
    }

    /**
     * Constructs a new <code>CharColumn</code> composed of the content of 
     * the specified char array 
     * 
     * @param column The entries of the column to be constructed. Must not be null
     */
    public CharColumn(final char[] column){
        if(column == null){
            throw new IllegalArgumentException("Arg must not be null");
        }
        checkAsciiRange(column);
        this.entries = column;
    }

    /**
     * Constructs a new labeled <code>CharColumn</code> composed of the content of 
     * the specified char array
     * 
     * @param name The name of the column to construct. Must not be null or empty
     * @param column The entries of the column to be constructed. Must not be null
     */
    public CharColumn(final String name, final char[] column){
        this(name);
        if(column == null){
            throw new IllegalArgumentException("Arg must not be null");
        }
        checkAsciiRange(column);
        this.entries = column;
    }

    /**
     * Constructs a new <code>CharColumn</code> composed of the content of 
     * the specified list
     * 
     * @param list The list representing the entries of the column to be constructed
     */
    public CharColumn(final List<Character> list){
        fillFrom(list);
    }

    /**
     * Constructs a new labeled <code>CharColumn</code> composed of the content of 
     * the specified list
     * 
     * @param name The name of the column to construct. Must not be null or empty
     * @param list The list representing the entries of the column to be constructed.
     *             Must not be null or empty
     */
    public CharColumn(final String name, final List<Character> list){
        this(name);
        fillFrom(list);
    }

    /**
     * Gets the entry of this column at the specified index
     * 
     * @param index The index of the entry to get
     * @return The char value at the specified index
     */
    public char get(final int index){
        return entries[index];
    }

    /**
     * Sets the entry of this column at the specified index
     * to the given value
     * 
     * @param index The index of the entry to set
     * @param value The char value to set the entry to
     */
    public void set(final int index, final char value){
        if((value < 32) || (value > 126)){
            throw new IllegalArgumentException("Invalid character value. "
                                     + "Only printable ASCII is permitted");
        }
        entries[index] = value;
    }

    /**
     * Returns a reference to the internal array of this column
     * 
     * @return The internal char array
     */
    public char[] asArray(){
        return this.entries;
    }

    @Override
    public Column clone(){
        final char[] clone = new char[entries.length];
        for(int i=0; i<entries.length; ++i){
            clone[i] = entries[i];
        }
        return ((name != null) && !name.isEmpty())
                ? new CharColumn(name, clone)
                : new CharColumn(clone);
    }

    @Override
    public boolean equals(Object obj){
        if(this == obj){
            return true;
        }
        if(!(obj instanceof CharColumn)){
            return false;
        }
        final CharColumn col = (CharColumn)obj;
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
        this.set(index, (Character)value);
    }

    @Override
    public byte typeCode(){
        return TYPE_CODE;
    }

    @Override
    public String typeName(){
        return "char";
    }

    @Override
    public int capacity(){
        return entries.length;
    }

    @Override
    public boolean isNullable(){
        return false;
    }

    @Override
    public boolean isNumeric(){
        return false;
    }

    @Override
    public Object getDefaultValue(){
        return DEFAULT_VALUE;
    }

    @Override
    public int memoryUsage(){
        return entries.length;
    }

    @Override
    public Column convertTo(byte typeCode){
        Column converted = null;
        switch(typeCode){
        case ByteColumn.TYPE_CODE:
            final byte[] bytes = new byte[entries.length];
            for(int i=0; i<entries.length; ++i){
                bytes[i] = Byte.valueOf(String.valueOf(entries[i]));
            }
            converted = new ByteColumn(bytes);
            break;
        case ShortColumn.TYPE_CODE:
            final short[] shorts = new short[entries.length];
            for(int i=0; i<entries.length; ++i){
                shorts[i] = Short.valueOf(String.valueOf(entries[i]));
            }
            converted = new ShortColumn(shorts);
            break;
        case IntColumn.TYPE_CODE:
            final int[] ints = new int[entries.length];
            for(int i=0; i<entries.length; ++i){
                ints[i] = Integer.valueOf(String.valueOf(entries[i]));
            }
            converted = new IntColumn(ints);
            break;
        case LongColumn.TYPE_CODE:
            final long[] longs = new long[entries.length];
            for(int i=0; i<entries.length; ++i){
                longs[i] = Long.valueOf(String.valueOf(entries[i]));
            }
            converted = new LongColumn(longs);
            break;
        case StringColumn.TYPE_CODE:
            final String[] strings = new String[entries.length];
            for(int i=0; i<entries.length; ++i){
                strings[i] = String.valueOf(entries[i]);
            }
            converted = new StringColumn(strings);
            break;
        case FloatColumn.TYPE_CODE:
            final float[] floats = new float[entries.length];
            for(int i=0; i<entries.length; ++i){
                floats[i] = Float.valueOf(String.valueOf(entries[i]));
            }
            converted = new FloatColumn(floats);
            break;
        case DoubleColumn.TYPE_CODE:
            final double[] doubles = new double[entries.length];
            for(int i=0; i<entries.length; ++i){
                doubles[i] = Double.valueOf(String.valueOf(entries[i]));
            }
            converted = new DoubleColumn(doubles);
            break;
        case CharColumn.TYPE_CODE:
            converted = this.clone();
            break;
        case BooleanColumn.TYPE_CODE:
            final Set<String> valuesTrue = new HashSet<>(
                    Arrays.asList("t","1","y"));

            final Set<String> valuesFalse = new HashSet<>(
                    Arrays.asList("f","0","n"));

            final boolean[] bools = new boolean[entries.length];
            for(int i=0; i<entries.length; ++i){
                final String s = String.valueOf(entries[i]).toLowerCase();
                final boolean isTrue = valuesTrue.contains(s);
                final boolean isFalse = valuesFalse.contains(s);
                if(!isTrue && !isFalse){
                    throw new DataFrameException("Invalid boolean character: "
                                                 + entries[i]);
                }
                bools[i] = isTrue;
            }
            converted = new BooleanColumn(bools);
            break;
        case BinaryColumn.TYPE_CODE:
            final byte[][] bins = new byte[entries.length][];
            for(int i=0; i<entries.length; ++i){
                bins[i] = new byte[]{(byte)entries[i]};
            }
            converted = new BinaryColumn(bins);
            break;
        case NullableByteColumn.TYPE_CODE:
            final Byte[] bytesn = new Byte[entries.length];
            for(int i=0; i<entries.length; ++i){
                bytesn[i] = Byte.valueOf(String.valueOf(entries[i]));
            }
            converted = new NullableByteColumn(bytesn);
            break;
        case NullableShortColumn.TYPE_CODE:
            final Short[] shortsn = new Short[entries.length];
            for(int i=0; i<entries.length; ++i){
                shortsn[i] = Short.valueOf(String.valueOf(entries[i]));
            }
            converted = new NullableShortColumn(shortsn);
            break;
        case NullableIntColumn.TYPE_CODE:
            final Integer[] intsn = new Integer[entries.length];
            for(int i=0; i<entries.length; ++i){
                intsn[i] = Integer.valueOf(String.valueOf(entries[i]));
            }
            converted = new NullableIntColumn(intsn);
            break;
        case NullableLongColumn.TYPE_CODE:
            final Long[] longsn = new Long[entries.length];
            for(int i=0; i<entries.length; ++i){
                longsn[i] = Long.valueOf(String.valueOf(entries[i]));
            }
            converted = new NullableLongColumn(longsn);
            break;
        case NullableStringColumn.TYPE_CODE:
            final String[] stringsn = new String[entries.length];
            for(int i=0; i<entries.length; ++i){
                stringsn[i] = String.valueOf(entries[i]);
            }
            converted = new NullableStringColumn(stringsn);
            break;
        case NullableFloatColumn.TYPE_CODE:
            final Float[] floatsn = new Float[entries.length];
            for(int i=0; i<entries.length; ++i){
                floatsn[i] = Float.valueOf(String.valueOf(entries[i]));
            }
            converted = new NullableFloatColumn(floatsn);
            break;
        case NullableDoubleColumn.TYPE_CODE:
            final Double[] doublesn = new Double[entries.length];
            for(int i=0; i<entries.length; ++i){
                doublesn[i] = Double.valueOf(String.valueOf(entries[i]));
            }
            converted = new NullableDoubleColumn(doublesn);
            break;
        case NullableCharColumn.TYPE_CODE:
            final Character[] charsn = new Character[entries.length];
            for(int i=0; i<entries.length; ++i){
                charsn[i] = entries[i];
            }
            converted = new NullableCharColumn(charsn);
            break;
        case NullableBooleanColumn.TYPE_CODE:
            final Set<String> valuesnTrue = new HashSet<>(
                    Arrays.asList("t","1","y"));

            final Set<String> valuesnFalse = new HashSet<>(
                    Arrays.asList("f","0","n"));

            final Boolean[] boolsn = new Boolean[entries.length];
            for(int i=0; i<entries.length; ++i){
                final String s = String.valueOf(entries[i]).toLowerCase();
                final boolean isTrue = valuesnTrue.contains(s);
                final boolean isFalse = valuesnFalse.contains(s);
                if(!isTrue && !isFalse){
                    throw new DataFrameException("Invalid boolean character: "
                                                 + entries[i]);
                }
                boolsn[i] = isTrue;
            }
            converted = new NullableBooleanColumn(boolsn);
            break;
        case NullableBinaryColumn.TYPE_CODE:
            final byte[][] binsn = new byte[entries.length][];
            for(int i=0; i<entries.length; ++i){
                binsn[i] = new byte[]{(byte)entries[i]};
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
        final char c = (Character)value;
        if((c < 32) || (c > 126)){
            throw new IllegalArgumentException("Invalid character value. "
                                     + "Only printable ASCII is permitted");

        }
        for(int i=next; i>index; --i){
            entries[i] = entries[i-1];
        }
        entries[index] = c;
    }

    @Override
    protected Class<?> memberClass(){
        return Character.class;
    }

    @Override
    protected void resize(){
        char[] newEntries = new char[(entries.length > 0 ? entries.length*2 : 2)];
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
            entries[i] = '\u0000';
        }
    }

    @Override
    protected void matchLength(int length){
        if(length != entries.length){
            final char[] tmp = new char[length];
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
    
    private void checkAsciiRange(final char[] values){
        for(int i=0; i<values.length; ++i){
            if((values[i] < 32) || (values[i] > 126)){
                throw new IllegalArgumentException(
                        "Invalid character value for CharColumn at index "
                      + i + ". Only printable ASCII is permitted");

            }
        }
    }

    private void fillFrom(final List<Character> list){
        if((list == null) || (list.isEmpty())){
            throw new IllegalArgumentException("Arg must not be null or empty");
        }
        char[] tmp = new char[list.size()];
        Iterator<Character> iter = list.iterator();
        int i=0;
        while(iter.hasNext()){
            tmp[i++] = iter.next();
        }
        checkAsciiRange(tmp);
        this.entries = tmp;
    }
}
