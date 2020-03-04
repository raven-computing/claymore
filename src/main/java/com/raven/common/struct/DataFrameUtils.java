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

/**
 * Uninstantiable utility class providing static DataFrame operations.
 * 
 * @author Phil Gaiser
 * @since 3.0.0
 *
 */
public final class DataFrameUtils {

    private DataFrameUtils(){ }

    /**
     * Creates and returns a copy of the given {@link DataFrame}
     * 
     * @param df The DataFrame instance to copy
     * @return A copy of the specified DataFrame
     */
    public static DataFrame copyOf(final DataFrame df){
        DataFrame copy = null;
        if(df instanceof DefaultDataFrame){
            copy = new DefaultDataFrame();
        }else{
            copy = new NullableDataFrame();
        }
        df.flush();
        for(final Column col : df){
            copy.addColumn(col.clone());
        }
        if(df.hasColumnNames()){
            copy.setColumnNames(df.getColumnNames());
        }
        return copy;
    }

    /**
     * Merges all given {@link DataFrame} instances into one DataFrame of the same type. 
     * All DataFrames must be of equal size. All columns are added to the returned DataFrame
     * in the order of the arguments passed to this method. Only passing one DataFrame to this
     * method will simply return that instance.
     * <p>All columns of the returned DataFrame are backed by their origin, which means that 
     * changes to the original DataFrame are reflected in the merged DataFrame, and vice-versa.
     * If that is not what you want, simply pass a clone (copy) of each DataFrame argument to 
     * this method.
     * <p>Example:<br> 
     * <code>
     * DataFrame merged = DataFrame.merge(DataFrame.copyOf(df1), DataFrame.copyOf(df2));
     * </code>
     * 
     * @param dataFrames The DataFrames to be merged
     * @return A DataFrame composed of all columns of the given DataFrames
     */
    public static DataFrame merge(final DataFrame... dataFrames){
        if((dataFrames == null) || (dataFrames.length == 0)){
            throw new IllegalArgumentException("Arg must not be null or empty"); 
        }
        if(dataFrames.length == 1){
            return dataFrames[0];
        }
        final Class<?> type = dataFrames[0].getClass();
        for(int i=1; i<dataFrames.length; ++i){
            if(!(dataFrames[i].getClass().equals(type))){
                throw new DataFrameException(String.format(
                        "Type missmatch at index %s. Expected %s but found %s",
                        i, type.getSimpleName(), dataFrames[i].getClass().getSimpleName()));
            }
        }
        final int size = dataFrames[0].rows();
        for(int i=1; i<dataFrames.length; ++i){
            if(dataFrames[i].rows() != size){
                throw new DataFrameException(String.format(
                        "Size missmatch at index %s. Expected %s rows but found %s",
                        i, size, dataFrames[i].rows()));
            }
        }
        DataFrame merged = null;
        if(dataFrames[0] instanceof DefaultDataFrame){
            merged = new DefaultDataFrame();
        }else{
            merged = new NullableDataFrame();
        }
        boolean isNamed = false;
        int cols = 0;
        for(final DataFrame df : dataFrames){
            df.flush();
            for(final Column col : df){
                merged.addColumn(col);
                ++cols;
            }
            if(df.hasColumnNames()){
                isNamed = true;
            }
        }
        if(isNamed){
            final String[] mergedNames = new String[cols];
            int idx = 0;
            for(final DataFrame df : dataFrames){
                for(int i=0; i<df.columns(); ++i){
                    final String s = df.getColumnName(i);
                    mergedNames[idx] = (s != null ? s : String.valueOf(idx));
                    ++idx;
                }
            }
            merged.setColumnNames(mergedNames);
        }
        return merged;
    }

    /**
     * Converts the given {@link DataFrame} from a {@link DefaultDataFrame} to a 
     * {@link NullableDataFrame} or vice-versa.<br>
     * Converting a DefaultDataFrame to a NullableDataFrame will not change any internal
     * values, except that now you can add/insert null values to it.<br>
     * Converting a NullableDataFrame to a DefaultDataFrame will convert all null
     * occurrences to the primitive defaults according to the column they are located.
     * Null values in NullableBinaryColumns will be converted
     * to a single byte with value zero.<br>
     * <p>Example: (if 'myDf' is a DefaultDataFrame)<br>
     * <code>DataFrame df = DataFrame.convert(myDf, NullableDataFrame.class);</code>
     * 
     * @param df The DataFrame instance to convert
     * @param type The type to convert the given DataFrame to
     * @return A DataFrame converted from the type of the argument passed to this 
     *         method to the type specified
     */
    public static DataFrame convert(final DataFrame df, final Class<?> type){
        if((df == null) || (type == null)){
            throw new IllegalArgumentException("Arg must not be null"); 
        }
        if((!type.getSimpleName().equals(DefaultDataFrame.class.getSimpleName())) 
                && (!type.getSimpleName().equals(NullableDataFrame.class.getSimpleName()))){

            throw new IllegalArgumentException(String.format("Unable to convert to %s."
                    + " Must be a DataFrame type",
                    type.getName()));
        }
        if(df.getClass().getSimpleName().equals(type.getSimpleName())){
            return copyOf(df);
        }
        DataFrame converted = null;
        //convert from Nullable to Default
        if(type.getSimpleName().equals(DefaultDataFrame.class.getSimpleName())){
            converted = new DefaultDataFrame();
            for(final Column col : df){
                switch(col.typeCode()){
                case NullableByteColumn.TYPE_CODE:
                    byte[] copyByte = new byte[df.rows()];
                    for(int i=0; i<df.rows(); ++i){
                        final Byte val = (Byte)col.getValueAt(i);
                        copyByte[i] = (val != null ? val : 0);
                    }
                    converted.addColumn(new ByteColumn(copyByte));
                    break;
                case NullableShortColumn.TYPE_CODE:
                    short[] copyShort = new short[df.rows()];
                    for(int i=0; i<df.rows(); ++i){
                        final Short val = (Short)col.getValueAt(i);
                        copyShort[i] = (val != null ? val : 0);
                    }
                    converted.addColumn(new ShortColumn(copyShort));
                    break;
                case NullableIntColumn.TYPE_CODE:
                    int[] copyInt = new int[df.rows()];
                    for(int i=0; i<df.rows(); ++i){
                        final Integer val = (Integer)col.getValueAt(i);
                        copyInt[i] = (val != null ? val : 0);
                    }
                    converted.addColumn(new IntColumn(copyInt));
                    break;
                case NullableLongColumn.TYPE_CODE:
                    long[] copyLong = new long[df.rows()];
                    for(int i=0; i<df.rows(); ++i){
                        final Long val = (Long)col.getValueAt(i);
                        copyLong[i] = (val != null ? val : 0l);
                    }
                    converted.addColumn(new LongColumn(copyLong));
                    break;
                case NullableStringColumn.TYPE_CODE:
                    String[] copyString = new String[df.rows()];
                    for(int i=0; i<df.rows(); ++i){
                        final String val = (String)col.getValueAt(i);
                        copyString[i] = (((val != null) && (!val.isEmpty())) ? val : "n/a");
                    }
                    converted.addColumn(new StringColumn(copyString));
                    break;
                case NullableFloatColumn.TYPE_CODE:
                    float[] copyFloat = new float[df.rows()];
                    for(int i=0; i<df.rows(); ++i){
                        final Float val = (Float)col.getValueAt(i);
                        copyFloat[i] = (val != null ? val : 0f);
                    }
                    converted.addColumn(new FloatColumn(copyFloat));
                    break;
                case NullableDoubleColumn.TYPE_CODE:
                    double[] copyDouble = new double[df.rows()];
                    for(int i=0; i<df.rows(); ++i){
                        final Double val = (Double)col.getValueAt(i);
                        copyDouble[i] = (val != null ? val : 0d);
                    }
                    converted.addColumn(new DoubleColumn(copyDouble));
                    break;
                case NullableCharColumn.TYPE_CODE:
                    char[] copyChar = new char[df.rows()];
                    for(int i=0; i<df.rows(); ++i){
                        final Character val = (Character)col.getValueAt(i);
                        copyChar[i] = (val != null ? val : '\u0000');
                    }
                    converted.addColumn(new CharColumn(copyChar));
                    break;
                case NullableBooleanColumn.TYPE_CODE:
                    boolean[] copyBoolean = new boolean[df.rows()];
                    for(int i=0; i<df.rows(); ++i){
                        final Boolean val = (Boolean)col.getValueAt(i);
                        copyBoolean[i] = (val != null ? val : false);
                    }
                    converted.addColumn(new BooleanColumn(copyBoolean));
                    break;
                case NullableBinaryColumn.TYPE_CODE:
                    byte[][] copyBinary = new byte[df.rows()][0];
                    final byte[] defaultVal = new byte[]{0};
                    for(int i=0; i<df.rows(); ++i){
                        final byte[] val = (byte[])col.getValueAt(i);
                        copyBinary[i] = (val != null ? val : defaultVal);
                    }
                    converted.addColumn(new BinaryColumn(copyBinary));
                    break;
                default://undefined type
                    throw new DataFrameException(String.format("Unable to convert dataframe."
                            + " Unrecognized column type %s",
                            col.memberClass().getSimpleName()));
                }
            }
        }else{//convert from Default to Nullable
            converted = new NullableDataFrame();
            for(final Column col : df){
                switch(col.typeCode()){
                case ByteColumn.TYPE_CODE:
                    Byte[] copyByte = new Byte[df.rows()];
                    for(int i=0; i<df.rows(); ++i){
                        copyByte[i] = (Byte)col.getValueAt(i);
                    }
                    converted.addColumn(new NullableByteColumn(copyByte));
                    break;
                case ShortColumn.TYPE_CODE:
                    Short[] copyShort = new Short[df.rows()];
                    for(int i=0; i<df.rows(); ++i){
                        copyShort[i] = (Short)col.getValueAt(i);
                    }
                    converted.addColumn(new NullableShortColumn(copyShort));
                    break;
                case IntColumn.TYPE_CODE:
                    Integer[] copyInt = new Integer[df.rows()];
                    for(int i=0; i<df.rows(); ++i){
                        copyInt[i] = (Integer)col.getValueAt(i);
                    }
                    converted.addColumn(new NullableIntColumn(copyInt));
                    break;
                case LongColumn.TYPE_CODE:
                    Long[] copyLong = new Long[df.rows()];
                    for(int i=0; i<df.rows(); ++i){
                        copyLong[i] = (Long)col.getValueAt(i);
                    }
                    converted.addColumn(new NullableLongColumn(copyLong));
                    break;
                case StringColumn.TYPE_CODE:
                    String[] copyString = new String[df.rows()];
                    for(int i=0; i<df.rows(); ++i){
                        copyString[i] = (String)col.getValueAt(i);
                    }
                    converted.addColumn(new NullableStringColumn(copyString));
                    break;
                case FloatColumn.TYPE_CODE:
                    Float[] copyFloat = new Float[df.rows()];
                    for(int i=0; i<df.rows(); ++i){
                        copyFloat[i] = (Float)col.getValueAt(i);
                    }
                    converted.addColumn(new NullableFloatColumn(copyFloat));
                    break;
                case DoubleColumn.TYPE_CODE:
                    Double[] copyDouble = new Double[df.rows()];
                    for(int i=0; i<df.rows(); ++i){
                        copyDouble[i] = (Double)col.getValueAt(i);
                    }
                    converted.addColumn(new NullableDoubleColumn(copyDouble));
                    break;
                case CharColumn.TYPE_CODE:
                    Character[] copyChar = new Character[df.rows()];
                    for(int i=0; i<df.rows(); ++i){
                        copyChar[i] = (Character)col.getValueAt(i);
                    }
                    converted.addColumn(new NullableCharColumn(copyChar));
                    break;
                case BooleanColumn.TYPE_CODE:
                    Boolean[] copyBoolean = new Boolean[df.rows()];
                    for(int i=0; i<df.rows(); ++i){
                        copyBoolean[i] = (Boolean)col.getValueAt(i);
                    }
                    converted.addColumn(new NullableBooleanColumn(copyBoolean));
                    break;
                case BinaryColumn.TYPE_CODE:
                    final BinaryColumn data = (BinaryColumn)col;
                    converted.addColumn(new NullableBinaryColumn(data.asArray()));
                    break;
                default://undefined type
                    throw new DataFrameException(String.format("Unable to convert dataframe."
                            + " Unrecognized column type %s",
                            col.memberClass().getSimpleName()));
                }
            }
        }
        if(df.hasColumnNames()){
            converted.setColumnNames(df.getColumnNames());
        }
        return converted;
    }
}
