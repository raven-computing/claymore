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

import java.util.Collection;
import java.util.HashSet;

/**
 * Uninstantiable utility class providing static DataFrame operations.<br>
 * This class is not a public API. Use the <code>DataFrame</code> interface
 * instead.
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
     * @return A copy of the specified DataFrame or null if the argument is null
     */
    public static DataFrame copyOf(final DataFrame df){
        if(df == null){
            return null;
        }
        DataFrame copy = null;
        if(df instanceof DefaultDataFrame){
            copy = new DefaultDataFrame();
        }else{
            copy = new NullableDataFrame();
        }
        df.flush();
        final int cols = df.columns();
        for(int i=0; i<cols; ++i){
            copy.addColumn(df.getColumn(i).clone());
        }
        if(df.hasColumnNames()){
            copy.setColumnNames(df.getColumnNames());
        }
        return copy;
    }

    /**
     * Creates and returns a DataFrame which has the same column structure
     * and column names as the specified DataFrame instance but is otherwise empty
     * 
     * @param df The <code>DataFrame</code> from which to copy the
     *           column structure
     * @return A <code>DataFrame</code> with the same column structure and names
     *         as the specified DataFrame, or null if the specified DataFrame is null
     */
    public static DataFrame like(final DataFrame df){
        if(df == null){
            return null;
        }
        final int c = df.columns();
        if(c == 0){
            return df.isNullable()
                    ? new NullableDataFrame()
                    : new DefaultDataFrame();
        }
        final Column[] cols = new Column[c];
        for(int i=0; i<c; ++i){
            cols[i] = Column.ofType(df.getColumn(i).typeCode());
        }
        final DataFrame like = df.isNullable()
                ? new NullableDataFrame(cols)
                : new DefaultDataFrame(cols);

        if(df.hasColumnNames()){
            like.setColumnNames(df.getColumnNames());
        }
        return like;
    }

    /**
     * Merges all given {@link DataFrame} instances into one DataFrame.
     * All DataFames are merged by columns. All DataFrames must have an
     * equal number of rows but may be of any type. All columns are added to
     * the returned DataFrame in the order of the arguments passed to this
     * method. Only passing one DataFrame to this method will simply
     * return that instance.<br>
     * Columns with duplicate names are included in the returned DataFrame
     * and a postfix is added to each duplicate column name. 
     * 
     * <p>All columns of the returned DataFrame are backed by their origin,
     * which means that changes to the original DataFrame are reflected in
     * the merged DataFrame and vice versa. This does not apply, however,
     * if columns need to be converted to a nullable type. For example, if
     * one DataFrame argument is nullable, then all columns from non-nullable
     * DataFrame arguments are converted to their corresponding
     * nullable equivalent.<br>
     * 
     * If columns should be independent from their origin, then simply pass
     * a clone (copy) of each DataFrame argument to this method.
     * <p>Example:<br> 
     * <code>
     * DataFrame merged = DataFrame.merge(DataFrame.copy(df1), DataFrame.copy(df2));
     * </code>
     * 
     * @param dataFrames The DataFrames to be merged
     * @return A DataFrame composed of all columns of the given DataFrames
     */
    public static DataFrame merge(final DataFrame... dataFrames){
        if((dataFrames == null) || (dataFrames.length == 0)){
            throw new DataFrameException("Arg must not be null or empty"); 
        }
        if(dataFrames.length == 1){
            return dataFrames[0];
        }
        final int rows = dataFrames[0].rows();
        int cols = 0;
        boolean hasNullable = false;
        boolean hasNames = false;
        for(int i=0; i<dataFrames.length; ++i){
            if(dataFrames[i] == null){
                throw new DataFrameException(
                        "Invalid argument. DataFrame at index "
                        + i + " must not be null");
            }
            cols += dataFrames[i].columns();
            if(dataFrames[i] == null){
                throw new DataFrameException(
                        "DataFrame argument must not be null"); 
            }
            if(dataFrames[i].rows() != rows){
                throw new DataFrameException(String.format(
                        "Size missmatch for DataFrame argument at index %s. "
                        + "Expected %s rows but found %s",
                        i, rows, dataFrames[i].rows()));
            }
            if(dataFrames[i].isNullable()){
                hasNullable = true;
            }
            if(dataFrames[i].hasColumnNames()){
                hasNames = true;
            }
        }
        for(int i=0; i<dataFrames.length; ++i){
            dataFrames[i].flush();
        }
        
        String[] names = null;
        if(hasNames){
            names = new String[cols];
            for(int i=0; i<cols; ++i){
                names[i] = String.valueOf(i);
            }
            int k = 0;
            for(int i=0; i<dataFrames.length; ++i){
                for(int j=0; j<dataFrames[i].columns(); ++j){
                    final Column c = dataFrames[i].getColumn(j);
                    if((c.name != null) && !c.name.isEmpty()){
                        names[k] = c.name;
                    }
                    ++k;
                }
            }
            for(int i=0; i<cols; ++i){
                k = 0;
                boolean alreadySet = false;
                final String n = names[i];
                for(int j=0; j<cols; ++j){
                    if(i != j){
                        if(n.equals(names[j])){
                            if(!alreadySet){
                                names[i] = names[i] + "_" + k++;
                                alreadySet = true;
                            }
                            names[j] = names[j] + "_" + (k++);
                        }
                    }
                }
            }
        }
        final Column[] columns = new Column[cols];
        int k = 0;
        for(int i=0; i<dataFrames.length; ++i){
            for(int j=0; j<dataFrames[i].columns(); ++j){
                if(hasNullable){
                    columns[k++] = dataFrames[i].getColumn(j).asNullable();
                }else{
                    columns[k++] = dataFrames[i].getColumn(j);
                }
            }
        }
        DataFrame merged = null;
        if(hasNullable){
            if(hasNames){
                merged = new NullableDataFrame(names, columns);
            }else{
                merged = new NullableDataFrame(columns);
            }
        }else{
            if(hasNames){
                merged = new DefaultDataFrame(names, columns);
            }else{
                merged = new DefaultDataFrame(columns);
            }
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
            throw new DataFrameException("Arg must not be null"); 
        }
        if((!type.getSimpleName().equals(DefaultDataFrame.class.getSimpleName())) 
                && (!type.getSimpleName().equals(
                        NullableDataFrame.class.getSimpleName()))){


            throw new DataFrameException(String.format("Unable to convert to %s."
                    + " Must be a DataFrame type",
                    type.getName()));
        }
        if(df.getClass().getSimpleName().equals(type.getSimpleName())){
            return copyOf(df);
        }
        final int cols = df.columns();
        final int rows = df.rows();
        DataFrame converted = null;
        //convert from Nullable to Default
        if(type.getSimpleName().equals(DefaultDataFrame.class.getSimpleName())){
            converted = new DefaultDataFrame();
            for(int j=0; j<cols; ++j){
                final Column col = df.getColumn(j);
                switch(col.typeCode()){
                case NullableByteColumn.TYPE_CODE:
                    byte[] copyByte = new byte[df.rows()];
                    for(int i=0; i<rows; ++i){
                        final Byte val = (Byte)col.getValue(i);
                        copyByte[i] = (val != null ? val : 0);
                    }
                    converted.addColumn(new ByteColumn(copyByte));
                    break;
                case NullableShortColumn.TYPE_CODE:
                    short[] copyShort = new short[df.rows()];
                    for(int i=0; i<rows; ++i){
                        final Short val = (Short)col.getValue(i);
                        copyShort[i] = (val != null ? val : 0);
                    }
                    converted.addColumn(new ShortColumn(copyShort));
                    break;
                case NullableIntColumn.TYPE_CODE:
                    int[] copyInt = new int[df.rows()];
                    for(int i=0; i<rows; ++i){
                        final Integer val = (Integer)col.getValue(i);
                        copyInt[i] = (val != null ? val : 0);
                    }
                    converted.addColumn(new IntColumn(copyInt));
                    break;
                case NullableLongColumn.TYPE_CODE:
                    long[] copyLong = new long[df.rows()];
                    for(int i=0; i<rows; ++i){
                        final Long val = (Long)col.getValue(i);
                        copyLong[i] = (val != null ? val : 0l);
                    }
                    converted.addColumn(new LongColumn(copyLong));
                    break;
                case NullableStringColumn.TYPE_CODE:
                    String[] copyString = new String[df.rows()];
                    for(int i=0; i<rows; ++i){
                        final String val = (String)col.getValue(i);
                        copyString[i] = (((val != null) && !val.isEmpty())
                                                ? val
                                                : StringColumn.DEFAULT_VALUE);
                    }
                    converted.addColumn(new StringColumn(copyString));
                    break;
                case NullableFloatColumn.TYPE_CODE:
                    float[] copyFloat = new float[df.rows()];
                    for(int i=0; i<rows; ++i){
                        final Float val = (Float)col.getValue(i);
                        copyFloat[i] = (val != null ? val : 0f);
                    }
                    converted.addColumn(new FloatColumn(copyFloat));
                    break;
                case NullableDoubleColumn.TYPE_CODE:
                    double[] copyDouble = new double[df.rows()];
                    for(int i=0; i<rows; ++i){
                        final Double val = (Double)col.getValue(i);
                        copyDouble[i] = (val != null ? val : 0d);
                    }
                    converted.addColumn(new DoubleColumn(copyDouble));
                    break;
                case NullableCharColumn.TYPE_CODE:
                    char[] copyChar = new char[df.rows()];
                    for(int i=0; i<rows; ++i){
                        final Character val = (Character)col.getValue(i);
                        copyChar[i] = (val != null ? val : CharColumn.DEFAULT_VALUE);
                    }
                    converted.addColumn(new CharColumn(copyChar));
                    break;
                case NullableBooleanColumn.TYPE_CODE:
                    boolean[] copyBoolean = new boolean[df.rows()];
                    for(int i=0; i<rows; ++i){
                        final Boolean val = (Boolean)col.getValue(i);
                        copyBoolean[i] = (val != null ? val : false);
                    }
                    converted.addColumn(new BooleanColumn(copyBoolean));
                    break;
                case NullableBinaryColumn.TYPE_CODE:
                    NullableBinaryColumn data = (NullableBinaryColumn)col;
                    byte[][] copyBinary = new byte[df.rows()][];
                    for(int i=0; i<rows; ++i){
                        final byte[] val = data.get(i);
                        copyBinary[i] = (val != null ? val : new byte[]{0});
                    }
                    converted.addColumn(new BinaryColumn(copyBinary));
                    break;
                default://undefined type
                    throw new DataFrameException(String.format(
                            "Unable to convert DataFrame."
                            + " Unrecognized column type %s",
                            col.memberClass().getSimpleName()));
                }
            }
        }else{//convert from Default to Nullable
            converted = new NullableDataFrame();
            for(int j=0; j<cols; ++j){
                final Column col = df.getColumn(j);
                switch(col.typeCode()){
                case ByteColumn.TYPE_CODE:
                    Byte[] copyByte = new Byte[df.rows()];
                    for(int i=0; i<rows; ++i){
                        copyByte[i] = (Byte)col.getValue(i);
                    }
                    converted.addColumn(new NullableByteColumn(copyByte));
                    break;
                case ShortColumn.TYPE_CODE:
                    Short[] copyShort = new Short[df.rows()];
                    for(int i=0; i<rows; ++i){
                        copyShort[i] = (Short)col.getValue(i);
                    }
                    converted.addColumn(new NullableShortColumn(copyShort));
                    break;
                case IntColumn.TYPE_CODE:
                    Integer[] copyInt = new Integer[df.rows()];
                    for(int i=0; i<rows; ++i){
                        copyInt[i] = (Integer)col.getValue(i);
                    }
                    converted.addColumn(new NullableIntColumn(copyInt));
                    break;
                case LongColumn.TYPE_CODE:
                    Long[] copyLong = new Long[df.rows()];
                    for(int i=0; i<rows; ++i){
                        copyLong[i] = (Long)col.getValue(i);
                    }
                    converted.addColumn(new NullableLongColumn(copyLong));
                    break;
                case StringColumn.TYPE_CODE:
                    String[] copyString = new String[df.rows()];
                    for(int i=0; i<rows; ++i){
                        copyString[i] = (String)col.getValue(i);
                    }
                    converted.addColumn(new NullableStringColumn(copyString));
                    break;
                case FloatColumn.TYPE_CODE:
                    Float[] copyFloat = new Float[df.rows()];
                    for(int i=0; i<rows; ++i){
                        copyFloat[i] = (Float)col.getValue(i);
                    }
                    converted.addColumn(new NullableFloatColumn(copyFloat));
                    break;
                case DoubleColumn.TYPE_CODE:
                    Double[] copyDouble = new Double[df.rows()];
                    for(int i=0; i<rows; ++i){
                        copyDouble[i] = (Double)col.getValue(i);
                    }
                    converted.addColumn(new NullableDoubleColumn(copyDouble));
                    break;
                case CharColumn.TYPE_CODE:
                    Character[] copyChar = new Character[df.rows()];
                    for(int i=0; i<rows; ++i){
                        copyChar[i] = (Character)col.getValue(i);
                    }
                    converted.addColumn(new NullableCharColumn(copyChar));
                    break;
                case BooleanColumn.TYPE_CODE:
                    Boolean[] copyBoolean = new Boolean[df.rows()];
                    for(int i=0; i<rows; ++i){
                        copyBoolean[i] = (Boolean)col.getValue(i);
                    }
                    converted.addColumn(new NullableBooleanColumn(copyBoolean));
                    break;
                case BinaryColumn.TYPE_CODE:
                    BinaryColumn data = (BinaryColumn)col;
                    byte[][] copyBinary = new byte[df.rows()][];
                    for(int i=0; i<rows; ++i){
                        copyBinary[i] = data.get(i);
                    }
                    converted.addColumn(new NullableBinaryColumn(copyBinary));
                    break;
                default://undefined type
                    throw new DataFrameException(String.format(
                            "Unable to convert DataFrame."
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

    /**
     * Combines all rows from the specified DataFrames which have matching
     * values in their columns with the corresponding specified name. Both
     * DataFrames must have a column with the corresponding specified name
     * and an identical element type. All columns in both DataFrame instances must
     * be labeled by the time this method is called. The specified DataFrames may be
     * of any type.
     * 
     * <p>All columns in the second DataFrame argument that are also existent in the
     * first, are excluded in the DataFrame returned by this method
     * 
     * @param df1 The first <code>DataFrame</code> to join. Must not be null
     * @param col1 The name of the <code>Column</code> in the first DataFrame argument
     *             to match values for
     * @param df2 The second <code>DataFrame</code> to join. Must not be null
     * @param col2 The name of the <code>Column</code> in the second DataFrame argument
     *             to match values for
     * @return A <code>DataFrame</code> with joined rows from both specified DataFrames
     *         that have matching values in the columns with the specified names
     */
    public static DataFrame join(final DataFrame df1, final String col1,
                                 final DataFrame df2, final String col2){

        if((df1 == null) || (df2 == null)){
            throw new DataFrameException(
                    "DataFrame argument must not be null");
        }
        if(df1 == df2){
            throw new DataFrameException(
                    "Join operation is self-referential");
        }
        if((col1 == null) || col1.isEmpty()){
            throw new DataFrameException(
                    "First column name argument must not be null or empty");
        }
        if((col2 == null) || col2.isEmpty()){
            throw new DataFrameException(
                    "Second column name argument must not be null or empty");
        }
        if(!df1.hasColumnNames()){
            throw new DataFrameException(
                    "DataFrame must has column labels");
        }
        if(!df2.hasColumnNames()){
            throw new DataFrameException(
                    "DataFrame argument must have column labels");
        }
        if(!df2.hasColumn(col2)){
            throw new DataFrameException(
                    "Invalid column name for DataFrame argument: '" + col2 + "'");
        }
        if(!df1.getColumn(col1).memberClass().getSimpleName()
                .equals(df2.getColumn(col2).memberClass().getSimpleName())){

            throw new DataFrameException(
                    String.format("Column '%s' in DataFrame argument has "
                            + "a different type. "
                            + "Expected %s but found %s",
                            df2.getColumn(col2).getName(),
                            df1.getColumn(col1).getType(), 
                            df2.getColumn(col2).getType()));
        }
        //create a set holding the names of all columns from df2
        //that should be bypassed in the result because they already exist in df1
        final Collection<String> duplicates = new HashSet<>();
        final String[] n = df2.getColumnNames();
        for(int i=0; i<n.length; ++i){
            if(df1.hasColumn(n[i])){
                duplicates.add(n[i]);
            }
        }
        //add the specified column name to make sure
        //it is not included in the below computations
        duplicates.add(col2);
        df1.flush();
        df2.flush();
        //find the elements common to both DataFrames
        final DataFrame intersec = df1.getColumns(col1).intersectionRows(
                df2.getColumns(col2));

        final boolean useNullable = df1.isNullable() || df2.isNullable();
        final DataFrame res = useNullable
                ? new NullableDataFrame()
                : new DefaultDataFrame();

        //add all columns from df1
        for(int i=0; i<df1.columns(); ++i){
            final Column c = Column.ofType(df1.getColumn(i).typeCode());
            res.addColumn(df1.getColumn(i).name, useNullable ? c.asNullable() : c);
        }
        //add all columns from df2 as long as they are not already in df1
        for(int i=0; i<df2.columns(); ++i){
            final Column col = df2.getColumn(i);
            //if the column is in the collection, then it
            //is either 'col2' or another duplicate, so it is skipped
            if(!duplicates.contains(col.name)){
                final Column c = Column.ofType(col.typeCode());
                res.addColumn(col.name, useNullable ? c.asNullable() : c);
            }
        }
        //iterate over all common elements and add all rows to the result
        //from both DataFrames that match the common element
        //in their respective key column
        for(int i=0; i<intersec.rows(); ++i){
            final String filterKey = String.valueOf(
                    intersec.getColumn(0).getValue(i));

            final DataFrame filter1 = df1.filter(col1, filterKey);
            final DataFrame filter2 = df2.filter(col2, filterKey);
            //remove 'col2' and any column already existent in df1
            for(final String s : duplicates){
                filter2.removeColumn(s);
            }
            final int lengthCol1 = df1.columns();
            final int lengthCol2 = df2.columns() - duplicates.size();
            final int lengthRow = lengthCol1 + lengthCol2;
            //reuse the row as a carrier to avoid reallocation
            final Object[] row = new Object[lengthRow];
            for(int j=0; j<filter1.rows(); ++j){
                for(int k=0; k<filter2.rows(); ++k){
                    for(int l=0; l<lengthCol1; ++l){
                        row[l] = filter1.getColumn(l).getValue(j);
                    }
                    for(int l=0; l<lengthCol2; ++l){
                        row[lengthCol1+l] = filter2.getColumn(l).getValue(k);
                    }
                    res.addRow(row);
                }
            }
        }
        res.flush();
        return res;
    }

    /**
     * Wraps a ValueReplacement object into an IndexedValueReplacement object
     * for further processing
     * 
     * @param <T> The type used by the given <code>ValueReplacement</code> object
     * @param value The <code>ValueReplacement</code> object to wrap
     * @return An <code>IndexedValueReplacement</code> object which delegates
     *         all calls to the specified <code>ValueReplacement</code> argument
     */
    protected static <T> IndexedValueReplacement<T> indexedWrapper(
            final ValueReplacement<T> value){

        return new IndexedValueReplacement<T>(){
            @Override
            public T replace(int index, T val){
                return value.replace(val);
            }
        };
    }

    /**
     * Wraps a reference to a replacement object into an
     * IndexedValueReplacement object for further processing
     * 
     * @param value The <code>Object</code> to wrap
     * @return An <code>IndexedValueReplacement</code> object which
     *         always returns the specified <code>Object</code> reference
     */
    protected static IndexedValueReplacement<Object> indexedWrapper(
            final Object value){

        return new IndexedValueReplacement<Object>(){
            @Override
            public Object replace(int index, Object val){
                return value;
            }
        };
    }
}
