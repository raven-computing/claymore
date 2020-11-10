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

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * DataFrame implementation using primitive wrapper objects as the underlying 
 * data structure.<br>This implementation does permit null values.<br>
 * Columns which support the use of null values must be used with
 * this implementation.
 * 
 * <p>As described in the {@link DataFrame} interface, most methods of this
 * class can throw a {@link DataFrameException} at runtime if any argument passed
 * to it is invalid, for example an out of bounds index, or if that operation
 * would result in an incoherent/invalid state of that DataFrame.
 * 
 * <p>A NullableDataFrame is {@link Cloneable}, {@link Iterable}
 * 
 * <p>This implementation is NOT thread-safe.
 * 
 * @author Phil Gaiser
 * @see DefaultDataFrame
 * @since 1.0.0
 *
 */
public final class NullableDataFrame extends AbstractDataFrame implements DataFrame {

    /**
     * Constructs an empty <code>NullableDataFrame</code> without any columns set.
     */
    public NullableDataFrame(){
        this.next = -1;
    }

    /**
     * Constructs a new <code>NullableDataFrame</code> with the specified columns.
     * 
     * <p>If a column was labeled during its construction, that column will be
     * referenceable by that name. All columns which have not been labeled
     * during their construction will have no name assigned to them.<br>
     * The order of the columns within the constructed DataFrame is defined
     * by the order of the arguments passed to this constructor. All columns
     * must have the same size.
     * 
     * <p>This implementation must use {@link Column} instances which permit
     * null values. If a given column is not nullable, then this constructor
     * will convert that column before it is added to the
     * constructed NullableDataFrame instance
     * 
     * @param columns The Column instances comprising the constructed DataFrame 
     */
    public NullableDataFrame(final Column... columns){
        assignColumns(columns);
    }

    /**
     * Constructs a new <code>NullableDataFrame</code> with the specified
     * columns and assigns them the specified names. The number of columns
     * must be equal to the number of names.
     * 
     * <p>If a column was labeled during its construction, that label
     * is overridden by the corresponding label of the <i>names</i> argument.
     * The order of the columns within the constructed DataFrame is defined
     * by the order of the arguments passed to this constructor. The index of
     * the name in the array determines to which column that name will be
     * assigned to.<br>All columns must have the same size.
     * 
     * <p>This implementation must use {@link Column} instances which
     * permit null values. If a given column is not nullable, then this
     * constructor will convert that column before it is added to the
     * constructed NullableDataFrame instance
     * 
     * @param names The names of all columns
     * @param columns The Column instances comprising the constructed DataFrame 
     */
    public NullableDataFrame(final String[] names, final Column... columns){
        if(names.length != columns.length){
            throw new DataFrameException("Arguments must have equal length");
        }
        //override any set names with the provided argument strings
        for(int i=0; i<columns.length; ++i){
            columns[i].name = names[i];
        }
        assignColumns(columns);
    }

    /**
     * Constructs a new empty <code>NullableDataFrame</code> from the annotated
     * members in the specified class.<br>
     * The provided class must implement the {@link Row} interface. The type
     * of each member annotated with {@link RowItem} will be used to determine
     * the type of the column for that row item. If the annotation does not
     * specify a column name, then the identifying name of the member will be
     * used as the name for that column.<br>
     * Members not carrying the <code>RowItem</code> annotation are ignored
     * when creating the column structure.<br>
     * Please note that the order of the constructed columns within the
     * returned DataFrame is not necessarily the order in which the members
     * in the provided class are declared
     * 
     * @param structure The class defining a row in the NullableDataFrame to
     *                  be constructed, which is used to infer the
     *                  column structure. Must implement <code>Row</code>.
     *                  Must not be null
     */
    public NullableDataFrame(final Class<? extends Row> structure){
        if(structure == null){
            throw new DataFrameException("Row argument must not be null");
        }
        final Field[] fields = structure.getDeclaredFields();
        if(fields.length == 0){
            throw new DataFrameException(structure.getName()
                    + " class does not declare any fields");
        }
        String[] declaredNames = new String[fields.length];
        Column[] cols = new Column[fields.length];
        int i = 0;
        for(final Field field : fields){
            final RowItem item = field.getAnnotation(RowItem.class);
            if(item != null){
                String name = item.value();
                if((name == null || name.isEmpty())){
                    name = field.getName();
                }
                cols[i] = inferColumnFromType(field.getType());
                declaredNames[i] = name;
                ++i;
            }
        }
        if(i == 0){
            throw new DataFrameException(structure.getName()
                    + " class does not declare any annotated fields");
        }
        this.columns = new Column[i];
        for(int j=0; j<i; ++j){
            this.columns[j] = cols[j];
        }
        this.names = new HashMap<String, Integer>(16);
        for(int j=0; j<i; ++j){
            this.names.put(declaredNames[j], j);
            this.columns[j].name = declaredNames[j];
        }
        this.next = 0;
    }

    @Override
    public Byte getByte(final int col, final int row){
        if((next == -1) || (col < 0) || (col >= columns.length)){
            throw new DataFrameException("Invalid column index: " + col);
        }
        if((row < 0) || (row >= next)){
            throw new DataFrameException("Invalid row index: " + row);
        }
        if(columns[col].typeCode() != NullableByteColumn.TYPE_CODE){
            throw new DataFrameException(createInvalidGetMessage(
                    col, NullableByteColumn.TYPE_CODE, columns[col]));
        }
        return ((NullableByteColumn)columns[col]).get(row);
    }

    @Override
    public Byte getByte(final String col, final int row){
        final int c = enforceName(col);
        if((row < 0) || (row >= next)){
            throw new DataFrameException("Invalid row index: " + row);
        }
        if(columns[c].typeCode() != NullableByteColumn.TYPE_CODE){
            throw new DataFrameException(createInvalidGetMessage(
                    c, NullableByteColumn.TYPE_CODE, columns[c]));
        }
        return ((NullableByteColumn)columns[c]).get(row);
    }

    @Override
    public Short getShort(final int col, final int row){
        if((next == -1) || (col < 0) || (col >= columns.length)){
            throw new DataFrameException("Invalid column index: " + col);
        }
        if((row < 0) || (row >= next)){
            throw new DataFrameException("Invalid row index: " + row);
        }
        if(columns[col].typeCode() != NullableShortColumn.TYPE_CODE){
            throw new DataFrameException(createInvalidGetMessage(
                    col, NullableShortColumn.TYPE_CODE, columns[col]));
        }
        return ((NullableShortColumn)columns[col]).get(row);
    }

    @Override
    public Short getShort(final String col, final int row){
        final int c = enforceName(col);
        if((row < 0) || (row >= next)){
            throw new DataFrameException("Invalid row index: " + row);
        }
        if(columns[c].typeCode() != NullableShortColumn.TYPE_CODE){
            throw new DataFrameException(createInvalidGetMessage(
                    c, NullableShortColumn.TYPE_CODE, columns[c]));
        }
        return ((NullableShortColumn)columns[c]).get(row);
    }

    @Override
    public Integer getInt(final int col, final int row){
        if((next == -1) || (col < 0) || (col >= columns.length)){
            throw new DataFrameException("Invalid column index: " + col);
        }
        if((row < 0) || (row >= next)){
            throw new DataFrameException("Invalid row index: " + row);
        }
        if(columns[col].typeCode() != NullableIntColumn.TYPE_CODE){
            throw new DataFrameException(createInvalidGetMessage(
                    col, NullableIntColumn.TYPE_CODE, columns[col]));
        }
        return ((NullableIntColumn)columns[col]).get(row);
    }

    @Override
    public Integer getInt(final String col, final int row){
        final int c = enforceName(col);
        if((row < 0) || (row >= next)){
            throw new DataFrameException("Invalid row index: " + row);
        }
        if(columns[c].typeCode() != NullableIntColumn.TYPE_CODE){
            throw new DataFrameException(createInvalidGetMessage(
                    c, NullableIntColumn.TYPE_CODE, columns[c]));
        }
        return ((NullableIntColumn)columns[c]).get(row);
    }

    @Override
    public Long getLong(final int col, final int row){
        if((next == -1) || (col < 0) || (col >= columns.length)){
            throw new DataFrameException("Invalid column index: " + col);
        }
        if((row < 0) || (row >= next)){
            throw new DataFrameException("Invalid row index: " + row);
        }
        if(columns[col].typeCode() != NullableLongColumn.TYPE_CODE){
            throw new DataFrameException(createInvalidGetMessage(
                    col, NullableLongColumn.TYPE_CODE, columns[col]));
        }
        return ((NullableLongColumn)columns[col]).get(row);
    }

    @Override
    public Long getLong(final String col, final int row){
        final int c = enforceName(col);
        if((row < 0) || (row >= next)){
            throw new DataFrameException("Invalid row index: " + row);
        }
        if(columns[c].typeCode() != NullableLongColumn.TYPE_CODE){
            throw new DataFrameException(createInvalidGetMessage(
                    c, NullableLongColumn.TYPE_CODE, columns[c]));
        }
        return ((NullableLongColumn)columns[c]).get(row);
    }

    @Override
    public String getString(final int col, final int row){
        if((next == -1) || (col < 0) || (col >= columns.length)){
            throw new DataFrameException("Invalid column index: " + col);
        }
        if((row < 0) || (row >= next)){
            throw new DataFrameException("Invalid row index: " + row);
        }
        if(columns[col].typeCode() != NullableStringColumn.TYPE_CODE){
            throw new DataFrameException(createInvalidGetMessage(
                    col, NullableStringColumn.TYPE_CODE, columns[col]));
        }
        return ((NullableStringColumn)columns[col]).get(row);
    }

    @Override
    public String getString(final String col, final int row){
        final int c = enforceName(col);
        if((row < 0) || (row >= next)){
            throw new DataFrameException("Invalid row index: " + row);
        }
        if(columns[c].typeCode() != NullableStringColumn.TYPE_CODE){
            throw new DataFrameException(createInvalidGetMessage(
                    c, NullableStringColumn.TYPE_CODE, columns[c]));
        }
        return ((NullableStringColumn)columns[c]).get(row);
    }

    @Override
    public Float getFloat(final int col, final int row){
        if((next == -1) || (col < 0) || (col >= columns.length)){
            throw new DataFrameException("Invalid column index: " + col);
        }
        if((row < 0) || (row >= next)){
            throw new DataFrameException("Invalid row index: " + row);
        }
        if(columns[col].typeCode() != NullableFloatColumn.TYPE_CODE){
            throw new DataFrameException(createInvalidGetMessage(
                    col, NullableFloatColumn.TYPE_CODE, columns[col]));
        }
        return ((NullableFloatColumn)columns[col]).get(row);
    }

    @Override
    public Float getFloat(final String col, final int row){
        final int c = enforceName(col);
        if((row < 0) || (row >= next)){
            throw new DataFrameException("Invalid row index: " + row);
        }
        if(columns[c].typeCode() != NullableFloatColumn.TYPE_CODE){
            throw new DataFrameException(createInvalidGetMessage(
                    c, NullableFloatColumn.TYPE_CODE, columns[c]));
        }
        return ((NullableFloatColumn)columns[c]).get(row);
    }

    @Override
    public Double getDouble(final int col, final int row){
        if((next == -1) || (col < 0) || (col >= columns.length)){
            throw new DataFrameException("Invalid column index: " + col);
        }
        if((row < 0) || (row >= next)){
            throw new DataFrameException("Invalid row index: " + row);
        }
        if(columns[col].typeCode() != NullableDoubleColumn.TYPE_CODE){
            throw new DataFrameException(createInvalidGetMessage(
                    col, NullableDoubleColumn.TYPE_CODE, columns[col]));
        }
        return ((NullableDoubleColumn)columns[col]).get(row);
    }

    @Override
    public Double getDouble(final String col, final int row){
        final int c = enforceName(col);
        if((row < 0) || (row >= next)){
            throw new DataFrameException("Invalid row index: " + row);
        }
        if(columns[c].typeCode() != NullableDoubleColumn.TYPE_CODE){
            throw new DataFrameException(createInvalidGetMessage(
                    c, NullableDoubleColumn.TYPE_CODE, columns[c]));
        }
        return ((NullableDoubleColumn)columns[c]).get(row);
    }

    @Override
    public Character getChar(final int col, final int row){
        if((next == -1) || (col < 0) || (col >= columns.length)){
            throw new DataFrameException("Invalid column index: " + col);
        }
        if((row < 0) || (row >= next)){
            throw new DataFrameException("Invalid row index: " + row);
        }
        if(columns[col].typeCode() != NullableCharColumn.TYPE_CODE){
            throw new DataFrameException(createInvalidGetMessage(
                    col, NullableCharColumn.TYPE_CODE, columns[col]));
        }
        return ((NullableCharColumn)columns[col]).get(row);
    }

    @Override
    public Character getChar(final String col, final int row){
        final int c = enforceName(col);
        if((row < 0) || (row >= next)){
            throw new DataFrameException("Invalid row index: " + row);
        }
        if(columns[c].typeCode() != NullableCharColumn.TYPE_CODE){
            throw new DataFrameException(createInvalidGetMessage(
                    c, NullableCharColumn.TYPE_CODE, columns[c]));
        }
        return ((NullableCharColumn)columns[c]).get(row);
    }

    @Override
    public Boolean getBoolean(final int col, final int row){
        if((next == -1) || (col < 0) || (col >= columns.length)){
            throw new DataFrameException("Invalid column index: " + col);
        }
        if((row < 0) || (row >= next)){
            throw new DataFrameException("Invalid row index: " + row);
        }
        if(columns[col].typeCode() != NullableBooleanColumn.TYPE_CODE){
            throw new DataFrameException(createInvalidGetMessage(
                    col, NullableBooleanColumn.TYPE_CODE, columns[col]));
        }
        return ((NullableBooleanColumn)columns[col]).get(row);
    }

    @Override
    public Boolean getBoolean(final String col, final int row){
        final int c = enforceName(col);
        if((row < 0) || (row >= next)){
            throw new DataFrameException("Invalid row index: " + row);
        }
        if(columns[c].typeCode() != NullableBooleanColumn.TYPE_CODE){
            throw new DataFrameException(createInvalidGetMessage(
                    c, NullableBooleanColumn.TYPE_CODE, columns[c]));
        }
        return ((NullableBooleanColumn)columns[c]).get(row);
    }

    @Override
    public byte[] getBinary(final int col, final int row){
        if((next == -1) || (col < 0) || (col >= columns.length)){
            throw new DataFrameException("Invalid column index: " + col);
        }
        if((row < 0) || (row >= next)){
            throw new DataFrameException("Invalid row index: " + row);
        }
        if(columns[col].typeCode() != NullableBinaryColumn.TYPE_CODE){
            throw new DataFrameException(createInvalidGetMessage(
                    col, NullableBinaryColumn.TYPE_CODE, columns[col]));
        }
        return ((NullableBinaryColumn)columns[col]).get(row);
    }

    @Override
    public byte[] getBinary(final String col, final int row){
        final int c = enforceName(col);
        if((row < 0) || (row >= next)){
            throw new DataFrameException("Invalid row index: " + row);
        }
        if(columns[c].typeCode() != NullableBinaryColumn.TYPE_CODE){
            throw new DataFrameException(createInvalidGetMessage(
                    c, NullableBinaryColumn.TYPE_CODE, columns[c]));
        }
        return ((NullableBinaryColumn)columns[c]).get(row);
    }

    @Override
    public Number getNumber(final int col, final int row){
        if((next == -1) || (col < 0) || (col >= columns.length)){
            throw new DataFrameException("Invalid column index: " + col);
        }
        if((row < 0) || (row >= next)){
            throw new DataFrameException("Invalid row index: " + row);
        }
        if(!columns[col].isNumeric()){
            final String msg = (columns[col].name != null)
                    ? "'" + columns[col].name + "'"
                    : ("at index " + col);

            throw new DataFrameException(
                    "Cannot get number from column " + msg
                    + ". Expected numeric column but found "
                    + columns[col].getClass().getSimpleName());
        }
        return (Number) columns[col].getValue(row);
    }

    @Override
    public Number getNumber(final String col, final int row){
        return getNumber(enforceName(col), row);
    }

    @Override
    public void setByte(final int col, final int row, final Byte value){
        if((next == -1) || (col < 0) || (col >= columns.length)){
            throw new DataFrameException("Invalid column index: " + col);
        }
        if((row < 0) || (row >= next)){
            throw new DataFrameException("Invalid row index: " + row);
        }
        if(columns[col].typeCode() != NullableByteColumn.TYPE_CODE){
            throw new DataFrameException(createInvalidSetMessage(
                    col, NullableByteColumn.TYPE_CODE, columns[col]));
        }
        ((NullableByteColumn)columns[col]).set(row, value);
    }

    @Override
    public void setByte(final String col, final int row, final Byte value){
        final int c = enforceName(col);
        if((row < 0) || (row >= next)){
            throw new DataFrameException("Invalid row index: " + row);
        }
        if(columns[c].typeCode() != NullableByteColumn.TYPE_CODE){
            throw new DataFrameException(createInvalidSetMessage(
                    c, NullableByteColumn.TYPE_CODE, columns[c]));
        }
        ((NullableByteColumn)columns[c]).set(row, value);
    }

    @Override
    public void setShort(final int col, final int row, final Short value){
        if((next == -1) || (col < 0) || (col >= columns.length)){
            throw new DataFrameException("Invalid column index: " + col);
        }
        if((row < 0) || (row >= next)){
            throw new DataFrameException("Invalid row index: " + row);
        }
        if(columns[col].typeCode() != NullableShortColumn.TYPE_CODE){
            throw new DataFrameException(createInvalidSetMessage(
                    col, NullableShortColumn.TYPE_CODE, columns[col]));
        }
        ((NullableShortColumn)columns[col]).set(row, value);
    }

    @Override
    public void setShort(final String col, final int row, final Short value){
        final int c = enforceName(col);
        if((row < 0) || (row >= next)){
            throw new DataFrameException("Invalid row index: " + row);
        }
        if(columns[c].typeCode() != NullableShortColumn.TYPE_CODE){
            throw new DataFrameException(createInvalidSetMessage(
                    c, NullableShortColumn.TYPE_CODE, columns[c]));
        }
        ((NullableShortColumn)columns[c]).set(row, value);
    }

    @Override
    public void setInt(final int col, final int row, final Integer value){
        if((next == -1) || (col < 0) || (col >= columns.length)){
            throw new DataFrameException("Invalid column index: " + col);
        }
        if((row < 0) || (row >= next)){
            throw new DataFrameException("Invalid row index: " + row);
        }
        if(columns[col].typeCode() != NullableIntColumn.TYPE_CODE){
            throw new DataFrameException(createInvalidSetMessage(
                    col, NullableIntColumn.TYPE_CODE, columns[col]));
        }
        ((NullableIntColumn)columns[col]).set(row, value);
    }

    @Override
    public void setInt(final String col, final int row, final Integer value){
        final int c = enforceName(col);
        if((row < 0) || (row >= next)){
            throw new DataFrameException("Invalid row index: " + row);
        }
        if(columns[c].typeCode() != NullableIntColumn.TYPE_CODE){
            throw new DataFrameException(createInvalidSetMessage(
                    c, NullableIntColumn.TYPE_CODE, columns[c]));
        }
        ((NullableIntColumn)columns[c]).set(row, value);
    }

    @Override
    public void setLong(final int col, final int row, final Long value){
        if((next == -1) || (col < 0) || (col >= columns.length)){
            throw new DataFrameException("Invalid column index: " + col);
        }
        if((row < 0) || (row >= next)){
            throw new DataFrameException("Invalid row index: " + row);
        }
        if(columns[col].typeCode() != NullableLongColumn.TYPE_CODE){
            throw new DataFrameException(createInvalidSetMessage(
                    col, NullableLongColumn.TYPE_CODE, columns[col]));
        }
        ((NullableLongColumn)columns[col]).set(row, value);
    }

    @Override
    public void setLong(final String col, final int row, final Long value){
        final int c = enforceName(col);
        if((row < 0) || (row >= next)){
            throw new DataFrameException("Invalid row index: " + row);
        }
        if(columns[c].typeCode() != NullableLongColumn.TYPE_CODE){
            throw new DataFrameException(createInvalidSetMessage(
                    c, NullableLongColumn.TYPE_CODE, columns[c]));
        }
        ((NullableLongColumn)columns[c]).set(row, value);
    }

    @Override
    public void setString(final int col, final int row, final String value){
        if((next == -1) || (col < 0) || (col >= columns.length)){
            throw new DataFrameException("Invalid column index: " + col);
        }
        if((row < 0) || (row >= next)){
            throw new DataFrameException("Invalid row index: " + row);
        }
        if(columns[col].typeCode() != NullableStringColumn.TYPE_CODE){
            throw new DataFrameException(createInvalidSetMessage(
                    col, NullableStringColumn.TYPE_CODE, columns[col]));
        }
        ((NullableStringColumn)columns[col]).set(row, value);
    }

    @Override
    public void setString(final String col, final int row, final String value){
        final int c = enforceName(col);
        if((row < 0) || (row >= next)){
            throw new DataFrameException("Invalid row index: " + row);
        }
        if(columns[c].typeCode() != NullableStringColumn.TYPE_CODE){
            throw new DataFrameException(createInvalidSetMessage(
                    c, NullableStringColumn.TYPE_CODE, columns[c]));
        }
        ((NullableStringColumn)columns[c]).set(row, value);
    }

    @Override
    public void setFloat(final int col, final int row, final Float value){
        if((next == -1) || (col < 0) || (col >= columns.length)){
            throw new DataFrameException("Invalid column index: " + col);
        }
        if((row < 0) || (row >= next)){
            throw new DataFrameException("Invalid row index: " + row);
        }
        if(columns[col].typeCode() != NullableFloatColumn.TYPE_CODE){
            throw new DataFrameException(createInvalidSetMessage(
                    col, NullableFloatColumn.TYPE_CODE, columns[col]));
        }
        ((NullableFloatColumn)columns[col]).set(row, value);
    }

    @Override
    public void setFloat(final String col, final int row, final Float value){
        final int c = enforceName(col);
        if((row < 0) || (row >= next)){
            throw new DataFrameException("Invalid row index: " + row);
        }
        if(columns[c].typeCode() != NullableFloatColumn.TYPE_CODE){
            throw new DataFrameException(createInvalidSetMessage(
                    c, NullableFloatColumn.TYPE_CODE, columns[c]));
        }
        ((NullableFloatColumn)columns[c]).set(row, value);
    }

    @Override
    public void setDouble(final int col, final int row, final Double value){
        if((next == -1) || (col < 0) || (col >= columns.length)){
            throw new DataFrameException("Invalid column index: " + col);
        }
        if((row < 0) || (row >= next)){
            throw new DataFrameException("Invalid row index: " + row);
        }
        if(columns[col].typeCode() != NullableDoubleColumn.TYPE_CODE){
            throw new DataFrameException(createInvalidSetMessage(
                    col, NullableDoubleColumn.TYPE_CODE, columns[col]));
        }
        ((NullableDoubleColumn)columns[col]).set(row, value);
    }

    @Override
    public void setDouble(final String col, final int row, final Double value){
        final int c = enforceName(col);
        if((row < 0) || (row >= next)){
            throw new DataFrameException("Invalid row index: " + row);
        }
        if(columns[c].typeCode() != NullableDoubleColumn.TYPE_CODE){
            throw new DataFrameException(createInvalidSetMessage(
                    c, NullableDoubleColumn.TYPE_CODE, columns[c]));
        }
        ((NullableDoubleColumn)columns[c]).set(row, value);
    }

    @Override
    public void setChar(final int col, final int row, final Character value){
        if((next == -1) || (col < 0) || (col >= columns.length)){
            throw new DataFrameException("Invalid column index: " + col);
        }
        if((row < 0) || (row >= next)){
            throw new DataFrameException("Invalid row index: " + row);
        }
        if((value != null) && ((value < 32) || (value > 126))){
            throw new DataFrameException("Invalid character value. "
                                       + "Only printable ASCII is permitted");
        }
        if(columns[col].typeCode() != NullableCharColumn.TYPE_CODE){
            throw new DataFrameException(createInvalidSetMessage(
                    col, NullableCharColumn.TYPE_CODE, columns[col]));
        }
        ((NullableCharColumn)columns[col]).set(row, value);
    }

    @Override
    public void setChar(final String col, final int row, final Character value){
        final int c = enforceName(col);
        if((row < 0) || (row >= next)){
            throw new DataFrameException("Invalid row index: " + row);
        }
        if((value != null) && ((value < 32) || (value > 126))){
            throw new DataFrameException("Invalid character value. "
                                       + "Only printable ASCII is permitted");
        }
        if(columns[c].typeCode() != NullableCharColumn.TYPE_CODE){
            throw new DataFrameException(createInvalidSetMessage(
                    c, NullableCharColumn.TYPE_CODE, columns[c]));
        }
        ((NullableCharColumn)columns[c]).set(row, value);
    }

    @Override
    public void setBoolean(final int col, final int row, final Boolean value){
        if((next == -1) || (col < 0) || (col >= columns.length)){
            throw new DataFrameException("Invalid column index: " + col);
        }
        if((row < 0) || (row >= next)){
            throw new DataFrameException("Invalid row index: " + row);
        }
        if(columns[col].typeCode() != NullableBooleanColumn.TYPE_CODE){
            throw new DataFrameException(createInvalidSetMessage(
                    col, NullableBooleanColumn.TYPE_CODE, columns[col]));
        }
        ((NullableBooleanColumn)columns[col]).set(row, value);
    }

    @Override
    public void setBoolean(final String col, final int row, final Boolean value){
        final int c = enforceName(col);
        if((row < 0) || (row >= next)){
            throw new DataFrameException("Invalid row index: " + row);
        }
        if(columns[c].typeCode() != NullableBooleanColumn.TYPE_CODE){
            throw new DataFrameException(createInvalidSetMessage(
                    c, NullableBooleanColumn.TYPE_CODE, columns[c]));
        }
        ((NullableBooleanColumn)columns[c]).set(row, value);
    }

    @Override
    public void setBinary(final int col, final int row, final byte[] value){
        if((next == -1) || (col < 0) || (col >= columns.length)){
            throw new DataFrameException("Invalid column index: " + col);
        }
        if((row < 0) || (row >= next)){
            throw new DataFrameException("Invalid row index: " + row);
        }
        if(columns[col].typeCode() != NullableBinaryColumn.TYPE_CODE){
            throw new DataFrameException(createInvalidSetMessage(
                    col, NullableBinaryColumn.TYPE_CODE, columns[col]));
        }
        ((NullableBinaryColumn)columns[col]).set(row, value);
    }

    @Override
    public void setBinary(final String col, final int row, final byte[] value){
        final int c = enforceName(col);
        if((row < 0) || (row >= next)){
            throw new DataFrameException("Invalid row index: " + row);
        }
        if(columns[c].typeCode() != NullableBinaryColumn.TYPE_CODE){
            throw new DataFrameException(createInvalidSetMessage(
                    c, NullableBinaryColumn.TYPE_CODE, columns[c]));
        }
        ((NullableBinaryColumn)columns[c]).set(row, value);
    }

    @Override
    public void setNumber(final int col, final int row, final Number value){
        if((next == -1) || (col < 0) || (col >= columns.length)){
            throw new DataFrameException("Invalid column index: " + col);
        }
        if((row < 0) || (row >= next)){
            throw new DataFrameException("Invalid row index: " + row);
        }
        if(!columns[col].isNumeric()){
            final String msg = (columns[col].name != null)
                    ? "'" + columns[col].name + "'"
                    : ("at index " + col);

            throw new DataFrameException(
                   "Cannot set number in column " + msg
                 + ". Expected numeric column but found "
                 + columns[col].getClass().getSimpleName());
        }
        if(value == null){
            columns[col].setValue(row, null);
            return;
        }
        switch(columns[col].typeCode()){
        case NullableByteColumn.TYPE_CODE:
            ((NullableByteColumn)columns[col]).set(row, value.byteValue());
            break;
        case NullableShortColumn.TYPE_CODE:
            ((NullableShortColumn)columns[col]).set(row, value.shortValue());
            break;
        case NullableIntColumn.TYPE_CODE:
            ((NullableIntColumn)columns[col]).set(row, value.intValue());
            break;
        case NullableLongColumn.TYPE_CODE:
            ((NullableLongColumn)columns[col]).set(row, value.longValue());
            break;
        case NullableFloatColumn.TYPE_CODE:
            ((NullableFloatColumn)columns[col]).set(row, value.floatValue());
            break;
        case NullableDoubleColumn.TYPE_CODE:
            ((NullableDoubleColumn)columns[col]).set(row, value.doubleValue());
            break;
        }
    }

    @Override
    public void setNumber(final String col, final int row, final Number value){
        setNumber(enforceName(col), row, value);
    }

    @Override
    public DataFrame addColumn(Column col){
        if(col == null){
            throw new DataFrameException(
                    "Column argument must not be null");
        }
        if(!col.isNullable()){
            throw new DataFrameException(
                    "NullableDataFrame must use NullableColumn instance");
        }
        if((col.capacity() == 0) && (next > 0)){
            col = Column.like(col, next);
        }
        if(next == -1){
            this.columns = new Column[1];
            this.columns[0] = col;
            this.next = col.capacity();
            if((col.name != null) && !(col.name.isEmpty())){
                this.names = new HashMap<String, Integer>(16);
                this.names.put(col.name, 0);
            }
        }else{
            if(col.capacity() > next){
                final int diff = (col.capacity() - next);
                for(int i=0; i<diff; ++i){
                    addRow(new Object[columns.length]);
                }
            }
            col.matchLength(capacity());
            final Column[] tmp = new Column[columns.length+1];
            for(int i=0; i<columns.length; ++i){
                tmp[i] = columns[i];
            }
            tmp[columns.length] = col;
            if((col.name != null) && !(col.name.isEmpty())){
                if(names == null){
                    this.names = new HashMap<String, Integer>(16);
                }
                this.names.put(col.name, columns.length);
            }
            this.columns = tmp;
        }
        return this;
    }

    @Override
    public DataFrame addColumn(final String colName, Column col){
        if((colName == null) || (colName.isEmpty()) || (col == null)){
            throw new DataFrameException(
                    "Column argument must not be null or empty");
        }
        if(!col.isNullable()){
            throw new DataFrameException(
                    "NullableDataFrame must use NullableColumn instance");
        }
        if((col.capacity() == 0) && (next > 0)){
            col = Column.like(col, next);
        }
        if(next == -1){
            this.columns = new Column[1];
            this.columns[0] = col;
            this.next = col.capacity();
            this.names = new HashMap<String, Integer>(16);
            this.names.put(colName, 0);
            col.name = colName;
        }else{
            if(col.capacity() > next){
                final int diff = (col.capacity() - next);
                for(int i=0; i<diff; ++i){
                    addRow(new Object[columns.length]);
                }
            }
            col.matchLength(capacity());
            final Column[] tmp = new Column[columns.length+1];
            for(int i=0; i<columns.length; ++i){
                tmp[i] = columns[i];
            }
            tmp[columns.length] = col;
            this.columns = tmp;
            if(this.names == null){
                this.names = new HashMap<String, Integer>(16);
            }
            this.names.put(colName, columns.length-1);
            col.name = colName;
        }
        return this;
    }

    @Override
    public DataFrame insertColumn(final int index, Column col){
        if(col == null){
            throw new DataFrameException(
                    "Column argument must not be null");
        }
        if(!col.isNullable()){
            throw new DataFrameException(
                    "NullableDataFrame must use NullableColumn instance");
        }
        if((col.capacity() == 0) && (next > 0)){
            col = Column.like(col, next);
        }
        if(next == -1){
            if(index != 0){
                throw new DataFrameException("Invalid column index: " + index);
            }
            this.columns = new Column[1];
            this.columns[0] = col;
            this.next = col.capacity();
            if((col.name != null) && !(col.name.isEmpty())){
                this.names = new HashMap<String, Integer>(16);
                this.names.put(col.name, 0);
            }
        }else{
            if((index < 0) || (index > columns.length)){
                throw new DataFrameException("Invalid column index: " + index);
            }
            if(col.capacity() > next){
                final int diff = (col.capacity() - next);
                final Object[] nulls = new Object[columns.length];
                for(int i=0; i<diff; ++i){
                    addRow(nulls);
                }
            }
            col.matchLength(capacity());
            final Column[] tmp = new Column[columns.length+1];
            for(int i=tmp.length-1; i>index; --i){
                tmp[i] = columns[i-1];
            }
            tmp[index] = col;
            for(int i=0; i<index; ++i){
                tmp[i] = columns[i];
            }
            this.columns = tmp;
            if(names != null){
                Iterator<Map.Entry<String, Integer>> iter = names.entrySet().iterator();
                while(iter.hasNext()){
                    final Map.Entry<String, Integer> entry = iter.next();
                    if(entry.getValue()>=index){
                        entry.setValue(entry.getValue()+1);
                    }
                }
            }
            if((col.name != null) && !(col.name.isEmpty())){
                if(names == null){
                    this.names = new HashMap<String, Integer>(16);
                }
                this.names.put(col.name, index);
            }
        }
        return this;
    }

    @Override
    public boolean isNullable(){
        return true;
    }

    @Override
    public DataFrame setColumn(final int index, Column col){
        if(col == null){
            throw new DataFrameException(
                    "Column argument must not be null");
        }
        if(!col.isNullable()){
            throw new DataFrameException(
                    "NullableDataFrame must use NullableColumn instance");
        }
        if((next == -1) || (index < 0) || (index >= columns.length)){
            throw new DataFrameException("Invalid column index: " + index);
        }
        if((col.capacity() == 0) && (next > 0)){
            col = Column.like(col, next);
        }
        if(col.capacity() != next){
            throw new DataFrameException(
                    "Invalid column length. Must be of length " + next);
        }
        col.matchLength(capacity());
        final String oldName = columns[index].name;
        columns[index] = col;
        if((col.name != null) && !(col.name.isEmpty())){
            if((names != null) && (oldName != null)){
                this.names.remove(oldName);
            }
            if(names == null){
                this.names = new HashMap<String, Integer>(16);
            }
            this.names.put(col.name, index);
        }else{
            col.name = oldName;
        }
        return this;
    }

    @Override
    public DataFrame setColumn(final String colName, Column col){
        if((colName == null) || (colName.isEmpty())){
            throw new DataFrameException("Column name must not be null or empty");
        }
        if(next == -1){
            return addColumn(colName, col);
        }
        if(col == null){
            throw new DataFrameException("Column argument must not be null");
        }
        if(!col.isNullable()){
            throw new DataFrameException(
                    "NullableDataFrame must use NullableColumn instance");

        }
        if((col.capacity() == 0) && (next > 0)){
            col = Column.like(col, next);
        }
        if(names == null){
            this.names = new HashMap<String, Integer>(16);
        }
        final Integer i = names.get(colName);
        if(i != null){//replace
            if(col.capacity() != next){
                throw new DataFrameException(
                        "Invalid column length. Must be of length " + next);
            }
            col.matchLength(capacity());
            this.columns[i] = col;
            col.name = colName;
        }else{//add
            addColumn(colName, col);
        }
        return this;
    }

    @Override
    public DataFrame convert(final int col, final byte typeCode){
        if((next == -1) || (col < 0) || (col >= columns.length)){
            throw new DataFrameException("Invalid column index: " + col);
        }
        Column c = columns[col];
        if(c.typeCode() == typeCode){
            return this;
        }
        this.flush();
        try{
            c = c.convertTo(typeCode);
        }catch(NumberFormatException ex){
            throw new DataFrameException(
                    "Cannot convert column. Invalid number format", ex);
        }
        if(!c.isNullable()){
            throw new DataFrameException(
                    "NullableDataFrame must use NullableColumn instance");
        }
        columns[col] = c;
        return this;
    }

    @Override
    public DataFrame convert(final String col, final byte typeCode){
        return convert(enforceName(col), typeCode);
    }

    @Override
    public int replace(final DataFrame df){
        if(df == null){
            return 0;//NO-OP
        }
        if(df.rows() != next){
            throw new DataFrameException(
                    String.format("Row count differs. Expected %s rows but found %s",
                                   next, df.rows()));

        }
        if(this.hasColumnNames() ^ df.hasColumnNames()){
            throw new DataFrameException("Cannot replace columns. "
                    + "DataFrames must be both either labeled or unlabeled");
        }
        this.flush();
        df.flush();
        int replaced = 0;
        if(this.hasColumnNames()){
            for(int i=0; i<df.columns(); ++i){
                final Column col = df.getColumn(i);
                final String name = col.getName();
                if((name != null) && !name.isEmpty() && hasColumn(name)){
                   this.setColumn(name, col.asNullable());
                   ++replaced;
                }
            }
        }else{
            for(int i=0; i<df.columns(); ++i){
                final Column col = df.getColumn(i);
                if(replaced < this.columns()){
                    this.setColumn(replaced, col.asNullable());
                    ++replaced;
                }
            }
        }
        return replaced;
    }

    @Override
    public Map<Object, Integer> factor(final int col){
        if((next == -1) || (col < 0) || (col >= columns.length)){
            throw new DataFrameException("Invalid column index: " + col);
        }
        final Column c = columns[col];
        if(c.isNumeric()){
            return new HashMap<Object, Integer>();
        }
        final Map<Object, Integer> map = new HashMap<>();
        final NullableIntColumn factors = new NullableIntColumn(capacity());
        factors.name = c.name;
        int totalFactors = 0;
        for(int i=0; i<next; ++i){
            final Object val = c.getValue(i);
            if(val == null){
                continue;
            }
            final Integer factor = map.get(val);
            if(factor != null){
                factors.set(i, factor);
            }else{
                ++totalFactors;
                map.put(val, totalFactors);
                factors.set(i, totalFactors);
            }
        }
        columns[col] = factors;
        return map;
    }

    @Override
    public Map<Object, Integer> factor(final String col){
        return factor(enforceName(col));
    }

    @Override
    public DataFrame count(final int col){
        if((next == -1) || (col < 0) || (col >= columns.length)){
            throw new DataFrameException("Invalid column index: " + col);
        }
        final Column c = columns[col];
        final DataFrame df = new NullableDataFrame(
                Column.ofType(c.typeCode()),
                new NullableIntColumn("count"),
                new NullableFloatColumn("%"));
        
        String name = c.name;
        if((name != null) && !name.isEmpty()){
            if(name.equals("count") || name.equals("%")){
                name = name + "_";
            }
            df.setColumnName(0, name);
        }
        final Map<Object, Integer> map = new HashMap<>();
        for(int i=0; i<next; ++i){
            final Object value = c.getValue(i);
            final Integer count = map.get(value);
            if(count != null){
                map.put(value, count + 1);
            }else{
                map.put(value, 1);
            }
        }
        for(final Map.Entry<Object, Integer> count : map.entrySet()){
            if(count.getKey() != null){//skip null counts
                df.addRow(count.getKey(),
                          count.getValue(),
                          (float)count.getValue() / next);
            }
        }
        //add null count as the last row
        if(map.containsKey(null)){
            df.addRow(null,
                      map.get(null),
                      (float)map.get(null) / next);
        }
        return df;
    }

    @Override
    public DataFrame count(final String col){
        return count(enforceName(col));
    }

    @Override
    public int countUnique(final int col){
        if((next == -1) || (col < 0) || (col >= columns.length)){
            throw new DataFrameException("Invalid column index: " + col);
        }
        final Column c = columns[col];
        final Set<Object> unique = new HashSet<>();
        for(int i=0; i<next; ++i){
            final Object value = c.getValue(i);
            if(value != null){
                unique.add(value);
            }
        }
        return unique.size();
    }

    @Override
    public int countUnique(final String col){
        return countUnique(enforceName(col));
    }

    @Override
    public DataFrame differenceColumns(final DataFrame df){
        ensureValidColumnSetOperation(df);
        if(!df.isNullable()){
            throw new DataFrameException(
                    "Argument must be a NullableDataFrame instance");
        }
        final DataFrame res = new NullableDataFrame();
        for(int i=0; i<columns.length; ++i){
            final String name = columns[i].name;
            if((name == null) || name.isEmpty()){
                throw new DataFrameException(
                        "Encountered an unlabeled "
                        + "column at index " + i);
            }
            if(!df.hasColumn(name)){
                res.addColumn(columns[i]);
            }
        }
        final int length = df.columns();
        for(int i=0; i<length; ++i){
            final Column col = df.getColumn(i);
            final String name = col.getName();
            if((name == null) || name.isEmpty()){
                throw new DataFrameException(
                        "Encountered an unlabeled "
                        + "column in the argument DataFrame at index "
                        + i);
            }
            if(!hasColumn(name)){
                res.addColumn(col);
            }
        }
        return res;
    }

    @Override
    public DataFrame unionColumns(final DataFrame df){
        ensureValidColumnSetOperation(df);
        if(!df.isNullable()){
            throw new DataFrameException(
                    "Argument must be a NullableDataFrame instance");
        }
        final DataFrame res = new NullableDataFrame();
        for(int i=0; i<columns.length; ++i){
            final String name = columns[i].name;
            if((name == null) || name.isEmpty()){
                throw new DataFrameException(
                        "Encountered an unlabeled "
                        + "column at index " + i);
            }
            res.addColumn(columns[i]);
        }
        final int length = df.columns();
        for(int i=0; i<length; ++i){
            final Column col = df.getColumn(i);
            final String name = col.getName();
            if((name == null) || name.isEmpty()){
                throw new DataFrameException(
                        "Encountered an unlabeled "
                        + "column in the argument DataFrame at index "
                        + i);
            }
            if(!res.hasColumn(name)){
                res.addColumn(col);
            }
        }
        return res;
    }

    @Override
    public DataFrame intersectionColumns(final DataFrame df){
        ensureValidColumnSetOperation(df);
        if(!df.isNullable()){
            throw new DataFrameException(
                    "Argument must be a NullableDataFrame instance");
        }
        final DataFrame res = new NullableDataFrame();
        for(int i=0; i<columns.length; ++i){
            final String name = columns[i].name;
            if((name == null) || name.isEmpty()){
                throw new DataFrameException(
                        "Encountered an unlabeled "
                        + "column at index " + i);
            }
            if(df.hasColumn(name)){
                res.addColumn(columns[i]);
            }
        }
        return res;
    }

    @Override
    public DataFrame differenceRows(final DataFrame df){
        ensureValidRowSetOperation(df);
        final DataFrame res = new NullableDataFrame();
        for(int i=0; i<columns.length; ++i){
            res.addColumn(Column.ofType(columns[i].typeCode()));
        }
        if(hasColumnNames()){
            res.setColumnNames(getColumnNames());
        }
        final int argRows = df.rows();
        final int[] hash0 = new int[next];
        final int[] hash1 = new int[argRows];
        for(int i=0; i<next; ++i){
            hash0[i] = Arrays.hashCode(getRow(i));
        }
        for(int i=0; i<argRows; ++i){
            hash1[i] = Arrays.hashCode(df.getRow(i));
        }
        for(int i=0; i<next; ++i){
            final Object[] row = getRow(i);
            boolean match = false;
            for(int j=0; j<argRows; ++j){
                if(hash0[i] == hash1[j]){
                    if(Arrays.equals(row, df.getRow(j))){
                        match = true;
                        break;
                    }
                }
            }
            if(!match){
                for(int k=0; k<i; ++k){
                    if(hash0[i] == hash0[k]){
                        if(Arrays.equals(row, getRow(k))){
                            match = true;
                        }
                    }
                }
                if(!match){
                    res.addRow(row);
                }
            }
        }
        for(int i=0; i<argRows; ++i){
            final Object[] row = df.getRow(i);
            boolean match = false;
            for(int j=0; j<next; ++j){
                if(hash1[i] == hash0[j]){
                    if(Arrays.equals(row, getRow(j))){
                        match = true;
                        break;
                    }
                }

            }
            if(!match){
                for(int k=0; k<i; ++k){
                    if(hash1[i] == hash1[k]){
                        if(Arrays.equals(row, df.getRow(k))){
                            match = true;
                        }
                    }
                }
                if(!match){
                    res.addRow(row);
                }
            }
        }
        return res;
    }

    @Override
    public DataFrame unionRows(final DataFrame df){
        ensureValidRowSetOperation(df);
        final DataFrame res = new NullableDataFrame();
        for(int i=0; i<columns.length; ++i){
            res.addColumn(Column.ofType(columns[i].typeCode()));
        }
        if(hasColumnNames()){
            res.setColumnNames(getColumnNames());
        }
        final int argRows = df.rows();
        final int[] hash0 = new int[next];
        final int[] hash1 = new int[argRows];
        for(int i=0; i<next; ++i){
            hash0[i] = Arrays.hashCode(getRow(i));
        }
        for(int i=0; i<argRows; ++i){
            hash1[i] = Arrays.hashCode(df.getRow(i));
        }
        for(int i=0; i<next; ++i){
            final Object[] row = getRow(i);
            boolean match = false;
            for(int k=0; k<i; ++k){
                if(hash0[k] == hash0[i]){
                    if(Arrays.equals(row, getRow(k))){
                        match = true;
                    }
                }
            }
            if(!match){
                res.addRow(row);
            }
        }
        for(int i=0; i<argRows; ++i){
            final Object[] row = df.getRow(i);
            boolean match = false;
            for(int j=0; j<next; ++j){
                if(hash0[j] == hash1[i]){
                    if(Arrays.equals(row, getRow(j))){
                        match = true;
                        break;
                    }
                }

            }
            if(!match){
                for(int k=0; k<i; ++k){
                    if(hash1[k] == hash1[i]){
                        if(Arrays.equals(row, df.getRow(k))){
                            match = true;
                        }
                    }
                }
                if(!match){
                    res.addRow(row);
                }
            }
        }
        return res;
    }

    @Override
    public DataFrame intersectionRows(final DataFrame df){
        ensureValidRowSetOperation(df);
        final DataFrame res = new NullableDataFrame();
        for(int i=0; i<columns.length; ++i){
            res.addColumn(Column.ofType(columns[i].typeCode()));
        }
        if(hasColumnNames()){
            res.setColumnNames(getColumnNames());
        }
        final int argRows = df.rows();
        final int[] hash0 = new int[next];
        final int[] hash1 = new int[argRows];
        for(int i=0; i<next; ++i){
            hash0[i] = Arrays.hashCode(getRow(i));
        }
        for(int i=0; i<argRows; ++i){
            hash1[i] = Arrays.hashCode(df.getRow(i));
        }
        for(int i=0; i<next; ++i){
            final Object[] row = getRow(i);
            boolean match = false;
            for(int j=0; j<argRows; ++j){
                if(hash0[i] == hash1[j]){
                    if(Arrays.equals(row, df.getRow(j))){
                        match = true;
                        break;
                    }
                }
            }
            if(match){
                //check for duplicate row already
                //in the result DataFrame
                for(int k=0; k<i; ++k){
                    if(hash0[i] == hash0[k]){
                        //hashes match. Check for equality
                        if(Arrays.equals(row, getRow(k))){
                            //duplicate row
                            match = false;
                        }
                    }
                }
                if(match){
                    res.addRow(row);
                }
            }
        }
        return res;
    }

    @Override
    public double average(final int col){
        if((next == -1) || (col < 0) || (col >= columns.length)){
            throw new DataFrameException("Invalid column index: " + col);
        }
        final Column c = columns[col];
        if(!c.isNumeric()){
            final String s = (c.name != null)
                    ? "'" + c.name + "'" : "at index " + col;

            throw new DataFrameException("Unable to compute average. "
                                       + "Column " + s + " is not numeric");
        }
        if(next == 0){
            return Double.NaN;
        }
        double avg = 0;
        int total = 0;
        switch(c.typeCode()){
        case NullableFloatColumn.TYPE_CODE:
            final NullableFloatColumn columnFloat = ((NullableFloatColumn)c);
            for(int i=0; i<next; ++i){
                if(columnFloat.get(i) != null){
                    avg += columnFloat.get(i);
                    ++total;
                }
            }
            break;
        case NullableDoubleColumn.TYPE_CODE:
            final NullableDoubleColumn columnDouble = ((NullableDoubleColumn)c);
            for(int i=0; i<next; ++i){
                if(columnDouble.get(i) != null){
                    avg += columnDouble.get(i);
                    ++total;
                }
            }
            break;
        case NullableByteColumn.TYPE_CODE:
            final NullableByteColumn columnByte = ((NullableByteColumn)c);
            for(int i=0; i<next; ++i){
                if(columnByte.get(i) != null){
                    avg += columnByte.get(i);
                    ++total;
                }
            }
            break;
        case NullableShortColumn.TYPE_CODE:
            final NullableShortColumn columnShort = ((NullableShortColumn)c);
            for(int i=0; i<next; ++i){
                if(columnShort.get(i) != null){
                    avg += columnShort.get(i);
                    ++total;
                }
            }
            break;
        case NullableIntColumn.TYPE_CODE:
            final NullableIntColumn columnInt = ((NullableIntColumn)c);
            for(int i=0; i<next; ++i){
                if(columnInt.get(i) != null){
                    avg += columnInt.get(i);
                    ++total;
                }
            }
            break;
        case NullableLongColumn.TYPE_CODE:
            final NullableLongColumn columnLong = ((NullableLongColumn)c);
            for(int i=0; i<next; ++i){
                if(columnLong.get(i) != null){
                    avg += columnLong.get(i);
                    ++total;
                }
            }
            break;
        default:
            throw new DataFrameException("Unrecognized column type");
        }
        return (total > 0) ? (avg/total) : Double.NaN;
    }

    @Override
    public double average(final String col){
        return average(enforceName(col));
    }
    
    @Override
    public double median(final int col){
        if((next == -1) || (col < 0) || (col >= columns.length)){
            throw new DataFrameException("Invalid column index: " + col);
        }
        final Column c = columns[col];
        if(!c.isNumeric()){
            final String s = (c.name != null)
                    ? "'" + c.name + "'" : "at index " + col;

            throw new DataFrameException("Unable to compute median. "
                                       + "Column " + s + " is not numeric");
        }
        if(next == 0){
            return Double.NaN;
        }
        this.flush();
        int nulls = 0;
        for(int i=0; i<next; ++i){
            if(c.getValue(i) == null){
                ++nulls;
            }
        }
        int j = 0;
        switch(c.typeCode()){
        case NullableFloatColumn.TYPE_CODE:
            final float[] f = new float[next-nulls];
            for(int i=0; i<next; ++i){
                if(c.getValue(i) != null){
                    f[j++] = (float) c.getValue(i);
                }
            }
            return new DefaultDataFrame(new FloatColumn(f)).median(0);
        case NullableDoubleColumn.TYPE_CODE:
            final double[] d = new double[next-nulls];
            for(int i=0; i<next; ++i){
                if(c.getValue(i) != null){
                    d[j++] = (double) c.getValue(i);
                }
            }
            return new DefaultDataFrame(new DoubleColumn(d)).median(0);
        case NullableByteColumn.TYPE_CODE:
            final byte[] b = new byte[next-nulls];
            for(int i=0; i<next; ++i){
                if(c.getValue(i) != null){
                    b[j++] = (byte) c.getValue(i);
                }
            }
            return new DefaultDataFrame(new ByteColumn(b)).median(0);
        case NullableShortColumn.TYPE_CODE:
            final short[] s = new short[next-nulls];
            for(int i=0; i<next; ++i){
                if(c.getValue(i) != null){
                    s[j++] = (short) c.getValue(i);
                }
            }
            return new DefaultDataFrame(new ShortColumn(s)).median(0);
        case NullableIntColumn.TYPE_CODE:
            final int[] ints = new int[next-nulls];
            for(int i=0; i<next; ++i){
                if(c.getValue(i) != null){
                    ints[j++] = (int) c.getValue(i);
                }
            }
            return new DefaultDataFrame(new IntColumn(ints)).median(0);
        case NullableLongColumn.TYPE_CODE:
            final long[] l = new long[next-nulls];
            for(int i=0; i<next; ++i){
                if(c.getValue(i) != null){
                    l[j++] = (long) c.getValue(i);
                }
            }
            return new DefaultDataFrame(new LongColumn(l)).median(0);
        default:
            throw new DataFrameException("Unrecognized column type");
        }
    }

    @Override
    public double median(final String col){
        return median(enforceName(col));
    }

    @Override
    public double minimum(final int col){
        if((next == -1) || (col < 0) || (col >= columns.length)){
            throw new DataFrameException("Invalid column index: " + col);
        }
        final Column c = columns[col];
        if(!c.isNumeric()){
            final String s = (c.name != null)
                    ? "'" + c.name + "'" : "at index " + col;

            throw new DataFrameException("Unable to compute minimum. "
                                       + "Column " + s + " is not numeric");
        }
        if(next == 0){
            return Double.NaN;
        }
        double min = 0.0;
        boolean hit = false;
        switch(c.typeCode()){
        case NullableFloatColumn.TYPE_CODE:
            float minFloat = Float.MAX_VALUE;
            final NullableFloatColumn columnFloat = ((NullableFloatColumn)c);
            for(int i=0; i<next; ++i){
                if((columnFloat.get(i) != null) && (columnFloat.get(i) < minFloat)){
                    minFloat = columnFloat.get(i);
                    hit = true;
                }
            }
            min = (double) minFloat;
            break;
        case NullableDoubleColumn.TYPE_CODE:
            double minDouble = Double.MAX_VALUE;
            final NullableDoubleColumn columnDouble = ((NullableDoubleColumn)c);
            for(int i=0; i<next; ++i){
                if((columnDouble.get(i) != null) && (columnDouble.get(i) < minDouble)){
                    minDouble = columnDouble.get(i);
                    hit = true;
                }
            }
            min = minDouble;
            break;
        case NullableByteColumn.TYPE_CODE:
            byte minByte = Byte.MAX_VALUE;
            final NullableByteColumn columnByte = ((NullableByteColumn)c);
            for(int i=0; i<next; ++i){
                if((columnByte.get(i) != null) && (columnByte.get(i) < minByte)){
                    minByte = columnByte.get(i);
                    hit = true;
                }
            }
            min = (double) minByte;
            break;
        case NullableShortColumn.TYPE_CODE:
            short minShort = Short.MAX_VALUE;
            final NullableShortColumn columnShort = ((NullableShortColumn)c);
            for(int i=0; i<next; ++i){
                if((columnShort.get(i) != null) && (columnShort.get(i) < minShort)){
                    minShort = columnShort.get(i);
                    hit = true;
                }
            }
            min = (double) minShort;
            break;
        case NullableIntColumn.TYPE_CODE:
            int minInt = Integer.MAX_VALUE;
            final NullableIntColumn columnInt = ((NullableIntColumn)c);
            for(int i=0; i<next; ++i){
                if((columnInt.get(i) != null) && (columnInt.get(i) < minInt)){
                    minInt = columnInt.get(i);
                    hit = true;
                }
            }
            min = (double) minInt;
            break;
        case NullableLongColumn.TYPE_CODE:
            long minLong = Long.MAX_VALUE;
            final NullableLongColumn columnLong = ((NullableLongColumn)c);
            for(int i=0; i<next; ++i){
                if((columnLong.get(i) != null) && (columnLong.get(i) < minLong)){
                    minLong = columnLong.get(i);
                    hit = true;
                }
            }
            min = (double) minLong;
            break;
        default:
            throw new DataFrameException("Unrecognized column type");
        }
        return hit ? min : Double.NaN;
    }

    @Override
    public double minimum(final String col){
        return minimum(enforceName(col));
    }

    @Override
    public DataFrame minimum(final int col, int rank){
        if((next == -1) || (col < 0) || (col >= columns.length)){
            throw new DataFrameException("Invalid column index: " + col);
        }
        if(rank <= 0){
            throw new DataFrameException("Invalid rank argument: " + rank);
        }
        final Column c = columns[col];
        if(!c.isNumeric()){
            final String s = (c.name != null)
                    ? "'" + c.name + "'" : "at index " + col;

            throw new DataFrameException("Unable to compute minimum. "
                                       + "Column " + s + " is not numeric");
        }
        if(rank > next){
            rank = next;
        }
        final Column[] cols = new Column[columns.length];
        for(int i=0; i<columns.length; ++i){
            cols[i] = Column.ofType(columns[i].typeCode(), rank);
        }
        final DataFrame df = new NullableDataFrame(cols);
        if(hasColumnNames()){
            df.setColumnNames(getColumnNames());
        }
        final int[] indices = new int[rank];
        for(int i=0; i<rank; ++i){
            indices[i] = -1;
        }
        switch(c.typeCode()){
        case NullableFloatColumn.TYPE_CODE:
            final NullableFloatColumn columnFloat = ((NullableFloatColumn)c);
            for(int i=0; i<rank; ++i){
                float minFloat = Float.MAX_VALUE;
                for(int j=0; j<next; ++j){
                    final Float value = columnFloat.get(j); 
                    if((value != null) && (value < minFloat)){
                        boolean taken = false;
                        for(int k=0; k<rank; ++k){
                            if(indices[k] == j){
                                taken = true;
                                break;
                            }
                        }
                        if(!taken){
                            minFloat = value;
                            indices[i] = j;
                        }
                    }
                }
            }
            break;
        case NullableDoubleColumn.TYPE_CODE:
            final NullableDoubleColumn columnDouble = ((NullableDoubleColumn)c);
            for(int i=0; i<rank; ++i){
                double minDouble = Double.MAX_VALUE;
                for(int j=0; j<next; ++j){
                    final Double value = columnDouble.get(j); 
                    if((value != null) && (value < minDouble)){
                        boolean taken = false;
                        for(int k=0; k<rank; ++k){
                            if(indices[k] == j){
                                taken = true;
                                break;
                            }
                        }
                        if(!taken){
                            minDouble = value;
                            indices[i] = j;
                        }
                    }
                }
            }
            break;
        case NullableByteColumn.TYPE_CODE:
            final NullableByteColumn columnByte = ((NullableByteColumn)c);
            for(int i=0; i<rank; ++i){
                byte minByte = Byte.MAX_VALUE;
                for(int j=0; j<next; ++j){
                    final Byte value = columnByte.get(j); 
                    if((value != null) && (value < minByte)){
                        boolean taken = false;
                        for(int k=0; k<rank; ++k){
                            if(indices[k] == j){
                                taken = true;
                                break;
                            }
                        }
                        if(!taken){
                            minByte = value;
                            indices[i] = j;
                        }
                    }
                }
            }
            break;
        case NullableShortColumn.TYPE_CODE:
            final NullableShortColumn columnShort = ((NullableShortColumn)c);
            for(int i=0; i<rank; ++i){
                short minShort = Short.MAX_VALUE;
                for(int j=0; j<next; ++j){
                    final Short value = columnShort.get(j); 
                    if((value != null) && (value < minShort)){
                        boolean taken = false;
                        for(int k=0; k<rank; ++k){
                            if(indices[k] == j){
                                taken = true;
                                break;
                            }
                        }
                        if(!taken){
                            minShort = value;
                            indices[i] = j;
                        }
                    }
                }
            }
            break;
        case NullableIntColumn.TYPE_CODE:
            final NullableIntColumn columnInt = ((NullableIntColumn)c);
            for(int i=0; i<rank; ++i){
                int minInt = Integer.MAX_VALUE;
                for(int j=0; j<next; ++j){
                    final Integer value = columnInt.get(j); 
                    if((value != null) && (value < minInt)){
                        boolean taken = false;
                        for(int k=0; k<rank; ++k){
                            if(indices[k] == j){
                                taken = true;
                                break;
                            }
                        }
                        if(!taken){
                            minInt = value;
                            indices[i] = j;
                        }
                    }
                }
            }
            break;
        case NullableLongColumn.TYPE_CODE:
            final NullableLongColumn columnLong = ((NullableLongColumn)c);
            for(int i=0; i<rank; ++i){
                long minLong = Long.MAX_VALUE;
                for(int j=0; j<next; ++j){
                    final Long value = columnLong.get(j); 
                    if((value != null) && (value < minLong)){
                        boolean taken = false;
                        for(int k=0; k<rank; ++k){
                            if(indices[k] == j){
                                taken = true;
                                break;
                            }
                        }
                        if(!taken){
                            minLong = value;
                            indices[i] = j;
                        }
                    }
                }
            }
            break;
        default:
            throw new DataFrameException("Unrecognized column type");
        }
        int removedOffset = 0;
        for(int i=0; i<rank; ++i){
            if(indices[i] != -1){
                df.setRow(i, this.getRow(indices[i]));
            }else{
                df.removeRow(i - removedOffset++);
            }
        }
        df.flush();
        return df;
    }

    @Override
    public DataFrame minimum(final String col, final int rank){
        return minimum(enforceName(col), rank);
    }

    @Override
    public double maximum(final int col){
        if((next == -1) || (col < 0) || (col >= columns.length)){
            throw new DataFrameException("Invalid column index: " + col);
        }
        final Column c = columns[col];
        if(!c.isNumeric()){
            final String s = (c.name != null)
                    ? "'" + c.name + "'" : "at index " + col;

            throw new DataFrameException("Unable to compute maximum. "
                                       + "Column " + s + " is not numeric");
        }
        if(next == 0){
            return Double.NaN;
        }
        double max = 0.0;
        boolean hit = false;
        switch(c.typeCode()){
        case NullableFloatColumn.TYPE_CODE:
            float maxFloat = -Float.MAX_VALUE;
            final NullableFloatColumn columnFloat = ((NullableFloatColumn)c);
            for(int i=0; i<next; ++i){
                if((columnFloat.get(i) != null) && (columnFloat.get(i) > maxFloat)){
                    maxFloat = columnFloat.get(i);
                    hit = true;
                }
            }
            max = (double) maxFloat;
            break;
        case NullableDoubleColumn.TYPE_CODE:
            double maxDouble = -Double.MAX_VALUE;
            final NullableDoubleColumn columnDouble = ((NullableDoubleColumn)c);
            for(int i=0; i<next; ++i){
                if((columnDouble.get(i) != null) && (columnDouble.get(i) > maxDouble)){
                    maxDouble = columnDouble.get(i);
                    hit = true;
                }
            }
            max = maxDouble;
            break;
        case NullableByteColumn.TYPE_CODE:
            byte maxByte = Byte.MIN_VALUE;
            final NullableByteColumn columnByte = ((NullableByteColumn)c);
            for(int i=0; i<next; ++i){
                if((columnByte.get(i) != null) && (columnByte.get(i) > maxByte)){
                    maxByte = columnByte.get(i);
                    hit = true;
                }
            }
            max = (double) maxByte;
            break;
        case NullableShortColumn.TYPE_CODE:
            short maxShort = Short.MIN_VALUE;
            final NullableShortColumn columnShort = ((NullableShortColumn)c);
            for(int i=0; i<next; ++i){
                if((columnShort.get(i) != null) && (columnShort.get(i) > maxShort)){
                    maxShort = columnShort.get(i);
                    hit = true;
                }
            }
            max = (double) maxShort;
            break;
        case NullableIntColumn.TYPE_CODE:
            int maxInt = Integer.MIN_VALUE;
            final NullableIntColumn columnInt = ((NullableIntColumn)c);
            for(int i=0; i<next; ++i){
                if((columnInt.get(i) != null) && (columnInt.get(i) > maxInt)){
                    maxInt = columnInt.get(i);
                    hit = true;
                }
            }
            max = (double) maxInt;
            break;
        case NullableLongColumn.TYPE_CODE:
            long maxLong = Long.MIN_VALUE;
            final NullableLongColumn columnLong = ((NullableLongColumn)c);
            for(int i=0; i<next; ++i){
                if((columnLong.get(i) != null) && (columnLong.get(i) > maxLong)){
                    maxLong = columnLong.get(i);
                    hit = true;
                }
            }
            max = (double) maxLong;
            break;
        default:
            throw new DataFrameException("Unrecognized column type");
        }
        return hit ? max : Double.NaN;
    }

    @Override
    public double maximum(final String col){
        return maximum(enforceName(col));
    }

    @Override
    public DataFrame maximum(final int col, int rank){
        if((next == -1) || (col < 0) || (col >= columns.length)){
            throw new DataFrameException("Invalid column index: " + col);
        }
        if(rank <= 0){
            throw new DataFrameException("Invalid rank argument: " + rank);
        }
        final Column c = columns[col];
        if(!c.isNumeric()){
            final String s = (c.name != null)
                    ? "'" + c.name + "'" : "at index " + col;

            throw new DataFrameException("Unable to compute maximum. "
                                       + "Column " + s + " is not numeric");
        }
        if(rank > next){
            rank = next;
        }
        final Column[] cols = new Column[columns.length];
        for(int i=0; i<columns.length; ++i){
            cols[i] = Column.ofType(columns[i].typeCode(), rank);
        }
        final DataFrame df = new NullableDataFrame(cols);
        if(hasColumnNames()){
            df.setColumnNames(getColumnNames());
        }
        final int[] indices = new int[rank];
        for(int i=0; i<rank; ++i){
            indices[i] = -1;
        }
        switch(c.typeCode()){
        case NullableFloatColumn.TYPE_CODE:
            final NullableFloatColumn columnFloat = ((NullableFloatColumn)c);
            for(int i=0; i<rank; ++i){
                float maxFloat = -Float.MAX_VALUE;
                for(int j=0; j<next; ++j){
                    final Float value = columnFloat.get(j); 
                    if((value != null) && (value > maxFloat)){
                        boolean taken = false;
                        for(int k=0; k<rank; ++k){
                            if(indices[k] == j){
                                taken = true;
                                break;
                            }
                        }
                        if(!taken){
                            maxFloat = value;
                            indices[i] = j;
                        }
                    }
                }
            }
            break;
        case NullableDoubleColumn.TYPE_CODE:
            final NullableDoubleColumn columnDouble = ((NullableDoubleColumn)c);
            for(int i=0; i<rank; ++i){
                double maxDouble = -Double.MAX_VALUE;
                for(int j=0; j<next; ++j){
                    final Double value = columnDouble.get(j); 
                    if((value != null) && (value > maxDouble)){
                        boolean taken = false;
                        for(int k=0; k<rank; ++k){
                            if(indices[k] == j){
                                taken = true;
                                break;
                            }
                        }
                        if(!taken){
                            maxDouble = value;
                            indices[i] = j;
                        }
                    }
                }
            }
            break;
        case NullableByteColumn.TYPE_CODE:
            final NullableByteColumn columnByte = ((NullableByteColumn)c);
            for(int i=0; i<rank; ++i){
                byte maxByte = Byte.MIN_VALUE;
                for(int j=0; j<next; ++j){
                    final Byte value = columnByte.get(j); 
                    if((value != null) && (value > maxByte)){
                        boolean taken = false;
                        for(int k=0; k<rank; ++k){
                            if(indices[k] == j){
                                taken = true;
                                break;
                            }
                        }
                        if(!taken){
                            maxByte = value;
                            indices[i] = j;
                        }
                    }
                }
            }
            break;
        case NullableShortColumn.TYPE_CODE:
            final NullableShortColumn columnShort = ((NullableShortColumn)c);
            for(int i=0; i<rank; ++i){
                short maxShort = Short.MIN_VALUE;
                for(int j=0; j<next; ++j){
                    final Short value = columnShort.get(j); 
                    if((value != null) && (value > maxShort)){
                        boolean taken = false;
                        for(int k=0; k<rank; ++k){
                            if(indices[k] == j){
                                taken = true;
                                break;
                            }
                        }
                        if(!taken){
                            maxShort = value;
                            indices[i] = j;
                        }
                    }
                }
            }
            break;
        case NullableIntColumn.TYPE_CODE:
            final NullableIntColumn columnInt = ((NullableIntColumn)c);
            for(int i=0; i<rank; ++i){
                int maxInt = Integer.MIN_VALUE;
                for(int j=0; j<next; ++j){
                    final Integer value = columnInt.get(j); 
                    if((value != null) && (value > maxInt)){
                        boolean taken = false;
                        for(int k=0; k<rank; ++k){
                            if(indices[k] == j){
                                taken = true;
                                break;
                            }
                        }
                        if(!taken){
                            maxInt = value;
                            indices[i] = j;
                        }
                    }
                }
            }
            break;
        case NullableLongColumn.TYPE_CODE:
            final NullableLongColumn columnLong = ((NullableLongColumn)c);
            for(int i=0; i<rank; ++i){
                long maxLong = Long.MIN_VALUE;
                for(int j=0; j<next; ++j){
                    final Long value = columnLong.get(j); 
                    if((value != null) && (value > maxLong)){
                        boolean taken = false;
                        for(int k=0; k<rank; ++k){
                            if(indices[k] == j){
                                taken = true;
                                break;
                            }
                        }
                        if(!taken){
                            maxLong = value;
                            indices[i] = j;
                        }
                    }
                }
            }
            break;
        default:
            throw new DataFrameException("Unrecognized column type");
        }
        int removedOffset = 0;
        for(int i=0; i<rank; ++i){
            if(indices[i] != -1){
                df.setRow(i, this.getRow(indices[i]));
            }else{
                df.removeRow(i - removedOffset++);
            }
        }
        df.flush();
        return df;
    }

    @Override
    public DataFrame maximum(final String col, final int rank){
        return maximum(enforceName(col), rank);
    }

    @Override
    public double sum(final int col){
        if((next == -1) || (col < 0) || (col >= columns.length)){
            throw new DataFrameException("Invalid column index: " + col);
        }
        final Column c = columns[col];
        if(!c.isNumeric()){
            final String s = (c.name != null)
                    ? "'" + c.name + "'" : "at index " + col;

            throw new DataFrameException("Unable to compute sum. "
                                       + "Column " + s + " is not numeric");
        }
        if(next == 0){
            return Double.NaN;
        }
        double sum = 0.0;
        long sumInteger = 0l;
        boolean hit = false;
        switch(c.typeCode()){
        case NullableFloatColumn.TYPE_CODE:
            for(int i=0; i<next; ++i){
                if(c.getValue(i) != null){
                    sum += (Float) c.getValue(i);
                    hit = true;
                }
            }
            return hit ? sum : Double.NaN;
        case NullableDoubleColumn.TYPE_CODE:
            for(int i=0; i<next; ++i){
                if(c.getValue(i) != null){
                    sum += (Double) c.getValue(i);
                    hit = true;
                }
            }
            return hit ? sum : Double.NaN;
        case NullableByteColumn.TYPE_CODE:
            for(int i=0; i<next; ++i){
                if(c.getValue(i) != null){
                    sumInteger += (Byte) c.getValue(i);
                    hit = true;
                }
            }
            return hit ? (double) sumInteger : Double.NaN;
        case NullableShortColumn.TYPE_CODE:
            for(int i=0; i<next; ++i){
                if(c.getValue(i) != null){
                    sumInteger += (Short) c.getValue(i);
                    hit = true;
                }
            }
            return hit ? (double) sumInteger : Double.NaN;
        case NullableIntColumn.TYPE_CODE:
            for(int i=0; i<next; ++i){
                if(c.getValue(i) != null){
                    sumInteger += (Integer) c.getValue(i);
                    hit = true;
                }
            }
            return hit ? (double) sumInteger : Double.NaN;
        case NullableLongColumn.TYPE_CODE:
            for(int i=0; i<next; ++i){
                if(c.getValue(i) != null){
                    sumInteger += (Long) c.getValue(i);
                    hit = true;
                }
            }
            return hit ? (double) sumInteger : Double.NaN;
        default:
            throw new DataFrameException("Unrecognized column type");
        }
    }

    @Override
    public double sum(final String col){
        return sum(enforceName(col));
    }

    @Override
    public DataFrame absolute(final int col){
        if((next == -1) || (col < 0) || (col >= columns.length)){
            throw new DataFrameException("Invalid column index: " + col);
        }
        final Column c = columns[col];
        if(!c.isNumeric()){
            final String s = (c.name != null)
                    ? "'" + c.name + "'" : "at index " + col;

            throw new DataFrameException("Unable to compute absolutes. "
                                       + "Column " + s + " is not numeric");
        }
        switch(c.typeCode()){
        case NullableFloatColumn.TYPE_CODE:
            final NullableFloatColumn cFloat = ((NullableFloatColumn)c);
            for(int i=0; i<next; ++i){
                if(cFloat.get(i) != null){
                    cFloat.set(i, Math.abs(cFloat.get(i)));
                }
            }
            break;
        case NullableDoubleColumn.TYPE_CODE:
            final NullableDoubleColumn cDouble = ((NullableDoubleColumn)c);
            for(int i=0; i<next; ++i){
                if(cDouble.get(i) != null){
                    cDouble.set(i, Math.abs(cDouble.get(i)));
                }
            }
            break;
        case NullableByteColumn.TYPE_CODE:
            final NullableByteColumn cByte = ((NullableByteColumn)c);
            for(int i=0; i<next; ++i){
                if(cByte.get(i) != null){
                    cByte.set(i, (byte)((cByte.get(i) < 0)
                            ? -cByte.get(i)
                            : cByte.get(i)));
                }
            }
            break;
        case NullableShortColumn.TYPE_CODE:
            final NullableShortColumn cShort = ((NullableShortColumn)c);
            for(int i=0; i<next; ++i){
                if(cShort.get(i) != null){
                    cShort.set(i, (short)((cShort.get(i) < 0)
                            ? -cShort.get(i)
                            : cShort.get(i)));
                }
            }
            break;
        case NullableIntColumn.TYPE_CODE:
            final NullableIntColumn cInt = ((NullableIntColumn)c);
            for(int i=0; i<next; ++i){
                if(cInt.get(i) != null){
                    cInt.set(i, Math.abs(cInt.get(i)));
                }
            }
            break;
        case NullableLongColumn.TYPE_CODE:
            final NullableLongColumn cLong = ((NullableLongColumn)c);
            for(int i=0; i<next; ++i){
                if(cLong.get(i) != null){
                    cLong.set(i, Math.abs(cLong.get(i)));
                }
            }
            break;
        default:
            throw new DataFrameException("Unrecognized column type");
        }
        return this;
    }

    @Override
    public DataFrame absolute(final String col){
        return absolute(enforceName(col));
    }

    @Override
    public DataFrame ceil(final int col){
        if((next == -1) || (col < 0) || (col >= columns.length)){
            throw new DataFrameException("Invalid column index: " + col);
        }
        final Column c = columns[col];
        if(!c.isNumeric()){
            final String s = (c.name != null)
                    ? "'" + c.name + "'" : "at index " + col;

            throw new DataFrameException("Unable to compute ceil values. "
                                       + "Column " + s + " is not numeric");
        }
        switch(c.typeCode()){
        case NullableFloatColumn.TYPE_CODE:
            final NullableFloatColumn cFloat = ((NullableFloatColumn)c);
            for(int i=0; i<next; ++i){
                if(cFloat.get(i) != null){
                    cFloat.set(i, (float)Math.ceil(cFloat.get(i)));
                }
            }
            break;
        case NullableDoubleColumn.TYPE_CODE:
            final NullableDoubleColumn cDouble = ((NullableDoubleColumn)c);
            for(int i=0; i<next; ++i){
                if(cDouble.get(i) != null){
                    cDouble.set(i, Math.ceil(cDouble.get(i)));
                }
            }
            break;
        case NullableByteColumn.TYPE_CODE:
            return this;
        case NullableShortColumn.TYPE_CODE:
            return this;
        case NullableIntColumn.TYPE_CODE:
            return this;
        case NullableLongColumn.TYPE_CODE:
            return this;
        default:
            throw new DataFrameException("Unrecognized column type");
        }
        return this;
    }

    @Override
    public DataFrame ceil(final String col){
        return ceil(enforceName(col));
    }

    @Override
    public DataFrame floor(final int col){
        if((next == -1) || (col < 0) || (col >= columns.length)){
            throw new DataFrameException("Invalid column index: " + col);
        }
        final Column c = columns[col];
        if(!c.isNumeric()){
            final String s = (c.name != null)
                    ? "'" + c.name + "'" : "at index " + col;

            throw new DataFrameException("Unable to compute floor values. "
                                       + "Column " + s + " is not numeric");
        }
        switch(c.typeCode()){
        case NullableFloatColumn.TYPE_CODE:
            final NullableFloatColumn cFloat = ((NullableFloatColumn)c);
            for(int i=0; i<next; ++i){
                if(cFloat.get(i) != null){
                    cFloat.set(i, (float)Math.floor(cFloat.get(i)));
                }
            }
            break;
        case NullableDoubleColumn.TYPE_CODE:
            final NullableDoubleColumn cDouble = ((NullableDoubleColumn)c);
            for(int i=0; i<next; ++i){
                if(cDouble.get(i) != null){
                    cDouble.set(i, Math.floor(cDouble.get(i)));
                }
            }
            break;
        case NullableByteColumn.TYPE_CODE:
            return this;
        case NullableShortColumn.TYPE_CODE:
            return this;
        case NullableIntColumn.TYPE_CODE:
            return this;
        case NullableLongColumn.TYPE_CODE:
            return this;
        default:
            throw new DataFrameException("Unrecognized column type");
        }
        return this;
    }

    @Override
    public DataFrame floor(final String col){
        return floor(enforceName(col));
    }

    @Override
    public DataFrame round(final int col, final int decPlaces){
        if((next == -1) || (col < 0) || (col >= columns.length)){
            throw new DataFrameException("Invalid column index: " + col);
        }
        if(decPlaces < 0){
            throw new DataFrameException("Invalid argument for decimal places: "
                                         + decPlaces);
        }
        final Column c = columns[col];
        if(!c.isNumeric()){
            final String s = (c.name != null)
                    ? "'" + c.name + "'" : "at index " + col;

            throw new DataFrameException("Unable to round values. "
                                       + "Column " + s + " is not numeric");
        }
        double op = 1.0;
        if(decPlaces > 0){
            for(int i=0; i<decPlaces; ++i){
                op *= 10;
            }
        }
        switch(c.typeCode()){
        case NullableFloatColumn.TYPE_CODE:
            final NullableFloatColumn cFloat = ((NullableFloatColumn)c);
            for(int i=0; i<next; ++i){
                if(cFloat.get(i) != null){
                    cFloat.set(i, (float)(Math.round(cFloat.get(i) * op) / op));
                }
            }
            break;
        case NullableDoubleColumn.TYPE_CODE:
            final NullableDoubleColumn cDouble = ((NullableDoubleColumn)c);
            for(int i=0; i<next; ++i){
                if(cDouble.get(i) != null){
                    cDouble.set(i, Math.round(cDouble.get(i) * op) / op);
                }
            }
            break;
        case NullableByteColumn.TYPE_CODE:
            return this;
        case NullableShortColumn.TYPE_CODE:
            return this;
        case NullableIntColumn.TYPE_CODE:
            return this;
        case NullableLongColumn.TYPE_CODE:
            return this;
        default:
            throw new DataFrameException("Unrecognized column type");
        }
        return this;
    }

    @Override
    public DataFrame round(final String col, final int decPlaces){
        return round(enforceName(col), decPlaces);
    }

    @Override
    public DataFrame clip(final int col, final Number low, final Number high){
        if((next == -1) || (col < 0) || (col >= columns.length)){
            throw new DataFrameException("Invalid column index: " + col);
        }
        final Column c = columns[col];
        if(!c.isNumeric()){
            final String s = (c.name != null)
                    ? "'" + c.name + "'" : "at index " + col;

            throw new DataFrameException("Unable to clip values. "
                                       + "Column " + s + " is not numeric");
        }
        switch(c.typeCode()){
        case NullableFloatColumn.TYPE_CODE:
            final float lowF = (low != null) ? low.floatValue() : -Float.MAX_VALUE;
            final float highF = (high != null) ? high.floatValue() : Float.MAX_VALUE;
            if(lowF >= highF){ throw new DataFrameException("Invalid threshold range"); }
            final NullableFloatColumn cFloat = ((NullableFloatColumn)c);
            for(int i=0; i<next; ++i){
                Float val = cFloat.get(i);
                if(val != null){
                    if(val < lowF){ val = lowF; }
                    if(val > highF){ val = highF; }
                    cFloat.set(i, val);
                }
            }
            break;
        case NullableDoubleColumn.TYPE_CODE:
            final double lowD = (low != null) ? low.doubleValue() : -Double.MAX_VALUE;
            final double highD = (high != null) ? high.doubleValue() : Double.MAX_VALUE;
            if(lowD >= highD){ throw new DataFrameException("Invalid threshold range"); }
            final NullableDoubleColumn cDouble = ((NullableDoubleColumn)c);
            for(int i=0; i<next; ++i){
                Double val = cDouble.get(i);
                if(val != null){
                    if(val < lowD){ val = lowD; }
                    if(val > highD){ val = highD; }
                    cDouble.set(i, val);
                }
            }
            break;
        case NullableByteColumn.TYPE_CODE:
            final byte lowB = (low != null) ? low.byteValue() : Byte.MIN_VALUE;
            final byte highB = (high != null) ? high.byteValue() : Byte.MAX_VALUE;
            if(lowB >= highB){ throw new DataFrameException("Invalid threshold range"); }
            final NullableByteColumn cByte = ((NullableByteColumn)c);
            for(int i=0; i<next; ++i){
                Byte val = cByte.get(i);
                if(val != null){
                    if(val < lowB){ val = lowB; }
                    if(val > highB){ val = highB; }
                    cByte.set(i, val);
                }
            }
            break;
        case NullableShortColumn.TYPE_CODE:
            final short lowS = (low != null) ? low.shortValue() : Short.MIN_VALUE;
            final short highS = (high != null) ? high.shortValue() : Short.MAX_VALUE;
            if(lowS >= highS){ throw new DataFrameException("Invalid threshold range"); }
            final NullableShortColumn cShort = ((NullableShortColumn)c);
            for(int i=0; i<next; ++i){
                Short val = cShort.get(i);
                if(val != null){
                    if(val < lowS){ val = lowS; }
                    if(val > highS){ val = highS; }
                    cShort.set(i, val);
                }
            }
            break;
        case NullableIntColumn.TYPE_CODE:
            final int lowI = (low != null) ? low.intValue() : Integer.MIN_VALUE;
            final int highI = (high != null) ? high.intValue() : Integer.MAX_VALUE;
            if(lowI >= highI){ throw new DataFrameException("Invalid threshold range"); }
            final NullableIntColumn cInt = ((NullableIntColumn)c);
            for(int i=0; i<next; ++i){
                Integer val = cInt.get(i);
                if(val != null){
                    if(val < lowI){ val = lowI; }
                    if(val > highI){ val = highI; }
                    cInt.set(i, val);
                }
            }
            break;
        case NullableLongColumn.TYPE_CODE:
            final long lowL = (low != null) ? low.longValue() : Long.MIN_VALUE;
            final long highL = (high != null) ? high.longValue() : Long.MAX_VALUE;
            if(lowL >= highL){ throw new DataFrameException("Invalid threshold range"); }
            final NullableLongColumn cLong = ((NullableLongColumn)c);
            for(int i=0; i<next; ++i){
                Long val = cLong.get(i);
                if(val != null){
                    if(val < lowL){ val = lowL; }
                    if(val > highL){ val = highL; }
                    cLong.set(i, val);
                }
            }
            break;
        default:
            throw new DataFrameException("Unrecognized column type");
        }
        return this;
    }

    @Override
    public DataFrame clip(final String col, final Number low, final Number high){
        return clip(enforceName(col), low, high);
    }

    @Override
    public DataFrame sortBy(final int col){
        return sortAscendingBy(col);
    }

    @Override
    public DataFrame sortBy(final String col){
        return sortAscendingBy(col);
    }

    @Override
    public DataFrame sortAscendingBy(final int col){
        if((next == -1) || (col < 0) || (col >= columns.length)){
            throw new DataFrameException("Invalid column index: " + col);
        }
        NullableDataFrame.QuickSort.sort(columns[col], columns, next, true);
        return this;
    }

    @Override
    public DataFrame sortAscendingBy(final String col){
        final int c = enforceName(col);
        NullableDataFrame.QuickSort.sort(columns[c], columns, next, true);
        return this;
    }

    @Override
    public DataFrame sortDescendingBy(final int col){
        if((next == -1) || (col < 0) || (col >= columns.length)){
            throw new DataFrameException("Invalid column index: " + col);
        }
        NullableDataFrame.QuickSort.sort(columns[col], columns, next, false);
        return this;
    }

    @Override
    public DataFrame sortDescendingBy(final String col){
        final int c = enforceName(col);
        NullableDataFrame.QuickSort.sort(columns[c], columns, next, false);
        return this;
    }

    @Override
    public DataFrame head(){
        return head(5);
    }

    @Override
    public DataFrame head(int rows){
        if(rows < 0){
            throw new DataFrameException("Invalid row argument: " + rows);
        }
        if(next == -1){
            return new NullableDataFrame();
        }
        if(rows > next){
            rows = next;
        }
        final Column[] cols = new Column[columns.length];
        for(int i=0; i<columns.length; ++i){
            cols[i] = Column.ofType(columns[i].typeCode(),
                    (rows >= 0) ? rows : 0);

        }
        final DataFrame df = new NullableDataFrame(cols);
        if(hasColumnNames()){
            df.setColumnNames(getColumnNames());
        }
        for(int i=0; i<rows; ++i){
            df.setRow(i, this.getRow(i));
        }
        return df;
    }

    @Override
    public DataFrame tail(){
        return tail(5);
    }

    @Override
    public DataFrame tail(int rows){
        if(rows < 0){
            throw new DataFrameException("Invalid row argument: " + rows);
        }
        if(next == -1){
            return new NullableDataFrame();
        }
        if(rows > next){
            rows = next;
        }
        final Column[] cols = new Column[columns.length];
        for(int i=0; i<columns.length; ++i){
            cols[i] = Column.ofType(columns[i].typeCode(),
                    (rows >= 0) ? rows : 0);

        }
        final DataFrame df = new NullableDataFrame(cols);
        if(hasColumnNames()){
            df.setColumnNames(getColumnNames());
        }
        if(rows >= 0){
            final int offset = next - rows;
            for(int i=0; i<rows; ++i){
                df.setRow(i, this.getRow(offset + i));
            }
        }
        return df;
    }

    @Override
    public boolean equals(final Object obj){
        if(obj == null){
            return false;
        }
        if(!(obj instanceof NullableDataFrame)){
            return false;
        }
        final DataFrame df = (NullableDataFrame) obj;
        if((this.rows() != df.rows()) || (this.columns() != df.columns())){
            return false;
        }
        final String[] n1 = this.getColumnNames();
        final String[] n2 = df.getColumnNames();
        if((n1 == null) ^ (n2 == null)){
            return false;
        }
        for(int i=0; i<df.columns(); ++i){
            if((n1 != null) && (n2 != null)){
                //compare column names
                if(!n1[i].equals(n2[i])){
                    return false;
                }
            }
            //compare column types
            if(this.getColumn(i).typeCode() != df.getColumn(i).typeCode()){
                return false;
            }
        }
        //ensure both DataFrames have the same capacity
        this.flush();
        df.flush();
        //compare data
        int idx = 0;
        for(int i=0; i<df.columns(); ++i){
            final Column col2 = df.getColumn(i);
            final Column col1 = this.getColumn(idx++);
            if(!col2.equals(col1)){
                return false;
            }
        }
        return true;
    }

    @Override
    protected DataFrame createInstance(){
        return new NullableDataFrame();
    }

    @Override
    protected void enforceTypes(final Object[] row){
        if((next == -1) || (row.length != columns.length)){
            throw new DataFrameException(
                    "Row length does not match number of columns: "
                    + row.length);
        }
        for(int i=0; i<columns.length; ++i){
            if(row[i] != null){
                if(!(columns[i].memberClass().equals(row[i].getClass()))){
                    final String colname = columns[i].name;
                    final String colmsg = ((colname != null) && !colname.isEmpty())
                            ? "'" + colname + "'"
                            : "index " + i;

                    throw new DataFrameException(String.format(
                            "Type missmatch at column %s. Expected %s but found %s",
                            colmsg, columns[i].memberClass().getSimpleName(),
                            row[i].getClass().getSimpleName()));

                }
            }
        }
    }

    @Override
    protected Object[] itemsByAnnotations(final Row row){
        final Object[] items = new Object[columns.length];
        for(final Field field : row.getClass().getDeclaredFields()){
            final RowItem item = field.getAnnotation(RowItem.class);
            if(item != null){
                String name = item.value();
                if((name == null || name.isEmpty())){
                    name = field.getName();
                }
                final int i = enforceName(name);
                field.setAccessible(true);
                Object value = null;
                try{
                    value = field.get(row);
                }catch(IllegalArgumentException | IllegalAccessException ex){
                    throw new DataFrameException(ex.getMessage());
                }
                if((value != null)
                        && !value.getClass().equals(columns[i].memberClass())){

                    throw new DataFrameException(
                            String.format("Row item '%s' has an incorrect type. " 
                                    + "Expected %s but found %s", name,
                                    columns[i].memberClass().getSimpleName(),
                                    value.getClass().getSimpleName()));

                }
                items[i] = value;
            }
        }
        return items;
    }

    @Override
    protected <T> int replace0(final int col, String regex,
            final IndexedValueReplacement<T> value){

        if((next == -1) || (col < 0) || (col >= columns.length)){
            throw new DataFrameException("Invalid column index: " + col);
        }
        if(value == null){
            return 0;//NO-OP
        }
        if((regex == null) || regex.isEmpty()){
            regex = "null";
        }
        final Column c = columns[col];
        final Pattern p = Pattern.compile(regex);//cache
        final Class<?> colClass = c.memberClass();
        int replaced = 0;
        for(int i=0; i<next; ++i){
            final T currentValue = c.getGenericValue(i);
            if(!p.matcher(String.valueOf(currentValue)).matches()){
                continue;
            }
            Object replacement = null;
            try{
                replacement = value.replace(i, currentValue);
            }catch(Exception ex){
                throw new DataFrameException(
                        String.format("Value replacement function has thrown %s",
                                       ex.getClass().getName()), ex);
            }
            if(replacement == currentValue){
                continue;
            }
            if((replacement != null) && !colClass.equals(replacement.getClass())){
                final String msg = (c.name != null)
                                        ? "for column '" + c.name + "'"
                                        : "at column index " + col;

                throw new DataFrameException(
                        String.format(
                            "Invalid replacement type %s. Expected %s but found %s",
                            msg, c.getType(), replacement.getClass().getSimpleName()));

            }
            c.setValue(i, replacement);
            ++replaced;
        }
        return replaced;
    }

    @Override
    protected DataFrame groupOperation(final int col, final int operation){
        if((next == -1) || (col < 0) || (col >= columns.length)){
            throw new DataFrameException("Invalid column index: " + col);
        }
        final Column c = columns[col];
        int nNumeric = 0;
        for(int i=0; i<columns.length; ++i){
            if((columns[i].name == null) || columns[i].name.isEmpty()){
                throw new DataFrameException(
                        "All columns must be labeled for group operations");
            }
            if((columns[i] != c) && columns[i].isNumeric()){
                ++nNumeric;
            }
        }
        final Set<Object> uniques = this.unique(col);
        final int nUniques = uniques.size();
        final boolean containsNull = this.contains(col, "null");
        final int colLength = containsNull ? nUniques + 1 : nUniques;
        final Column[] cols = new Column[nNumeric + 1];
        final String[] colNames = new String[nNumeric + 1];
        cols[0] = Column.ofType(c.typeCode(), colLength);
        colNames[0] = c.name;
        nNumeric = 1;
        for(int i=0; i<columns.length; ++i){
            if((columns[i] != c) && columns[i].isNumeric()){
                if((operation == 3) || (operation == 4)){//average or sum op
                    cols[nNumeric] = new NullableDoubleColumn(colLength);
                }else{
                    cols[nNumeric] = Column.ofType(columns[i].typeCode(), colLength);
                }
                colNames[nNumeric] = columns[i].name;
                ++nNumeric;
            }
        }
        final DataFrame df = new NullableDataFrame(colNames, cols);
        final int length = cols.length;
        int index = 0;
        for(final Object elem : uniques){
            final Object[] row = new Object[length];
            row[0] = elem;
            final DataFrame f = this.filter(c.name, elem.toString());
            for(int i=1; i<length; ++i){
                double value = 0.0;
                switch(operation){
                case 1:
                    value = f.minimum(colNames[i]);
                    break;
                case 2:
                    value = f.maximum(colNames[i]);
                    break;
                case 3:
                    value = f.average(colNames[i]);
                    break;
                case 4:
                    value = f.sum(colNames[i]);
                    break;
                default:
                    throw new DataFrameException("Unknown group operation: "
                                                 + operation);
                }
                row[i] = castToNumericType(cols[i], value);
            }
            df.setRow(index++, row);
        }
        if(containsNull){
            final Object[] row = new Object[length];
            row[0] = null;
            final DataFrame f = this.filter(c.name, "null");
            for(int i=1; i<length; ++i){
                double value = 0.0;
                switch(operation){
                case 1:
                    value = f.minimum(colNames[i]);
                    break;
                case 2:
                    value = f.maximum(colNames[i]);
                    break;
                case 3:
                    value = f.average(colNames[i]);
                    break;
                case 4:
                    value = f.sum(colNames[i]);
                    break;
                default:
                    throw new DataFrameException("Unknown group operation: "
                                                 + operation);
                }
                row[i] = castToNumericType(cols[i], value);
            }
            df.setRow(index++, row);
        }
        return df;
    }

    /**
     * Initialization method for assigning this DataFrame
     * the specified columns and their names
     * 
     * @param columns The columns to use in this DataFrame instance
     */
    private void assignColumns(final Column[] columns){
        if((columns == null) || (columns.length == 0)){
            throw new DataFrameException("Argument must not be null or empty");
        }
        int colSize = columns[0].capacity();
        for(int i=1; i<columns.length; ++i){
            if(columns[i].capacity() != colSize){
                throw new DataFrameException("Columns have deviating sizes");
            }
        }
        this.columns = new Column[columns.length];
        for(int i=0; i<columns.length; ++i){
            Column col = columns[i];
            if(!col.isNullable()){
                col = col.asNullable();
            }
            this.columns[i] = col;
            if((col.name != null) && !(col.name.isEmpty())){
                if(names == null){
                    this.names = new HashMap<String, Integer>(16);
                }
                this.names.put(col.name, i);
            }
        }
        this.next = colSize;
    }

    /**
     * Returns an instance of the correct <code>Column</code> for the specified 
     * class type
     * 
     * @param classOfField The class of the field to get a column for
     * @return A <code>Column</code> that matches the type of the provided class
     */
    private Column inferColumnFromType(final Class<?> classOfField){
        switch(classOfField.getSimpleName()){
        case "String":
            return new NullableStringColumn();
        case "Byte":
            return new NullableByteColumn();
        case "Short":
            return new NullableShortColumn();
        case "Integer":
            return new NullableIntColumn();
        case "Long":
            return new NullableLongColumn();
        case "Float":
            return new NullableFloatColumn();
        case "Double":
            return new NullableDoubleColumn();
        case "Character":
            return new NullableCharColumn();
        case "Boolean":
            return new NullableBooleanColumn();
        case "byte[]":
            return new NullableBinaryColumn();
        default:
            throw new DataFrameException(
                    classOfField.isPrimitive()
                    ? "NullableDataFrame does not support "
                      + "primitive types for row items: " 
                      + classOfField.getSimpleName() 
                    : "Unsupported type for row item: " 
                      + classOfField.getSimpleName());

        }
    }

    /**
     * Casts the specified double to the corresponding Number type of
     * the specified Column
     * 
     * @param col The <code>Column</code> which specifies the numeric type
     * @param value The double value to cast
     * @return A <code>Number</code> which has the concrete type used
     *         by the specified Column
     */
    private Number castToNumericType(final Column col, final double value){
        switch(col.typeCode()){
        case NullableDoubleColumn.TYPE_CODE:
            return value;
        case NullableFloatColumn.TYPE_CODE:
            return (float) value;
        case NullableByteColumn.TYPE_CODE:
            return (byte) value;
        case NullableShortColumn.TYPE_CODE:
            return (short) value;
        case NullableIntColumn.TYPE_CODE:
            return (int) value;
        case NullableLongColumn.TYPE_CODE:
            return (long) value;
        default:
            throw new DataFrameException("Unrecognized column type");
        }
    }

    /**
     * Internal Quicksort implementation for sorting
     * NullableDataFrame instances. Presorts the column by putting all null
     * values at the end and then only sorting the remaining part.
     *
     */
    private static class QuickSort {

        private static void sort(final Column col, final Column[] cols,
                final int next, final boolean ascend){

            switch(col.typeCode()){
            case NullableByteColumn.TYPE_CODE:
                sort(((NullableByteColumn)col).asArray(), cols, 0, 
                        presort(((NullableByteColumn)col).asArray(), cols, next), ascend);
                break;
            case NullableShortColumn.TYPE_CODE:
                sort(((NullableShortColumn)col).asArray(), cols, 0,
                        presort(((NullableShortColumn)col).asArray(), cols, next), ascend);
                break;
            case NullableIntColumn.TYPE_CODE:
                sort(((NullableIntColumn)col).asArray(), cols, 0, 
                        presort(((NullableIntColumn)col).asArray(), cols, next), ascend);
                break;
            case NullableLongColumn.TYPE_CODE:
                sort(((NullableLongColumn)col).asArray(), cols, 0, 
                        presort(((NullableLongColumn)col).asArray(), cols, next), ascend);
                break;
            case NullableStringColumn.TYPE_CODE:
                sort(((NullableStringColumn)col).asArray(), cols, 0, 
                        presort(((NullableStringColumn)col).asArray(), cols, next), ascend);
                break;
            case NullableFloatColumn.TYPE_CODE:
                sort(((NullableFloatColumn)col).asArray(), cols, 0, 
                        presort(((NullableFloatColumn)col).asArray(), cols, next), ascend);
                break;
            case NullableDoubleColumn.TYPE_CODE:
                sort(((NullableDoubleColumn)col).asArray(), cols, 0, 
                        presort(((NullableDoubleColumn)col).asArray(), cols, next), ascend);
                break;
            case NullableCharColumn.TYPE_CODE:
                sort(((NullableCharColumn)col).asArray(), cols, 0, 
                        presort(((NullableCharColumn)col).asArray(), cols, next), ascend);
                break;
            case NullableBooleanColumn.TYPE_CODE:
                sort(((NullableBooleanColumn)col).asArray(), cols, 0, 
                        presort(((NullableBooleanColumn)col).asArray(), cols, next), ascend);
                break;
            case NullableBinaryColumn.TYPE_CODE:
                sort(((NullableBinaryColumn)col).asArray(), cols, 0, 
                        presort(((NullableBinaryColumn)col).asArray(), cols, next), ascend);
                break;
            default:
                //undefined
                throw new DataFrameException("Unrecognized column type: "
                        + col.getClass().getName());
                
            }
        }	

        private static void sort(final Byte[] list, final Column[] cols,
                final int left, final int right, final boolean ascend){

            if(right <= -1){
                return;
            }
            final byte MID = list[(left+right)/2];
            int l = left;
            int r = right;
            while(l < r){
                if(ascend){
                    while(list[l] < MID){ ++l; }
                    while(list[r] > MID){ --r; }
                }else{
                    while(list[l] > MID){ ++l; }
                    while(list[r] < MID){ --r; }
                }
                if(l <= r){
                    swap(cols, l++, r--);
                }
            }
            if(left < r){
                sort(list, cols, left, r, ascend);
            }
            if(right > l){
                sort(list, cols, l, right, ascend);
            }
        }

        private static void sort(final Short[] list, final Column[] cols,
                final int left, final int right, final boolean ascend){

            if(right <= -1){
                return;
            }
            final short MID = list[(left+right)/2];
            int l = left;
            int r = right;
            while(l < r){
                if(ascend){
                    while(list[l] < MID){ ++l; }
                    while(list[r] > MID){ --r; }
                }else{
                    while(list[l] > MID){ ++l; }
                    while(list[r] < MID){ --r; }
                }
                if(l <= r){
                    swap(cols, l++, r--);
                }
            }
            if(left < r){
                sort(list, cols, left, r, ascend);
            }
            if(right > l){
                sort(list, cols, l, right, ascend);
            }
        }

        private static void sort(final Integer[] list, final Column[] cols,
                final int left, final int right, final boolean ascend){

            if(right <= -1){
                return;
            }
            final int MID = list[(left+right)/2];
            int l = left;
            int r = right;
            while(l < r){
                if(ascend){
                    while(list[l] < MID){ ++l; }
                    while(list[r] > MID){ --r; }
                }else{
                    while(list[l] > MID){ ++l; }
                    while(list[r] < MID){ --r; }
                }
                if(l <= r){
                    swap(cols, l++, r--);
                }
            }
            if(left < r){
                sort(list, cols, left, r, ascend);
            }
            if(right > l){
                sort(list, cols, l, right, ascend);
            }
        }

        private static void sort(final Long[] list, final Column[] cols,
                final int left, final int right, final boolean ascend){

            if(right <= -1){
                return;
            }
            final long MID = list[(left+right)/2];
            int l = left;
            int r = right;
            while(l < r){
                if(ascend){
                    while(list[l] < MID){ ++l; }
                    while(list[r] > MID){ --r; }
                }else{
                    while(list[l] > MID){ ++l; }
                    while(list[r] < MID){ --r; } 
                }
                if(l <= r){
                    swap(cols, l++, r--);
                }
            }
            if(left < r){
                sort(list, cols, left, r, ascend);
            }
            if(right > l){
                sort(list, cols, l, right, ascend);
            }
        }

        private static void sort(final String[] list, final Column[] cols,
                final int left, final int right, final boolean ascend){

            if(right <= -1){
                return;
            }
            final String MID = list[(left+right)/2];
            int l = left;
            int r = right;
            while(l < r){
                if(ascend){
                    while(list[l].compareTo(MID) < 0){ ++l; }
                    while(list[r].compareTo(MID) > 0){ --r; }
                }else{
                    while(list[l].compareTo(MID) > 0){ ++l; }
                    while(list[r].compareTo(MID) < 0){ --r; }
                }
                if(l <= r){
                    swap(cols, l++, r--);
                }
            }
            if(left < r){
                sort(list, cols, left, r, ascend);
            }
            if(right > l){
                sort(list, cols, l, right, ascend);
            }
        }

        private static void sort(final Float[] list, final Column[] cols,
                final int left, final int right, final boolean ascend){

            if(right <= -1){
                return;
            }
            final float MID = list[(left+right)/2];
            int l = left;
            int r = right;
            while(l < r){
                if(ascend){
                    while(list[l] < MID){ ++l; }
                    while(list[r] > MID){ --r; }
                }else{
                    while(list[l] > MID){ ++l; }
                    while(list[r] < MID){ --r; }
                }
                if(l <= r){
                    swap(cols, l++, r--);
                }
            }
            if(left < r){
                sort(list, cols, left, r, ascend);
            }
            if(right > l){
                sort(list, cols, l, right, ascend);
            }
        }

        private static void sort(final Double[] list, final Column[] cols,
                final int left, final int right, final boolean ascend){

            if(right <= -1){
                return;
            }
            final double MID = list[(left+right)/2];
            int l = left;
            int r = right;
            while(l < r){
                if(ascend){
                    while(list[l] < MID){ ++l; }
                    while(list[r] > MID){ --r; }
                }else{
                    while(list[l] > MID){ ++l; }
                    while(list[r] < MID){ --r; }
                }
                if(l <= r){
                    swap(cols, l++, r--);
                }
            }
            if(left < r){
                sort(list, cols, left, r, ascend);
            }
            if(right > l){
                sort(list, cols, l, right, ascend);
            }
        }

        private static void sort(final Character[] list, final Column[] cols,
                final int left, final int right, final boolean ascend){

            if(right <= -1){
                return;
            }
            final char MID = list[(left+right)/2];
            int l = left;
            int r = right;
            while(l < r){
                if(ascend){
                    while(list[l] < MID){ ++l; }
                    while(list[r] > MID){ --r; }
                }else{
                    while(list[l] > MID){ ++l; }
                    while(list[r] < MID){ --r; }
                }
                if(l <= r){
                    swap(cols, l++, r--);
                }
            }
            if(left < r){
                sort(list, cols, left, r, ascend);
            }
            if(right > l){
                sort(list, cols, l, right, ascend);
            }
        }

        private static void sort(final Boolean[] list, final Column[] cols,
                final int left, final int right, final boolean ascend){

            if(right <= -1){
                return;
            }
            final Boolean MID = list[(left+right)/2];
            int l = left;
            int r = right;
            while(l < r){
                if(ascend){
                    while(Boolean.valueOf(list[l]).compareTo(MID) < 0){ ++l; }
                    while(Boolean.valueOf(list[r]).compareTo(MID) > 0){ --r; }
                }else{
                    while(Boolean.valueOf(list[l]).compareTo(MID) > 0){ ++l; }
                    while(Boolean.valueOf(list[r]).compareTo(MID) < 0){ --r; }
                }
                if(l <= r){
                    swap(cols, l++, r--);
                }
            }
            if(left < r){
                sort(list, cols, left, r, ascend);
            }
            if(right > l){
                sort(list, cols, l, right, ascend);
            }
        }

        private static void sort(final byte[][] list, final Column[] cols,
                final int left, final int right, final boolean ascend){

            if(right <= -1){
                return;
            }
            final int MID = list[(left+right)/2].length;
            int l = left;
            int r = right;
            while(l < r){
                if(ascend){
                    while(list[l].length < MID){ ++l; }
                    while(list[r].length > MID){ --r; }
                }else{
                    while(list[l].length > MID){ ++l; }
                    while(list[r].length < MID){ --r; }
                }
                if(l <= r){
                    swap(cols, l++, r--);
                }
            }
            if(left < r){
                sort(list, cols, left, r, ascend);
            }
            if(right > l){
                sort(list, cols, l, right, ascend);
            }
        }

        private static void swap(final Column[] cols, final int i, final int j){
            for(int k=0; k<cols.length; ++k){
                final Column c = cols[k];
                final Object cache = c.getValue(i);
                c.setValue(i, c.getValue(j));
                c.setValue(j, cache);
            }
        }

        private static int presort(final Object[] list, final Column[] cols,
                final int next){

            int ptr = next-1;
            for(int i=0; i<ptr; ++i){
                while(list[i] == null){
                    if(i == ptr){
                        break;
                    }
                    swap(cols, i, ptr--);
                }
            }
            return (list[ptr] != null ? ptr : ptr-1);
        }
    }
}
