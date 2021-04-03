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

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Abstract base class for provided implementations of the {@link DataFrame} interface.
 * 
 * @author Phil Gaiser
 * @see DataFrame
 * @see DefaultDataFrame
 * @see NullableDataFrame
 *
 */
public abstract class AbstractDataFrame implements DataFrame {

    protected Column[] columns;
    protected Map<String, Integer> names;
    protected int next;

    /**
     * Creates a new instance of the concrete DataFrame implementation
     * 
     * @return A new empty <code>DataFrame</code> instance
     */
    protected abstract DataFrame createInstance();

    /**
     * Enforces that all entries in the given row adhere to the
     * column types in the concrete DataFrame
     * 
     * @param row The row to check against type missmatches
     */
    protected abstract void enforceTypes(Object[] row);

    /**
     * Collects all annotated items from a <code>Row</code> object and
     * returns them in an array at the correct index for further processing
     * 
     * @param row The row to get the items from
     * @return An array holding the row items of the specified Row object
     */
    protected abstract Object[] itemsByAnnotations(Row row);

    /**
     * Replace implementation. Replaces all values in the column at the
     * specified index with the value returned by the specified
     * <code>IndexedValueReplacement</code> functional interface if
     * the value matches the specified regular expression
     * 
     * @param <T> The type used by the underlying column
     * @param col The index of the column to replace values in
     * @param regex The regular expression that all column values
     *              to be replaced must match. May be null or empty
     * @param value The <code>IndexedValueReplacement</code> functional interface
     *              to determine the new value for each matched position.
     *              Passing null as a replacement argument should result in
     *              no change being applied
     * @return The number of values that were replaced by this operation
     */
    protected abstract <T> int replace0(int col,
            String regex, IndexedValueReplacement<T> value);

    /**
     * Performs a groupBy operation for the specified column.<br>
     * Operation codes:<br>
     * 1 = Minimum<br>
     * 2 = Maximum<br>
     * 3 = Average<br>
     * 4 = Sum<br>
     * 
     * @param col The <code>Column</code> to use for the group operation
     * @param operation The operation code to use
     * @return A <code>DataFrame</code> representing the result
     *         of the group operation
     */
    protected abstract DataFrame groupOperation(int col, int operation);

    @Override
    public String[] getColumnNames(){
        if(names != null){
            final String[] names = new String[columns.length];
            for(int i=0; i<columns.length; ++i){
                final String s = this.columns[i].name;
                names[i] = ((s == null) ? String.valueOf(i) : s);
            }
            return names;
        }
        return null;
    }

    @Override
    public String getColumnName(final int col){
        if((next == -1) || (col < 0) || (col >= columns.length)){
            throw new DataFrameException("Invalid column index: " + col);
        }
        if(names != null){
            return this.columns[col].name;
        }
        return null;
    }

    @Override
    public int getColumnIndex(final String col){
        return enforceName(col);
    }

    @Override
    public DataFrame setColumnNames(final String... names){
        if((names == null) || (names.length == 0)){
            throw new DataFrameException("Argument must not be null or empty");
        }
        if((next == -1) || (names.length != columns.length)){
            throw new DataFrameException(
                    "Length of column names array does not match number of "
                  + "columns: " + names.length + " (the DataFrame has "
                  + columns() + " columns)");

        }
        this.names = new HashMap<String, Integer>(16);
        for(int i=0; i<names.length; ++i){
            if((names[i] == null) || names[i].isEmpty()){
                throw new DataFrameException(
                        "Column name must not be null or empty");
            }
            this.names.put(names[i], i);
            this.columns[i].name = names[i];
        }
        return this;
    }

    @Override
    public boolean setColumnName(final int col, final String name){
        if((next == -1) || (col < 0) || (col >= columns.length)){
            throw new DataFrameException("Invalid column index: " + col);
        }
        if((name == null) || name.isEmpty()){
            throw new DataFrameException("Column name must not be null or empty");
        }
        if(names == null){
            this.names = new HashMap<String, Integer>(16);
        }
        boolean overridden = false;
        final String current = this.columns[col].name;
        Integer index = null;
        if(current != null){
            index = this.names.get(current);    
        }
        if((index != null) && (index == col)){
            this.names.remove(current);
            overridden = true;
        }
        this.names.put(name, col);
        this.columns[col].name = name;
        return overridden;
    }

    @Override
    public DataFrame setColumnName(final String col, final String name){
        if((col == null) || col.isEmpty() || (name == null) || name.isEmpty()){
            throw new DataFrameException("Column name must not be null or empty");
        }
        final int i = enforceName(col);
        this.names.remove(col);
        this.names.put(name, i);
        this.columns[i].name = name;
        return this;
    }

    @Override
    public DataFrame removeColumnNames(){
        this.names = null;
        for(int i=0; i<columns.length; ++i){
            this.columns[i].name = null;
        }
        return this;
    }

    @Override
    public boolean hasColumn(final String col){
        if((col == null) || col.isEmpty()){
            throw new DataFrameException("Column name must not be null or empty");
        }
        if(names == null){
            return false;
        }
        return (names.get(col) != null);
    }

    @Override
    public boolean hasColumnNames(){
        return (this.names != null);
    }

    @Override
    public Object[] getRow(final int index){
        if((index >= next) || (index < 0)){
            throw new DataFrameException("Invalid row index: " + index);
        }
        final Object[] row = new Object[columns.length];
        for(int i=0; i<columns.length; ++i){
            row[i] = columns[i].getValue(index);
        }
        return row;
    }

    @Override
    public <T extends Row> T getRow(final int index, final Class<T> classOfT){
        if(!hasColumnNames()){
            throw new DataFrameException("Columns must be labeled in order "
                    + "to use row annotation feature");

        }
        if(classOfT == null){
            throw new DataFrameException("Class argument must not be null");
        }
        if((index >= next) || (index < 0)){
            throw new DataFrameException("Invalid row index: " + index);
        }
        T row = null;
        try{
            row = classOfT.newInstance();
            for(final Field field : classOfT.getDeclaredFields()){
                final RowItem item = field.getAnnotation(RowItem.class);
                if(item != null){
                    String name = item.value();
                    if((name == null || name.isEmpty())){
                        name = field.getName();
                    }
                    final int i = enforceName(name);
                    field.setAccessible(true);
                    field.set(row, columns[i].getValue(index));
                }
            }
        }catch(InstantiationException ex){
            throw new DataFrameException(classOfT.getName() 
                    + " does not declare a default no-args constructor");
            
        }catch(IllegalAccessException ex){
            throw new DataFrameException(ex.getMessage(), ex);
        }catch(SecurityException ex){
            throw new DataFrameException("Access to field denied", ex);
        }
        return row;
    }

    @Override
    public DataFrame getRows(final int from, final int to){
        if(from >= to){
            throw new DataFrameException(
                    "End index must be greater than start index");
        }
        if((from < 0) || (to < 0) || (from >= next) || (to > next)){
            throw new DataFrameException("Invalid row index: "
                    + ((from < 0) || (from >= next) ? from : to));
        }
        final DataFrame df = createInstance();
        final int length = to - from;
        for(int j=0; j<columns.length; ++j){
            final Column c = columns[j];
            final Column col = Column.ofType(c.typeCode(), length);
            for(int i=from; i<to; ++i){
                col.setValue(i - from, c.getValue(i));
            }
            df.addColumn(col);
        }
        if(names != null){
            df.setColumnNames(getColumnNames());
        }
        return df;
    }

    @Override
    public DataFrame setRow(final int index, final Object... row){
        if((index >= next) || (index < 0)){
            throw new DataFrameException("Invalid row index: " + index);
        }
        enforceTypes(row);
        for(int i=0; i<columns.length; ++i){
            columns[i].setValue(index, row[i]);
        }
        return this;
    }

    @Override
    public DataFrame setRow(final int index, final Row row){
        if(!hasColumnNames()){
            throw new DataFrameException("Columns must be labeled in order "
                    + "to use row annotation feature");
            
        }
        if((index >= next) || (index < 0)){
            throw new DataFrameException("Invalid row index: "+index);
        }
        final Object[] items = itemsByAnnotations(row);
        for(int i=0; i<items.length; ++i){
            columns[i].setValue(index, items[i]);
        }
        return this;
    }

    @Override
    public DataFrame addRow(final Object... row){
        enforceTypes(row);
        if(next >= columns[0].capacity()){
            resize();
        }
        for(int i=0; i<columns.length; ++i){
            columns[i].setValue(next, row[i]);
        }
        ++next;
        return this;
    }

    @Override
    public DataFrame addRow(final Row row){
        if(!hasColumnNames()){
            throw new DataFrameException("Columns must be labeled in order "
                    + "to use row annotation feature");
            
        }
        if(next >= columns[0].capacity()){
            resize();
        }
        final Object[] items = itemsByAnnotations(row);
        for(int i=0; i<items.length; ++i){
            columns[i].setValue(next, items[i]);
        }
        ++next;
        return this;
    }

    @Override
    public DataFrame addRows(final DataFrame rows){
        if(rows == null){
            throw new DataFrameException("Rows must not be null");
        }
        if(rows.isEmpty()){
            return this;
        }
        //Cache
        final int nRows = rows.rows();
        final int nCols = columns.length;
        if(rows.hasColumnNames()){//Match columns by name
            for(int i=0; i<nRows; ++i){
                final Object[] row = new Object[nCols];
                for(int j=0; j<nCols; ++j){
                    final String name = columns[j].name;
                    if((name != null) && !name.isEmpty()){
                        if(rows.hasColumn(name)){
                            row[j] = rows.getColumn(name).getValue(i);
                        }else{
                            row[j] = columns[j].getDefaultValue();
                        }
                    }else{
                        row[j] = (j < rows.columns())
                                ? rows.getColumn(j).getValue(i)
                                : columns[j].getDefaultValue();
                    }
                }
                this.addRow(row);
            }
        }else{//Match columns by index
            for(int i=0; i<nRows; ++i){
                final Object[] row = new Object[nCols];
                int addedItems = 0;
                for(int j=0; j<rows.columns(); ++j){
                    row[j] = rows.getColumn(j).getValue(i);
                    ++addedItems;
                }
                if(addedItems < nCols){
                    //Add missing row items as default values
                    for(int j=addedItems; j<nCols; ++j){
                        row[j] = columns[j].getDefaultValue();
                    }
                }
                this.addRow(row);
            }
        }
        return this;
    }

    @Override
    public DataFrame insertRow(final int index, final Object... row){
        if((index > next) || (index < 0)){
            throw new DataFrameException("Invalid row index: " + index);
        }
        if(index == next){
            return addRow(row);
        }
        enforceTypes(row);
        if(next >= columns[0].capacity()){
            resize();
        }
        for(int i=0; i<columns.length; ++i){
            columns[i].insertValueAt(index, next, row[i]);
        }
        ++next;
        return this;
    }

    @Override
    public DataFrame insertRow(final int index, final Row row){
        if(!hasColumnNames()){
            throw new DataFrameException("Columns must be labeled in order "
                    + "to use row annotation feature");
            
        }
        if((index > next) || (index < 0)){
            throw new DataFrameException("Invalid row index: " + index);
        }
        if(index == next){
            return addRow(row);
        }
        final Object[] items = itemsByAnnotations(row);
        enforceTypes(items);
        if(next >= columns[0].capacity()){
            resize();
        }
        for(int i=0; i<items.length; ++i){
            columns[i].insertValueAt(index, next, items[i]);
        }
        ++next;
        return this;
    }

    @Override
    public DataFrame removeRow(final int index){
        if((index >= next) || (index < 0)){
            throw new DataFrameException("Invalid row index: " + index);
        }
        for(int i=0; i<columns.length; ++i){
            columns[i].remove(index, index+1, next);
        }
        --next;
        if((next*3) < columns[0].capacity()){
            flushAll(4);
        }
        return this;
    }

    @Override
    public DataFrame removeRows(final int from, final int to){
        if(from >= to){
            throw new DataFrameException(
                    "End index must be greater than start index");
        }
        if((from < 0) || (to < 0) || (from >= next) || (to > next)){
            throw new DataFrameException("Invalid row index: "
                    +((from < 0) || (from >= next) ? from : to));
        }
        for(int i=0; i<columns.length; ++i){
            columns[i].remove(from, to, next);
        }
        next-=(to-from);
        if((next*3) < columns[0].capacity()){
            flushAll(4);
        }
        return this;
    }

    @Override
    public int removeRows(final int col, String regex){
        if((next == -1) || (col < 0) || (col >= columns.length)){
            throw new DataFrameException("Invalid column index: " + col);
        }
        if((regex == null) || regex.isEmpty()){
            regex = "null";
        }
        final Column c = columns[col];
        final Pattern p = Pattern.compile(regex);//cache
        int i = 0;
        int k = -1;
        int removed = 0;
        while(i < next){
            if(p.matcher(String.valueOf(c.getValue(i))).matches()){
                if(k == -1){
                    k = i;
                }
                ++i;
            }else{
                if(k != -1){
                    this.removeRows(k, i);
                    final int range = (i - k);
                    removed += range;
                    i -= range;
                    k = -1;
                }else{
                    ++i;
                }
            }
        }
        if(k != -1){
            this.removeRows(k, i);
            removed += (i - k);
        }
        return removed;
    }

    @Override
    public int removeRows(final String col, final String regex){
        return removeRows(enforceName(col), regex);
    }

    @Override
    public Column removeColumn(final int col){
        if((next == -1) || (col < 0) || (col >= columns.length)){
            throw new DataFrameException("Invalid column index: " + col);
        }
        final Column[] tmp = new Column[columns.length-1];
        final Column removed = this.columns[col];
        int idx = 0;
        for(int i=0; i<columns.length; ++i){
            if(i != col){
                tmp[idx++] = columns[i];
            }
        }
        if(names != null){
            final String name = this.columns[col].name;
            if(name != null){
                this.names.remove(name);
            }
            Iterator<Map.Entry<String, Integer>> iter = names.entrySet().iterator();
            while(iter.hasNext()){
                final Map.Entry<String, Integer> entry = iter.next();
                if(entry.getValue()>=col){
                    entry.setValue(entry.getValue()-1);
                }
            }
        }
        this.columns = tmp;
        if(columns.length == 0){
            this.next = -1;
            this.columns = null;
            this.names = null;
        }
        return removed;
    }

    @Override
    public Column removeColumn(final String col){
        return removeColumn(enforceName(col));
    }

    @Override
    public boolean removeColumn(final Column col){
        if((next != -1) && (columns != null)){
            for(int i=0; i<columns.length; ++i){
                if(columns[i] == col){
                    this.removeColumn(i);
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public DataFrame insertColumn(final int index,
            final String colName, final Column col){

        if((col == null) || (colName == null) || colName.isEmpty()){
            throw new DataFrameException("Arg must not be null or empty");
        }
        col.name = colName;
        insertColumn(index, col);
        return this;
    }

    @Override
    public boolean contains(final int col, final String regex){
        return indexOf(col, regex) != -1;
    }

    @Override
    public boolean contains(final String col, final String regex){
        return contains(enforceName(col), regex);
    }

    @Override
    public int columns(){
        return (columns != null ? columns.length : 0);
    }

    @Override
    public int capacity(){
        return (columns != null ? columns[0].capacity() : 0);
    }

    @Override
    public int rows(){
        return (columns != null ? next : 0);
    }

    @Override
    public boolean isEmpty(){
        return (next <= 0);
    }

    @Override
    public void clear(){
        for(int i=0; i<columns.length; ++i){
            columns[i].remove(0, next, next);
        }
        this.next = 0;
        flushAll(2);
    }

    @Override
    public void flush(){
        if((next != -1) && (next != columns[0].capacity())){
            flushAll(0);
        }
    }

    @Override
    public Column getColumn(final int col){
        if((next == -1) || (col < 0) || (col >= columns.length)){
            throw new DataFrameException("Invalid column index: " + col);
        }
        return columns[col];
    }

    @Override
    public Column getColumn(final String col){
        return getColumn(enforceName(col));
    }

    @Override
    public DataFrame getColumns(final int... cols){
        if(cols == null){
            throw new DataFrameException("Columns argument must not be null");
        }
        if(next == -1){
            throw new DataFrameException("DataFrame has no columns to select");
        }
        this.flush();
        final DataFrame df = createInstance();
        for(final int col : cols){
            if((col < 0) || (col >= columns.length)){
                throw new DataFrameException("Invalid column index: " + col);
            }
            df.addColumn(columns[col]);
        }
        return df;
    }

    @Override
    public DataFrame getColumns(final String... cols){
        if(cols == null){
            throw new DataFrameException("Columns argument must not be null");
        }
        if(next == -1){
            throw new DataFrameException("DataFrame has no columns to select");
        }
        this.flush();
        final DataFrame df = createInstance();
        for(final String col : cols){
            df.addColumn(columns[enforceName(col)]);
        }
        return df;
    }

    @Override
    public DataFrame getColumns(final Class<?>... cols){
        if(cols == null){
            throw new DataFrameException("Columns argument must not be null");
        }
        if(next == -1){
            throw new DataFrameException("DataFrame has no columns to select");
        }
        this.flush();
        final DataFrame df = createInstance();
        for(final Class<?> elemType : cols){
            if(elemType != null){
                for(int i=0; i<columns.length; ++i){
                    if(elemType.isAssignableFrom(columns[i].memberClass())){
                        df.addColumn(columns[i]);
                    }
                }
            }
        }
        return df;
    }

    @Override
    public int indexOf(final int col, String regex){
        if((next == -1) || (col < 0) || (col >= columns.length)){
            throw new DataFrameException("Invalid column index: " + col);
        }
        if((regex == null) || regex.isEmpty()){
            regex = "null";
        }
        final Column c = columns[col];
        final Pattern p = Pattern.compile(regex);//cache
        for(int i=0; i<next; ++i){
            if(p.matcher(String.valueOf(c.getValue(i))).matches()){
                return i;
            }
        }
        return -1;
    }

    @Override
    public int indexOf(final String col, final String regex){
        return indexOf(enforceName(col), regex);
    }

    @Override
    public int indexOf(final int col, final int startFrom, String regex){
        if((next == -1) || (col < 0) || (col >= columns.length)){
            throw new DataFrameException("Invalid column index: " + col);
        }
        if((regex == null) || regex.isEmpty()){
            regex = "null";
        }
        if((startFrom < 0) || (startFrom >= next)){
            throw new DataFrameException("Invalid start argument: " + startFrom);
        }
        final Column c = columns[col];
        final Pattern p = Pattern.compile(regex);//cache
        for(int i=startFrom; i<next; ++i){
            if(p.matcher(String.valueOf(c.getValue(i))).matches()){
                return i;
            }
        }
        return -1;
    }

    @Override
    public int indexOf(final String col, final int startFrom, final String regex){
        return indexOf(enforceName(col), startFrom, regex);
    }

    @Override
    public int[] indexOfAll(final int col, String regex){
        if((next == -1) || (col < 0) || (col >= columns.length)){
            throw new DataFrameException("Invalid column index: " + col);
        }
        if((regex == null) || regex.isEmpty()){
            regex = "null";
        }
        final Column c = columns[col];
        final Pattern p = Pattern.compile(regex);//cache
        int[] res = new int[16];
        int hits = 0;
        for(int i=0; i<next; ++i){
            if(p.matcher(String.valueOf(c.getValue(i))).matches()){
                if(hits>=res.length){//resize
                    final int[] tmp = new int[res.length*2];
                    for(int j=0; j<hits; ++j){
                        tmp[j] = res[j];
                    }
                    res = tmp;
                }
                res[hits++] = i;
            }
        }
        if(res.length != hits){//trim
            final int[] tmp = new int[hits];
            for(int j=0; j<hits; ++j){
                tmp[j] = res[j];
            }
            res = tmp;
        }
        return (hits == 0 ? new int[0] : res);
    }

    @Override
    public int[] indexOfAll(final String col, final String regex){
        return indexOfAll(enforceName(col), regex);
    }

    @Override
    public DataFrame filter(final int col, String regex){
        if((next == -1) || (col < 0) || (col >= columns.length)){
            throw new DataFrameException("Invalid column index: " + col);
        }
        if((regex == null) || regex.isEmpty()){
            regex = "null";
        }
        final Column c = columns[col];
        final Pattern p = Pattern.compile(regex);//cache
        final DataFrame df = createInstance();
        for(int i=0; i<columns.length; ++i){
            df.addColumn(Column.ofType(columns[i].typeCode()));
        }
        for(int i=0; i<next; ++i){
            if(p.matcher(String.valueOf(c.getValue(i))).matches()){
                df.addRow(getRow(i));
            }
        }
        if(names != null){
            df.setColumnNames(getColumnNames());
        }
        df.flush();
        return df;
    }

    @Override
    public DataFrame filter(final String col, final String regex){
        return filter(enforceName(col), regex);
    }

    @Override
    public DataFrame include(final int col, String regex){
        if((next == -1) || (col < 0) || (col >= columns.length)){
            throw new DataFrameException("Invalid column index: " + col);
        }
        if((regex == null) || regex.isEmpty()){
            regex = "null";
        }
        final Column c = columns[col];
        final Pattern p = Pattern.compile(regex);//cache
        int i = 0;
        int k = -1;
        while(i < next){
            if(!p.matcher(String.valueOf(c.getValue(i))).matches()){
                if(k == -1){
                    k = i;
                }
                ++i;
            }else{
                if(k != -1){
                    this.removeRows(k, i);
                    i -= (i - k);
                    k = -1;
                }else{
                    ++i;
                }
            }
        }
        if(k != -1){
            this.removeRows(k, i);
        }
        return this;
    }

    @Override
    public DataFrame include(final String col, final String regex){
        return include(enforceName(col), regex);
    }

    @Override
    public DataFrame drop(final int col, String regex){
        if((next == -1) || (col < 0) || (col >= columns.length)){
            throw new DataFrameException("Invalid column index: " + col);
        }
        if((regex == null) || regex.isEmpty()){
            regex = "null";
        }
        final Column c = columns[col];
        final Pattern p = Pattern.compile(regex);//cache
        final DataFrame df = createInstance();
        for(int i=0; i<columns.length; ++i){
            df.addColumn(Column.ofType(columns[i].typeCode()));
        }
        for(int i=0; i<next; ++i){
            if(!p.matcher(String.valueOf(c.getValue(i))).matches()){
                df.addRow(getRow(i));
            }
        }
        if(names != null){
            df.setColumnNames(getColumnNames());
        }
        df.flush();
        return df;
    }

    @Override
    public DataFrame drop(final String col, final String regex){
        return drop(enforceName(col), regex);
    }

    @Override
    public DataFrame exclude(final int col, final String regex){
        this.removeRows(col, regex);
        return this;
    }

    @Override
    public DataFrame exclude(final String col, final String regex){
        return exclude(enforceName(col), regex);
    }

    @Override
    public int replace(final int col, final String regex, final Object value){
        return replace0(col, regex, DataFrameUtils.indexedWrapper(value));
    }

    @Override
    public int replace(final String col, final String regex, final Object value){
        return replace(enforceName(col), regex, value);
    }

    @Override
    public <T> int replace(final int col, final ValueReplacement<T> value){
        return replace0(col, ".*", DataFrameUtils.indexedWrapper(value));
    }

    @Override
    public <T> int replace(final String col,
            final ValueReplacement<T> value){

        return replace(enforceName(col), value);
    }

    @Override
    public <T> int replace(final int col,
            final IndexedValueReplacement<T> value){

        return replace0(col, ".*", value);
    }

    @Override
    public <T> int replace(final String col,
            final IndexedValueReplacement<T> value){

        return replace(enforceName(col), value);
    }

    @Override
    public <T> int replace(final int col, final String regex,
            final ValueReplacement<T> value){

        return replace0(col, regex, DataFrameUtils.indexedWrapper(value));
    }

    @Override
    public <T> int replace(final String col, final String regex,
            final ValueReplacement<T> value){

        return replace(enforceName(col), regex, value);
    }

    @Override
    public <T> int replace(final int col, final String regex,
            final IndexedValueReplacement<T> value){

        return replace0(col, regex, value);
    }

    @Override
    public <T> int replace(final String col, final String regex,
            final IndexedValueReplacement<T> value){

        return replace(enforceName(col), regex, value);
    }

    @Override
    public int count(final int col, String regex){
        if((next == -1) || (col < 0) || (col >= columns.length)){
            throw new DataFrameException("Invalid column index: " + col);
        }
        if((regex == null) || regex.isEmpty()){
            regex = "null";
        }
        final Column c = columns[col];
        final Pattern p = Pattern.compile(regex);//cache
        int count = 0;
        for(int i=0; i<next; ++i){
            if(p.matcher(String.valueOf(c.getValue(i))).matches()){
                ++count;
            }
        }
        return count;
    }

    @Override
    public int count(final String col, final String regex){
        return count(enforceName(col), regex);
    }

    @Override
    public <T> Set<T> unique(final int col){
        if((next == -1) || (col < 0) || (col >= columns.length)){
            throw new DataFrameException("Invalid column index: " + col);
        }
        final Column c = columns[col];
        final Set<T> unique = new HashSet<>();
        for(int i=0; i<next; ++i){
            final T value = c.getGenericValue(i);
            if(value != null){
                unique.add(value);
            }
        }
        return unique;
    }

    @Override
    public <T> Set<T> unique(final String col){
        return unique(enforceName(col));
    }

    @Override
    public DataFrame groupMinimumBy(final int col){
        return groupOperation(col, 1);
    }

    @Override
    public DataFrame groupMinimumBy(final String col){
        return groupMinimumBy(enforceName(col));
    }

    @Override
    public DataFrame groupMaximumBy(final int col){
        return groupOperation(col, 2);
    }

    @Override
    public DataFrame groupMaximumBy(final String col){
        return groupMaximumBy(enforceName(col));
    }

    @Override
    public DataFrame groupAverageBy(final int col){
        return groupOperation(col, 3);
    }

    @Override
    public DataFrame groupAverageBy(final String col){
        return groupAverageBy(enforceName(col));
    }

    @Override
    public DataFrame groupSumBy(final int col){
        return groupOperation(col, 4);
    }

    @Override
    public DataFrame groupSumBy(final String col){
        return groupSumBy(enforceName(col));
    }

    @Override
    public DataFrame join(final DataFrame df){
        if(df == null){
            throw new DataFrameException(
                    "DataFrame argument must not be null");
        }
        if(!hasColumnNames()){
            throw new DataFrameException(
                    "DataFrame must has column labels");
        }
        if(!df.hasColumnNames()){
            throw new DataFrameException(
                    "DataFrame argument must have column labels");
        }
        String col = null;
        final String[] n = df.getColumnNames();
        for(int i=0; i<n.length; ++i){
            if(this.names.containsKey(n[i])){
                if(col != null){
                    throw new DataFrameException(
                            "DataFrame argument has more than one matching column");

                }else{
                    col = n[i];
                }
            }
        }
        if(col == null){
            throw new DataFrameException(
                    "DataFrame argument has no matching column");
        }
        return join(df, col, col);
    }

    @Override
    public DataFrame join(final DataFrame df, final String col){
        return join(df, col, col);
    }

    @Override
    public DataFrame join(final DataFrame df, final String col1, final String col2){
        return DataFrameUtils.join(this, col1, df, col2);
    }

    @Override
    public String info(){
        final StringBuilder sb = new StringBuilder();
        final String nl = System.lineSeparator();
        sb.append("Type:    ");
        sb.append(isNullable() ? "Nullable" : "Default");
        sb.append(nl);
        sb.append("Columns: ");
        final int cols = columns();
        sb.append(cols);
        sb.append(nl);
        sb.append("Rows:    ");
        sb.append(rows());
        sb.append(nl);

        if(columns == null){
            return sb.toString();
        }
        final DataFrame types = new DefaultDataFrame(
                new StringColumn("column", cols),
                new StringColumn("type", cols),
                new ByteColumn("code", cols));
        
        String[] cnames = this.getColumnNames();
        if(cnames == null){
            cnames = new String[columns.length];
            for(int i=0; i<columns.length; ++i){
                cnames[i] = String.valueOf(i);
            }
        }
        for(int i=0; i<cnames.length; ++i){
            types.setRow(i, cnames[i],
                            columns[i].typeName(),
                            columns[i].typeCode());
        }
        sb.append(types.toString());
        return sb.toString();
    }

    @Override
    public Object[][] toArray(){
        if(next == -1){
            return null;
        }
        final Object[][] a = new Object[columns.length][next];
        for(int i=0; i<columns.length; ++i){
            final Column c = getColumn(i);
            for(int j=0; j<next; ++j){
                a[i][j] = c.getValue(j);
            }
        }
        return a;
    }

    @Override
    public String toString(){
        if(columns == null){
            return "uninitialized DataFrame instance";
        }
        final String nl = System.lineSeparator();
        int[] max = new int[columns.length];
        int maxIdx = String.valueOf(next-1).length();
        for(int i=0; i<columns.length; ++i){
            int k = 0;
            for(int j=0; j<next; ++j){
                if(String.valueOf(columns[i].getValue(j)).length() > k){
                    k = String.valueOf(columns[i].getValue(j)).length();
                }
            }
            max[i] = k;
        }
        String[] n = new String[columns.length];
        if(names != null){
            final Set<Map.Entry<String, Integer>> set = names.entrySet();
            for(int i=0; i<columns.length; ++i){
                String s = null;
                for(final Map.Entry<String, Integer> e : set){
                    if(e.getValue() == i){
                        s = e.getKey();
                        break;
                    }
                }
                n[i] = (s != null ? s : String.valueOf(i));
            }
        }else{
            for(int i=0; i<columns.length; ++i){
                n[i] = (i+" ");
            }
        }
        for(int i=0; i<columns.length; ++i){
            max[i] = (max[i]>=n[i].length() ? max[i] : n[i].length());
        }
        final StringBuilder sb = new StringBuilder();
        for(int i=0; i<maxIdx; ++i){
            sb.append("_");
        }
        sb.append("|");
        for(int i=0; i<columns.length; ++i){
            sb.append(" ");
            sb.append(n[i]);
            for(int j=(max[i]-n[i].length()); j>0; --j){
                sb.append(" ");
            }
        }
        sb.append(nl);
        for(int i=0; i<next; ++i){
            sb.append(i);
            for(int ii=0; ii<(maxIdx-String.valueOf(i).length()); ++ii){
                sb.append(" ");
            }
            sb.append("| ");
            for(int j=0; j<columns.length; ++j){
                final Object val = columns[j].getValue(i);
                final String s = (val != null ? val.toString() : "null");   
                sb.append(s);
                for(int k=(max[j]-s.length()); k>=0; --k){
                    sb.append(" ");
                }
            }
            sb.append(nl);
        }
        return sb.toString();
    }

    @Override
    public DataFrame clone(){
        return DataFrameUtils.copyOf(this);
    }

    @Override
    public int hashCode(){
        int hash = 0;
        final String[] n = this.getColumnNames();
        if(n != null){
            for(int i=0; i<columns.length; ++i){
                hash += n[i].hashCode();
                hash += columns[i].typeCode();
            }
        }
        if(columns != null){
            for(int i=0; i<columns.length; ++i){
                hash += columns[i].hashCode();
            }
        }
        return hash;
    }

    @Override
    public int memoryUsage(){
        if(next == -1){
            return 0;
        }
        this.flush();
        int size = 0;
        for(int i=0; i<columns.length; ++i){
            size += columns[i].memoryUsage();
        }
        return size;
    }

    @Override
    public Iterator<Column> iterator(){
        return new ColumnIterator(this);
    }

    /**
     * Resizes all columns sequentially
     */
    protected void resize(){
        for(int i=0; i<columns.length; ++i){
            columns[i].resize();
        }
    }

    /**
     * Enforces that all requirements are met in order to access a column
     * by its name. Throws an exception in the case of failure or returns
     * the index of the column in the case of success
     * 
     * @param col The name to check
     * @return The index of the column with the specified name 
     */
    protected int enforceName(final String col){
        if((col == null) || col.isEmpty()){
            throw new DataFrameException("Column name must not be null or empty");
        }
        if(names == null){
            throw new DataFrameException("Column names not set");
        }
        final Integer c = names.get(col);
        if(c == null){
            throw new DataFrameException("Invalid column name: '" + col + "'");
        }
        return c;
    }

    /**
     * Sequentially performs a flush operation on all columns. A buffer
     * can be set to keep some extra space between the current entries
     * and the column capacity 
     * 
     * @param buffer A buffer applied to each column. Using 0 (zero)
     *               will apply no buffer at all and will shrink each
     *               column to its minimum required length
     */
    protected void flushAll(final int buffer){
        for(int i=0; i<columns.length; ++i){
            columns[i].matchLength(next + buffer);
        }
    }

    /**
     * Generates an exception message for invalid get method calls
     * 
     * @param index The index of the <code>Column</code> for which the
     *              get request was made
     * @param expected The type code of the <code>Column</code> for which
     *                 the get request could be fulfilled
     * @param actual The <code>Column</code> for which the get request was made
     * @return A String with error details. To be used as an exception message
     */
    protected String createInvalidGetMessage(final int index, final byte expected,
            final Column actual){

        final Column colExpected = Column.ofType(expected);
        final String descr = (actual.name != null)
                ? "'" + actual.name + "'"
                : ("at index " + index);

        return "Cannot get " + colExpected.typeName() + " value from column "
             + descr + ". Expected " + colExpected.getClass().getSimpleName()
             + " but found " + actual.getClass().getSimpleName();
    }

    /**
     * Generates an exception message for invalid set method calls
     * 
     * @param index The index of the <code>Column</code> for which the
     *              set request was made
     * @param expected The type code of the <code>Column</code> for which
     *                 the set request could be fulfilled
     * @param actual The <code>Column</code> for which the set request was made
     * @return A String with error details. To be used as an exception message
     */
    protected String createInvalidSetMessage(final int index, final byte expected,
            final Column actual){

        final Column colExpected = Column.ofType(expected);
        final String descr = (actual.name != null)
                ? "'" + actual.name + "'"
                : ("at index " + index);

        return "Cannot set " + colExpected.typeName() + " value in column "
             + descr + ". Expected " + colExpected.getClass().getSimpleName()
             + " but found " + actual.getClass().getSimpleName();
    }

    /**
     * Ensures that conditions are met for set-theoretic operations with columns
     * 
     * @param df The <code>DataFrame</code> argument to check
     */
    protected void ensureValidColumnSetOperation(final DataFrame df){
        if(next == -1){
            throw new DataFrameException("Uninitialized DataFrame instance");
        }
        if(df == null){
            throw new DataFrameException("DataFrame argument must not be null");
        }
        if(rows() != df.rows()){
            throw new DataFrameException(String.format(
                    "Invalid number of rows for argument DataFrame. "
                    + "Expected %s but found %s",
                    rows(), df.rows()));
        }
        if(!hasColumnNames() || !df.hasColumnNames()){
            throw new DataFrameException("Both DataFrame instances "
                    + "must have labeled columns");
        }
        if(capacity() != rows()){
            this.flush();
        }
        if(df.capacity() != df.rows()){
            df.flush();
        }
    }

    /**
     * Ensures that conditions are met for set-theoretic operations with rows
     * 
     * @param df The <code>DataFrame</code> argument to check
     */
    protected void ensureValidRowSetOperation(final DataFrame df){
        if(next == -1){
            throw new DataFrameException("Uninitialized DataFrame instance");
        }
        if(df == null){
            throw new DataFrameException("DataFrame argument must not be null");
        }
        if(columns() != df.columns()){
            throw new DataFrameException(String.format(
                    "Invalid number of columns for argument DataFrame. "
                    + "Expected %s but found %s",
                    columns(), df.columns()));
        }
        if(hasColumnNames() ^ df.hasColumnNames()){
            throw new DataFrameException("Both DataFrame instances "
                    + "must have either labeled columns or unlabeled columns");
        }
    }
}
