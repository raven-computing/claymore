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

import java.util.Iterator;

/**
 * An iterator over a DataFrame.<br>
 * Enables a DataFrame to be target of the for-each-loop.
 * 
 * @see Iterable
 * @since 1.0.0
 *
 */
public class ColumnIterator implements Iterator<Column> {

    private final Column[] columns;
    private final int nColumns;
    private int ptr = 0;

    /**
     * Constructs a new <code>ColumnIterator</code> from the given DataFame
     * 
     * @param df The DataFrame to construct an iterator from
     */
    protected ColumnIterator(final DataFrame df){
        this.nColumns = df.columns();
        this.columns = new Column[nColumns];
        for(int i=0; i<nColumns; ++i){
            this.columns[i] = df.getColumn(i);
        }
    }

    @Override
    public boolean hasNext(){
        return (ptr != nColumns);
    }

    @Override
    public Column next(){
        return this.columns[ptr++];
    }
}
