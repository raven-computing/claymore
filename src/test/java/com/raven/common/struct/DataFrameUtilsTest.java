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

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.raven.common.struct.BooleanColumn;
import com.raven.common.struct.ByteColumn;
import com.raven.common.struct.CharColumn;
import com.raven.common.struct.DataFrame;
import com.raven.common.struct.DefaultDataFrame;
import com.raven.common.struct.DoubleColumn;
import com.raven.common.struct.FloatColumn;
import com.raven.common.struct.IntColumn;
import com.raven.common.struct.LongColumn;
import com.raven.common.struct.NullableBooleanColumn;
import com.raven.common.struct.NullableByteColumn;
import com.raven.common.struct.NullableCharColumn;
import com.raven.common.struct.NullableDataFrame;
import com.raven.common.struct.NullableDoubleColumn;
import com.raven.common.struct.NullableFloatColumn;
import com.raven.common.struct.NullableIntColumn;
import com.raven.common.struct.NullableLongColumn;
import com.raven.common.struct.NullableShortColumn;
import com.raven.common.struct.NullableStringColumn;
import com.raven.common.struct.ShortColumn;
import com.raven.common.struct.StringColumn;

/**
 * Tests for static utility methods provided by the DataFrame interface.
 *
 */
public class DataFrameUtilsTest {

    String[] columnNames;
    DefaultDataFrame df;
    NullableDataFrame nulldf;

    @BeforeClass
    public static void setUpBeforeClass(){ }

    @AfterClass
    public static void tearDownAfterClass(){ }

    @Before
    public void setUp(){

        columnNames = new String[]{
                "byteCol",    // 0
                "shortCol",   // 1
                "intCol",     // 2
                "longCol",    // 3
                "stringCol",  // 4
                "charCol",    // 5
                "floatCol",   // 6
                "doubleCol",  // 7
                "booleanCol", // 8
                "binaryCol"   // 9
        };

        df = new DefaultDataFrame(
                columnNames, 
                new ByteColumn(new byte[]{
                        1,2,3
                }),
                new ShortColumn(new short[]{
                        1,2,3
                }),
                new IntColumn(new int[]{
                        1,2,3
                }),
                new LongColumn(new long[]{
                        1l,2l,3l
                }),
                new StringColumn(new String[]{
                        "1","2","3"
                }),
                new CharColumn(new char[]{
                        'a','b','c'
                }),
                new FloatColumn(new float[]{
                        1f,2f,3f
                }),
                new DoubleColumn(new double[]{
                        1.0,2.0,3.0
                }),
                new BooleanColumn(new boolean[]{
                        true,false,true
                }),
                new BinaryColumn(new byte[][]{
                    new byte[]{1,2,3,4,5},
                    new byte[]{5,4,3,2,1},
                    new byte[]{5,4,1,2,3}
                }));

        nulldf = new NullableDataFrame(
                columnNames, 
                new NullableByteColumn(new Byte[]{
                        1,null,3
                }),
                new NullableShortColumn(new Short[]{
                        1,null,3
                }),
                new NullableIntColumn(new Integer[]{
                        1,null,3
                }),
                new NullableLongColumn(new Long[]{
                        1l,null,3l
                }),
                new NullableStringColumn(new String[]{
                        "1",null,"3"
                }),
                new NullableCharColumn(new Character[]{
                        'a',null,'c'
                }),
                new NullableFloatColumn(new Float[]{
                        1f,null,3f
                }),
                new NullableDoubleColumn(new Double[]{
                        1.0,null,3.0
                }),
                new NullableBooleanColumn(new Boolean[]{
                        true,null,false
                }),
                new NullableBinaryColumn(new byte[][]{
                    new byte[]{1,2,3,4,5},
                    null,
                    new byte[]{5,4,1,2,3}
                }));
    }

    @After
    public void tearDown(){ }

    @Test
    public void testExactCopyForDefault(){
        final String MSG = "Value missmatch. Is not exact copy of original";
        DataFrame copy = DataFrame.copy(df);
        assertTrue("DataFrame should be of type DefaultDataFrame",
                copy instanceof DefaultDataFrame);

        assertTrue("DataFrame should have 3 rows", copy.rows() == 3);
        assertTrue("DataFrame should have 10 columns", copy.columns() == 10);
        assertArrayEquals("Column names should match",
                columnNames, copy.getColumnNames());

        for(int i=1; i<=copy.rows(); ++i){
            assertTrue(MSG, copy.getByte(0, i-1) == i);
        }
        for(int i=1; i<=copy.rows(); ++i){
            assertTrue(MSG, copy.getShort(1, i-1) == i);
        }
        for(int i=1; i<=copy.rows(); ++i){
            assertTrue(MSG, copy.getInt(2, i-1) == i);
        }
        for(int i=1; i<=copy.rows(); ++i){
            assertTrue(MSG, copy.getLong(3, i-1) == i);
        }
        for(int i=1; i<=copy.rows(); ++i){
            assertTrue(MSG, copy.getString(4, i-1).equals(String.valueOf(i)));
        }
        assertTrue(MSG, copy.getChar(5, 0) == 'a');
        assertTrue(MSG, copy.getChar(5, 1) == 'b');
        assertTrue(MSG, copy.getChar(5, 2) == 'c');
        for(int i=1; i<=copy.rows(); ++i){
            assertTrue(MSG, copy.getFloat(6, i-1).equals(Float.valueOf(i)));
        }
        for(int i=1; i<=copy.rows(); ++i){
            assertTrue(MSG, copy.getDouble(7, i-1).equals(Double.valueOf(i)));
        }
        assertTrue(MSG, copy.getBoolean(8, 0));
        assertFalse(MSG, copy.getBoolean(8, 1));
        assertTrue(MSG, copy.getBoolean(8, 2));
        
        
        byte[] b0 = df.getBinary(9, 0);
        byte[] b1 = df.getBinary(9, 1);
        byte[] b2 = df.getBinary(9, 2);
        assertArrayEquals(MSG, b0, copy.getBinary(9, 0));
        assertArrayEquals(MSG, b1, copy.getBinary(9, 1));
        assertArrayEquals(MSG, b2, copy.getBinary(9, 2));
        
        assertTrue("Copy should have different reference",
                b0 != copy.getBinary(9, 0));
        assertTrue("Copy should have different reference",
                b1 != copy.getBinary(9, 1));
        assertTrue("Copy should have different reference",
                b2 != copy.getBinary(9, 2));
    }

    @Test
    public void testExactCopyForNullable(){
        final String MSG = "Value missmatch. Is not exact copy of original";
        DataFrame copy = DataFrame.copy(nulldf);
        assertTrue("DataFrame should be of type NullableDataFrame",
                copy instanceof NullableDataFrame);

        assertTrue("DataFrame should have 3 rows", copy.rows() == 3);
        assertTrue("DataFrame should have 10 columns", copy.columns() == 10);
        assertArrayEquals("Column names should match",
                columnNames, copy.getColumnNames());

        for(int i=1; i<=copy.rows(); ++i){
            if(i == 2){
                assertNull(MSG, copy.getByte(0, i-1));
            }else{
                assertTrue(MSG, copy.getByte(0, i-1) == i);
            }
        }
        for(int i=1; i<=copy.rows(); ++i){
            if(i == 2){
                assertNull(MSG, copy.getShort(1, i-1));
            }else{
                assertTrue(MSG, copy.getShort(1, i-1) == i);
            }
        }
        for(int i=1; i<=copy.rows(); ++i){
            if(i == 2){
                assertNull(MSG, copy.getInt(2, i-1));
            }else{
                assertTrue(MSG, copy.getInt(2, i-1) == i);
            }
        }
        for(int i=1; i<=copy.rows(); ++i){
            if(i == 2){
                assertNull(MSG, copy.getLong(3, i-1));
            }else{
                assertTrue(MSG, copy.getLong(3, i-1) == i);
            }
        }
        for(int i=1; i<=copy.rows(); ++i){
            if(i == 2){
                assertNull(MSG, copy.getString(4, i-1));
            }else{
                assertTrue(MSG, copy.getString(4, i-1).equals(String.valueOf(i)));
            }
        }
        assertTrue(MSG, copy.getChar(5, 0) == 'a');
        assertNull(MSG, copy.getChar(5, 1));
        assertTrue(MSG, copy.getChar(5, 2) == 'c');
        for(int i=1; i<=copy.rows(); ++i){
            if(i == 2){
                assertNull(MSG, copy.getFloat(6, i-1));
            }else{
                assertTrue(MSG, copy.getFloat(6, i-1).equals(Float.valueOf(i)));
            }
        }
        for(int i=1; i<=copy.rows(); ++i){
            if(i == 2){
                assertNull(MSG, copy.getDouble(7, i-1));
            }else{
                assertTrue(MSG, copy.getDouble(7, i-1).equals(Double.valueOf(i)));
            }
        }
        assertTrue(MSG, copy.getBoolean(8, 0));
        assertNull(MSG, copy.getBoolean(8, 1));
        assertFalse(MSG, copy.getBoolean(8, 2));
        
        byte[] b0 = nulldf.getBinary(9, 0);
        byte[] b2 = nulldf.getBinary(9, 2);
        assertArrayEquals(MSG, b0, copy.getBinary(9, 0));
        assertTrue(MSG, copy.getBinary(9, 1) == null);
        assertArrayEquals(MSG, b2, copy.getBinary(9, 2));
        
        assertTrue("Copy should have different reference",
                b0 != copy.getBinary(9, 0));
        assertTrue("Copy should have different reference",
                b2 != copy.getBinary(9, 2));
    }

    @Test
    public void testLikeDefault(){
        DataFrame df2 = DataFrame.like(df);
        assertTrue("DataFrame should be a DefaultDataFrame",
                df2 instanceof DefaultDataFrame);

        assertTrue("DataFrame should have 10 columns", df2.columns() == df.columns());
        assertTrue("DataFrame should be empty", df2.isEmpty());
        assertArrayEquals("Columns names do not match",
                df.getColumnNames(), df2.getColumnNames());

        for(int i=0; i<df2.columns(); ++i){
            assertTrue("Columns have deviating types",
                    df.getColumn(i).typeCode() == df2.getColumn(i).typeCode());
        }
    }

    @Test
    public void testLikeNullable(){
        DataFrame df2 = DataFrame.like(nulldf);
        assertTrue("DataFrame should be a NullableDataFrame",
                df2 instanceof NullableDataFrame);

        assertTrue("DataFrame should have 10 columns", df2.columns() == df.columns());
        assertTrue("DataFrame should be empty", df2.isEmpty());
        assertArrayEquals("Columns names do not match",
                df.getColumnNames(), df2.getColumnNames());

        for(int i=0; i<df2.columns(); ++i){
            assertTrue("Columns have deviating types",
                    nulldf.getColumn(i).typeCode() == df2.getColumn(i).typeCode());
        }
    }

    @Test
    public void testLikeUninitialized(){
        DataFrame df2 = DataFrame.like(new DefaultDataFrame());
        assertTrue("DataFrame should be a DefaultDataFrame",
                df2 instanceof DefaultDataFrame);

        assertTrue("DataFrame should have 0 columns", df2.columns() == 0);
        assertTrue("DataFrame should be empty", df2.isEmpty());
        assertFalse("DataFrame should not have column names", df2.hasColumnNames());
        df2 = DataFrame.like(new NullableDataFrame());
        assertTrue("DataFrame should be a NullableDataFrame",
                df2 instanceof NullableDataFrame);

        assertTrue("DataFrame should have 0 columns", df2.columns() == 0);
        assertTrue("DataFrame should be empty", df2.isEmpty());
        assertFalse("DataFrame should not have column names", df2.hasColumnNames());
    }

    //*************************************//
    //           Join operations           //
    //*************************************//

    @Test
    public void testJoinBothKeysSpecifiedDefaultDefault(){
        DataFrame df1 = new DefaultDataFrame(
                Column.create("A1", 1, 2, 3, 4, 5, 6),
                Column.create("B", "AAA","BBB","CCC","DDD","EEE","FFF"),
                Column.create("C", 53,51,54,62,41,54));

        DataFrame df2 = new DefaultDataFrame(
                Column.create("D", 517,575,896,741,210,231),
                Column.create("A2", 1, 2, 2, 4, 1, 6),
                Column.create("E", "2018", "2019", "2019", "2020", "2018", "2017"));

        DataFrame res = df1.join(df2, "A1", "A2");
        assertTrue("DataFrame should be of type DefaultDataFrame",
                res instanceof DefaultDataFrame);

        assertTrue("DataFrame should have 5 columns", res.columns() == 5);
        assertTrue("DataFrame should have 6 rows", res.rows() == 6);
        assertArrayEquals("Column names do not match",
                new String[]{"A1","B","C","D","E"}, res.getColumnNames());
        
        assertTrue("Column does not match",
                res.getColumn("A1").typeCode() == df1.getColumn("A1").typeCode());
        assertTrue("Column does not match",
                res.getColumn("B").typeCode() == df1.getColumn("B").typeCode());
        assertTrue("Column does not match",
                res.getColumn("C").typeCode() == df1.getColumn("C").typeCode());
        assertTrue("Column does not match",
                res.getColumn("D").typeCode() == df2.getColumn("D").typeCode());
        assertTrue("Column does not match",
                res.getColumn("E").typeCode() == df2.getColumn("E").typeCode());
        
        assertTrue("DataFrame result does not match expected",
                res.count("A1", "1") == 2);
        assertTrue("DataFrame result does not match expected",
                res.count("A1", "2") == 2);
        assertTrue("DataFrame result does not match expected",
                res.count("A1", "3") == 0);
        assertTrue("DataFrame result does not match expected",
                res.count("A1", "4") == 1);
        assertTrue("DataFrame result does not match expected",
                res.count("A1", "5") == 0);
        assertTrue("DataFrame result does not match expected",
                res.count("A1", "6") == 1);
    }

    @Test
    public void testJoinOneKeySpecifiedDefaultNullable(){
        DataFrame df1 = new DefaultDataFrame(
                Column.create("A", 1, 2, 3, 4, 5, 6),
                Column.create("B", "AAA","BBB","CCC","DDD","EEE","FFF"),
                Column.create("C", 53,51,54,62,41,54));
        
        DataFrame df2 = new NullableDataFrame(
                Column.nullable("D", 517,575,896,741,null,231),
                Column.nullable("A", 1, 2, null, 4, 1, 6),
                Column.nullable("E", "2018", "2019", null, "2020", "2018", null));

        DataFrame res = df1.join(df2, "A");
        assertTrue("DataFrame should be of type NullableDataFrame",
                res instanceof NullableDataFrame);

        assertTrue("DataFrame should have 5 columns", res.columns() == 5);
        assertTrue("DataFrame should have 5 rows", res.rows() == 5);
        assertArrayEquals("Column names do not match",
                new String[]{"A","B","C","D","E"}, res.getColumnNames());
        
        assertTrue("Column does not match",
                res.getColumn("A").typeCode() == df1.getColumn("A").asNullable().typeCode());
        assertTrue("Column does not match",
                res.getColumn("B").typeCode() == df1.getColumn("B").asNullable().typeCode());
        assertTrue("Column does not match",
                res.getColumn("C").typeCode() == df1.getColumn("C").asNullable().typeCode());
        assertTrue("Column does not match",
                res.getColumn("D").typeCode() == df2.getColumn("D").typeCode());
        assertTrue("Column does not match",
                res.getColumn("E").typeCode() == df2.getColumn("E").typeCode());
        
        assertTrue("DataFrame result does not match expected", res.count("A", "1") == 2);
        assertTrue("DataFrame result does not match expected", res.count("A", "2") == 1);
        assertTrue("DataFrame result does not match expected", res.count("A", "3") == 0);
        assertTrue("DataFrame result does not match expected", res.count("A", "4") == 1);
        assertTrue("DataFrame result does not match expected", res.count("A", "5") == 0);
        assertTrue("DataFrame result does not match expected", res.count("A", "6") == 1);
    }

    @Test
    public void testJoinNoKeySpecifiedNullableDefault(){
        DataFrame df1 = new NullableDataFrame(
                Column.nullable("D", 517,575,896,741,null,231),
                Column.nullable("A", 1, 2, null, 4, 1, 6),
                Column.nullable("E", "2018", "2019", null, "2020", "2018", null));

        DataFrame df2 = new DefaultDataFrame(
                Column.create("B", "AAA","BBB","CCC","DDD","EEE","FFF"),
                Column.create("C", 53,51,54,62,41,54),
                Column.create("A", 1, 2, 3, 4, 5, 6));

        DataFrame res = df1.join(df2);
        assertTrue("DataFrame should be of type NullableDataFrame",
                res instanceof NullableDataFrame);

        assertTrue("DataFrame should have 5 columns", res.columns() == 5);
        assertTrue("DataFrame should have 5 rows", res.rows() == 5);
        assertArrayEquals("Column names do not match",
                new String[]{"D","A","E","B","C"}, res.getColumnNames());
        
        assertTrue("Column does not match",
                res.getColumn("A").typeCode() == df2.getColumn("A").asNullable().typeCode());
        assertTrue("Column does not match",
                res.getColumn("B").typeCode() == df2.getColumn("B").asNullable().typeCode());
        assertTrue("Column does not match",
                res.getColumn("C").typeCode() == df2.getColumn("C").asNullable().typeCode());
        assertTrue("Column does not match",
                res.getColumn("D").typeCode() == df1.getColumn("D").typeCode());
        assertTrue("Column does not match",
                res.getColumn("E").typeCode() == df1.getColumn("E").typeCode());
        
        assertTrue("DataFrame result does not match expected", res.count("A", "1") == 2);
        assertTrue("DataFrame result does not match expected", res.count("A", "2") == 1);
        assertTrue("DataFrame result does not match expected", res.count("A", "3") == 0);
        assertTrue("DataFrame result does not match expected", res.count("A", "4") == 1);
        assertTrue("DataFrame result does not match expected", res.count("A", "5") == 0);
        assertTrue("DataFrame result does not match expected", res.count("A", "6") == 1);
    }

    @Test
    public void testJoinNoKeySpecifiedNullableNullable(){
        DataFrame df1 = new NullableDataFrame(
                Column.nullable("D", 517,575,896,741,null,231),
                Column.nullable("A", 1, 2, null, 4, 1, 6),
                Column.nullable("E", "2018", "2019", null, "2020", "2018", null));

        DataFrame df2 = new NullableDataFrame(
                Column.nullable("B", "AAA","BBB","CCC","DDD","EEE","FFF"),
                Column.nullable("C", 53,51,54,62,41,54),
                Column.nullable("A", 1, null, 3, 4, 5, null));

        DataFrame res = df1.join(df2);
        assertTrue("DataFrame should be of type NullableDataFrame",
                res instanceof NullableDataFrame);

        assertTrue("DataFrame should have 5 columns", res.columns() == 5);
        assertTrue("DataFrame should have 5 rows", res.rows() == 5);
        assertArrayEquals("Column names do not match",
                new String[]{"D","A","E","B","C"}, res.getColumnNames());
        
        assertTrue("Column does not match",
                res.getColumn("A").typeCode() == df2.getColumn("A").typeCode());
        assertTrue("Column does not match",
                res.getColumn("B").typeCode() == df2.getColumn("B").typeCode());
        assertTrue("Column does not match",
                res.getColumn("C").typeCode() == df2.getColumn("C").typeCode());
        assertTrue("Column does not match",
                res.getColumn("D").typeCode() == df1.getColumn("D").typeCode());
        assertTrue("Column does not match",
                res.getColumn("E").typeCode() == df1.getColumn("E").typeCode());
        
        assertTrue("DataFrame result does not match expected", res.count("A", "1") == 2);
        assertTrue("DataFrame result does not match expected", res.count("A", "null") == 2);
        assertTrue("DataFrame result does not match expected", res.count("A", "3") == 0);
        assertTrue("DataFrame result does not match expected", res.count("A", "4") == 1);
        assertTrue("DataFrame result does not match expected", res.count("A", "5") == 0);
        assertTrue("DataFrame result does not match expected", res.count("A", "6") == 0);
    }

    @Test
    public void testJoinEmptyArg(){
        DataFrame df1 = new DefaultDataFrame(
                Column.create("A", 517,575),
                Column.create("B", 1, 2),
                Column.create("C", "2018", "2019"));
        
        DataFrame df2 = new NullableDataFrame(
                Column.nullable("D", "AAA","BBB","CCC"),
                Column.nullable("E", 53,51,54),
                Column.nullable("A", 1, 2, 3));

        DataFrame df3 = df2.clone();
        df3.clear();
        DataFrame res = df1.join(df3);
        assertTrue("DataFrame should be of type NullableDataFrame",
                res instanceof NullableDataFrame);

        assertTrue("DataFrame should have 5 columns", res.columns() == 5);
        assertTrue("DataFrame should have 0 rows", res.rows() == 0);
        assertArrayEquals("Column names do not match",
                new String[]{"A","B","C","D","E"}, res.getColumnNames());
        
        df1.clear();
        res = df2.join(df1);
        assertTrue("DataFrame should be of type NullableDataFrame",
                res instanceof NullableDataFrame);

        assertTrue("DataFrame should have 5 columns", res.columns() == 5);
        assertTrue("DataFrame should have 0 rows", res.rows() == 0);
        assertArrayEquals("Column names do not match",
                new String[]{"D","E","A","B","C"}, res.getColumnNames());
    }

    @Test(expected=DataFrameException.class)
    public void testJoinFailNoMatchingKey(){
        DataFrame df1 = new NullableDataFrame(
                Column.nullable("A", 517,575),
                Column.nullable("B", 1, 2),
                Column.nullable("C", "2018", "2019"));

        DataFrame df2 = new NullableDataFrame(
                Column.nullable("D", "AAA","BBB"),
                Column.nullable("E", 53,51),
                Column.nullable("F", 1, null));
        
        df1.join(df2);
    }

    @Test(expected=DataFrameException.class)
    public void testJoinFailMultipleKeys(){
        DataFrame df1 = new NullableDataFrame(
                Column.nullable("A", 517,575),
                Column.nullable("B", 1, 2),
                Column.nullable("C", "2018", "2019"));

        DataFrame df2 = new NullableDataFrame(
                Column.nullable("A", "AAA","BBB"),
                Column.nullable("E", 53,51),
                Column.nullable("B", 1, null));
        
        df1.join(df2);
    }

    @Test(expected=DataFrameException.class)
    public void testJoinFailNullArg(){
        DataFrame df1 = new NullableDataFrame(
                Column.nullable("A", 517,575),
                Column.nullable("B", 1, 2),
                Column.nullable("C", "2018", "2019"));

        df1.join(null);
    }

    @Test(expected=DataFrameException.class)
    public void testJoinFailInvalidColumnName(){
        DataFrame df1 = new NullableDataFrame(
                Column.nullable("A", 517,575),
                Column.nullable("B", 1, 2),
                Column.nullable("C", "2018", "2019"));

        DataFrame df2 = new NullableDataFrame(
                Column.nullable("A", "AAA","BBB"),
                Column.nullable("D", 53,51),
                Column.nullable("E", 1, null));
        
        df1.join(df2, "INVALID");
    }

    @Test(expected=DataFrameException.class)
    public void testJoinFailInvalidColumnNameSecondArg(){
        DataFrame df1 = new NullableDataFrame(
                Column.nullable("A1", 517,575),
                Column.nullable("B", 1, 2),
                Column.nullable("C", "2018", "2019"));

        DataFrame df2 = new NullableDataFrame(
                Column.nullable("A2", "AAA","BBB"),
                Column.nullable("D", 53,51),
                Column.nullable("E", 1, null));
        
        df1.join(df2, "A1", "INVALID");
    }

    @Test(expected=DataFrameException.class)
    public void testJoinFailEmptyFirstColumnName(){
        DataFrame df1 = new NullableDataFrame(
                Column.nullable("A1", 517,575),
                Column.nullable("B", 1, 2),
                Column.nullable("C", "2018", "2019"));

        DataFrame df2 = new NullableDataFrame(
                Column.nullable("A2", "AAA","BBB"),
                Column.nullable("D", 53,51),
                Column.nullable("E", 1, null));
        
        df1.join(df2, "", "A2");
    }

    @Test(expected=DataFrameException.class)
    public void testJoinFailEmptySecondColumnName(){
        DataFrame df1 = new NullableDataFrame(
                Column.nullable("A1", 517,575),
                Column.nullable("B", 1, 2),
                Column.nullable("C", "2018", "2019"));

        DataFrame df2 = new NullableDataFrame(
                Column.nullable("A2", "AAA","BBB"),
                Column.nullable("D", 53,51),
                Column.nullable("E", 1, null));
        
        df1.join(df2, "A1", "");
    }

    @Test(expected=DataFrameException.class)
    public void testJoinFailSelfReferential(){
        DataFrame df1 = new NullableDataFrame(
                Column.nullable("A1", 517,575),
                Column.nullable("B", 1, 2),
                Column.nullable("C", "2018", "2019"));

        df1.join(df1);
    }

    @Test
    public void testMerge(){
        DataFrame df1 = new DefaultDataFrame(
                new String[]{"c1","c2","c3"}, 
                new ByteColumn(new byte[]{
                        1,2,3
                }),
                new ShortColumn(new short[]{
                        1,2,3
                }),
                new IntColumn(new int[]{
                        1,2,3
                }));

        DataFrame df2 = new DefaultDataFrame(
                new String[]{"c4","c5","c6"}, 
                new CharColumn(new char[]{
                        'a','b','c'
                }),
                new FloatColumn(new float[]{
                        1f,2f,3f
                }),
                new DoubleColumn(new double[]{
                        1.0,2.0,3.0
                }));

        DataFrame res = DataFrame.merge(df1, df2);
        assertTrue("DataFrame should be of type DefaultDataFrame",
                res instanceof DefaultDataFrame);

        assertTrue("DataFrame should have 3 rows", res.rows() == 3);
        assertTrue("DataFrame should have 6 columns", res.columns() == 6);
        assertArrayEquals("Column names should match",
                new String[]{"c1","c2","c3","c4","c5","c6"},
                res.getColumnNames());

        assertTrue("Column references do not match",
                res.getColumn(0) == df1.getColumn(0));
        assertTrue("Column references do not match",
                res.getColumn(1) == df1.getColumn(1));
        assertTrue("Column references do not match",
                res.getColumn(2) == df1.getColumn(2));
        assertTrue("Column references do not match",
                res.getColumn(3) == df2.getColumn(0));
        assertTrue("Column references do not match",
                res.getColumn(4) == df2.getColumn(1));
        assertTrue("Column references do not match",
                res.getColumn(5) == df2.getColumn(2));
    }
    
    @Test
    public void testMergeDifferentTypes(){
        DataFrame df1 = new DefaultDataFrame(
                Column.create("A", "AAA","AAB","AAC"),
                Column.create("B", 11.11,22.22,33.33),
                Column.create("C", 'A','B','C'));
        
        DataFrame df2 = new DefaultDataFrame(
                Column.create("D", "BBA","BBB","BBC"),
                Column.create("E", 10,11,12));
        
        DataFrame df3 = new NullableDataFrame(
                Column.nullable("F", 0,1,2),
                Column.nullable("G", 0.1f,0.2f,0.3f));

        DataFrame res = DataFrame.merge(df1, df2, df3);
        assertTrue("DataFrame should be of type NullableDataFrame",
                res instanceof NullableDataFrame);

        assertTrue("DataFrame should have 3 rows", res.rows() == 3);
        assertTrue("DataFrame should have 7 columns", res.columns() == 7);
        assertArrayEquals("Column names should match",
                new String[]{"A","B","C","D","E","F","G"},
                res.getColumnNames());

        assertTrue("Column references do not match",
                res.getColumn(5) == df3.getColumn(0));
        assertTrue("Column references do not match",
                res.getColumn(6) == df3.getColumn(1));
    }

    @Test
    public void testMergeDuplicateNames(){
        DataFrame df1 = new DefaultDataFrame(
                Column.create("A", "AAA","AAB","AAC"),
                Column.create("B", 11.11,22.22,33.33),
                Column.create("C", 'A','B','C'));
        
        DataFrame df2 = new DefaultDataFrame(
                Column.create("D", "BBA","BBB","BBC"),
                Column.create("B", 10,11,12));
        
        DataFrame df3 = new NullableDataFrame(
                Column.nullable("B", 0,1,2),
                Column.nullable("D", 0.1f,0.2f,0.3f));

        DataFrame res = DataFrame.merge(df1, df2, df3);
        assertTrue("DataFrame should be of type NullableDataFrame",
                res instanceof NullableDataFrame);

        assertTrue("DataFrame should have 3 rows", res.rows() == 3);
        assertTrue("DataFrame should have 7 columns", res.columns() == 7);
        assertArrayEquals("Column names should match",
                new String[]{"A","B_0","C","D_0","B_1","B_2","D_1"},
                res.getColumnNames());

        assertTrue("Column references do not match",
                res.getColumn(5) == df3.getColumn(0));
        assertTrue("Column references do not match",
                res.getColumn(6) == df3.getColumn(1));
    }

    @Test
    public void testMergeOneArg(){
        DataFrame df1 = new DefaultDataFrame(
                Column.create("A", "AAA","AAB","AAC"),
                Column.create("B", 11.11,22.22,33.33),
                Column.create("C", 'A','B','C'));

        DataFrame res = DataFrame.merge(df1);
        assertTrue("DataFrame reference does not match", res == df1);
    }

    @Test(expected=DataFrameException.class)
    public void testMergeFailInvalidRowSize(){
        DataFrame df1 = new DefaultDataFrame(
                Column.nullable("A", "AAA","AAB","AAC"),
                Column.nullable("B", 11.11,22.22,33.33),
                Column.nullable("C", 'A','B','C'));
        
        DataFrame df2 = new DefaultDataFrame(
                Column.create("D", "BBA","BBB","BBC"),
                Column.create("B", 10,11,12));
        
        DataFrame df3 = new NullableDataFrame(
                Column.nullable("B", 0,1,2,3),
                Column.nullable("D", 0.1f,0.2f,0.3f,0.4f));

       DataFrame.merge(df1, df2, df3);
    }

    @Test(expected=DataFrameException.class)
    public void testMergeFailNullArg(){
        DataFrame df1 = new DefaultDataFrame(
                Column.nullable("A", "AAA","AAB","AAC"),
                Column.nullable("B", 11.11,22.22,33.33),
                Column.nullable("C", 'A','B','C'));
        
        DataFrame df2 = new NullableDataFrame(
                Column.nullable("B", 0,1,2,3),
                Column.nullable("D", 0.1f,0.2f,0.3f,0.4f));

       DataFrame.merge(df1, df2, null);
    }

    @Test
    public void testConvertFromDefaultToNullable(){
        DataFrame conv = DataFrame.convert(df, NullableDataFrame.class);
        assertTrue("DataFrame should be of type NullableDataFrame",
                conv instanceof NullableDataFrame);

        assertTrue("DataFrame should have 3 rows", conv.rows() == 3);
        assertTrue("DataFrame should have 10 columns", conv.columns() == 10);
        assertArrayEquals("Column names should match",
                columnNames, conv.getColumnNames());

        assertArrayEquals("Rows do not match", df.getRow(0), conv.getRow(0));
        assertArrayEquals("Rows do not match", df.getRow(1), conv.getRow(1));
        assertArrayEquals("Rows do not match", df.getRow(2), conv.getRow(2));
    }

    @Test
    public void testConvertFromNullableToDefault(){
        DataFrame conv = DataFrame.convert(nulldf, DefaultDataFrame.class);
        assertTrue("DataFrame should be of type DefaultDataFrame",
                conv instanceof DefaultDataFrame);

        assertTrue("DataFrame should have 3 rows", conv.rows() == 3);
        assertTrue("DataFrame should have 10 columns", conv.columns() == 10);
        assertArrayEquals("Column names should match",
                columnNames, conv.getColumnNames());

        //check against null
        Object[][] o = conv.toArray();
        for(int i=0; i<o.length; ++i){
            for(int j=0; j<o[i].length; ++j){
                assertNotNull(
                        "Converted DataFrame should not contain any null values",
                        o[i][j]);
            }
        }
    }
}
