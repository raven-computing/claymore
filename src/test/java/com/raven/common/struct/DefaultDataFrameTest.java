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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Tests for DefaultDataFrame implementation.
 *
 */
public class DefaultDataFrameTest {

    String[] columnNames;
    DefaultDataFrame df;
    //DataFrame for sorting tests
    DefaultDataFrame toBeSorted;

    @BeforeClass
    public static void setUpBeforeClass(){ }

    @AfterClass
    public static void tearDownAfterClass(){ }

    @Before
    public void setUp(){

        columnNames = new String[]{
                "byteCol",   // 0
                "shortCol",  // 1
                "intCol",    // 2
                "longCol",   // 3
                "stringCol", // 4
                "charCol",   // 5
                "floatCol",  // 6
                "doubleCol", // 7
                "booleanCol" // 8
        };

        df = new DefaultDataFrame(
                columnNames, 
                new ByteColumn(new byte[]{
                        10,20,30,40,50
                }),
                new ShortColumn(new short[]{
                        11,21,31,41,51
                }),
                new IntColumn(new int[]{
                        12,22,32,42,52
                }),
                new LongColumn(new long[]{
                        13l,23l,33l,43l,53l
                }),
                new StringColumn(new String[]{
                        "10","20","30","40","50"
                }),
                new CharColumn(new char[]{
                        'a','b','c','d','e'
                }),
                new FloatColumn(new float[]{
                        10.1f,20.2f,30.3f,40.4f,50.5f
                }),
                new DoubleColumn(new double[]{
                        11.1,21.2,31.3,41.4,51.5
                }),
                new BooleanColumn(new boolean[]{
                        true,false,true,false,true
                }));

        toBeSorted = new DefaultDataFrame(
                columnNames, 
                new ByteColumn(new byte[]{
                        4,2,1,5,3
                }),
                new ShortColumn(new short[]{
                        4,2,1,5,3
                }),
                new IntColumn(new int[]{
                        4,2,1,5,3
                }),
                new LongColumn(new long[]{
                        4l,2l,1l,5l,3l
                }),
                new StringColumn(new String[]{
                        "4","2","1","5","3"
                }),
                new CharColumn(new char[]{
                        'd','b','a','e','c'
                }),
                new FloatColumn(new float[]{
                        4.0f,2.0f,1.0f,5.0f,3.0f
                }),
                new DoubleColumn(new double[]{
                        4.0,2.0,1.0,5.0,3.0
                }),
                new BooleanColumn(new boolean[]{
                        true,false,true,false,true
                }));
    }

    @After
    public void tearDown(){ }

    //**************************//
    //        Constructors      //
    //**************************//

    @Test
    public void testConstructorNoArgs(){
        DefaultDataFrame test = new DefaultDataFrame();
        assertTrue("DefaultDataFrame should be empty", test.isEmpty());
        assertTrue("DefaultDataFrame row count should be 0", test.rows() == 0);
        assertTrue("DefaultDataFrame column count should be 0", test.columns() == 0);
        assertFalse("DefaultDataFrame should not have column names set", test.hasColumnNames());
    }

    @Test
    public void testConstructorWithColumns(){
        DefaultDataFrame test = new DefaultDataFrame(
                new IntColumn(new int[]{1,2,3}),
                new StringColumn(new String[]{"1","2","3"}),
                new ByteColumn(new byte[]{1,2,3}));

        assertFalse("DefaultDataFrame should not be empty", test.isEmpty());
        assertTrue("DefaultDataFrame row count should be 3", test.rows() == 3);
        assertTrue("DefaultDataFrame column count should be 3", test.columns() == 3);
        assertFalse("DefaultDataFrame should not have column names set", test.hasColumnNames());
    }

    @Test
    public void testConstructorWithLabeledColumns(){
        String[] names = new String[]{"myInt","myString","myByte"};
        DefaultDataFrame test = new DefaultDataFrame(
                new IntColumn(names[0], new int[]{1,2,3}),
                new StringColumn(names[1], new String[]{"1","2","3"}),
                new ByteColumn(names[2], new byte[]{1,2,3}));

        assertFalse("DefaultDataFrame should not be empty", test.isEmpty());
        assertTrue("DefaultDataFrame row count should be 3", test.rows() == 3);
        assertTrue("DefaultDataFrame column count should be 3", test.columns() == 3);
        assertTrue("DefaultDataFrame should have column names set", test.hasColumnNames());
        assertArrayEquals("DefaultDataFrame column names do not match", names, test.getColumnNames());
    }

    @Test
    public void testConstructorWithColumnsAndNames(){
        DefaultDataFrame test = new DefaultDataFrame(
                new String[]{"col1","col2","col3"},
                new IntColumn(new int[]{1,2,3}),
                new StringColumn(new String[]{"1","2","3"}),
                new ByteColumn(new byte[]{1,2,3}));

        assertFalse("DefaultDataFrame should not be empty", test.isEmpty());
        assertTrue("DefaultDataFrame row count should be 3", test.rows() == 3);
        assertTrue("DefaultDataFrame column count should be 3", test.columns() == 3);
        assertTrue("DefaultDataFrame should have column names set", test.hasColumnNames());
    }

    @Test
    public void testConstructorWithLabeledColumnsOverridden(){
        String[] names = new String[]{"col1","col2","col3"};
        DefaultDataFrame test = new DefaultDataFrame(
                names,
                new IntColumn("myInt", new int[]{1,2,3}),
                new StringColumn("myString", new String[]{"1","2","3"}),
                new ByteColumn("myByte", new byte[]{1,2,3}));

        assertFalse("DefaultDataFrame should not be empty", test.isEmpty());
        assertTrue("DefaultDataFrame row count should be 3", test.rows() == 3);
        assertTrue("DefaultDataFrame column count should be 3", test.columns() == 3);
        assertTrue("DefaultDataFrame should have column names set", test.hasColumnNames());
        assertArrayEquals("DefaultDataFrame column names do not match", names, test.getColumnNames());
    }

    @Test
    public void testConstructorWithAnnotatedRow(){
        //the names of row items in the RowDummyDefault class
        String[] nTruth = new String[]{"BYTE","SHORT","INT","LONG","STRING","CHAR","FLOAT","DOUBLE","BOOLEAN"};
        DefaultDataFrame test = new DefaultDataFrame(RowDummyDefault.class);

        assertTrue("DefaultDataFrame should be empty", test.isEmpty());
        assertTrue("DefaultDataFrame row count should be 0", test.rows() == 0);
        assertTrue("DefaultDataFrame column count should be 9", test.columns() == 9);
        assertTrue("DefaultDataFrame should have column names set", test.hasColumnNames());
        String[] names = test.getColumnNames();
        assertTrue("Column names should be 9", names.length == 9);
        for(String s1 : nTruth){
            boolean found = false;
            for(String s2 : names){
                if(s1.equals(s2)){
                    found = true;
                    break;
                }
            }
            assertTrue(String.format("Column with name %s was not found", s1), found);
        }

        Column c = null;
        c = test.getColumn("BYTE");
        assertTrue("Column should be of type ByteColumn", c instanceof ByteColumn);
        c = test.getColumn("SHORT");
        assertTrue("Column should be of type ShortColumn", c instanceof ShortColumn);
        c = test.getColumn("INT");
        assertTrue("Column should be of type IntColumn", c instanceof IntColumn);
        c = test.getColumn("LONG");
        assertTrue("Column should be of type LongColumn", c instanceof LongColumn);
        c = test.getColumn("STRING");
        assertTrue("Column should be of type StringColumn", c instanceof StringColumn);
        c = test.getColumn("CHAR");
        assertTrue("Column should be of type CharColumn", c instanceof CharColumn);
        c = test.getColumn("FLOAT");
        assertTrue("Column should be of type FloatColumn", c instanceof FloatColumn);
        c = test.getColumn("DOUBLE");
        assertTrue("Column should be of type DoubleColumn", c instanceof DoubleColumn);
        c = test.getColumn("BOOLEAN");
        assertTrue("Column should be of type BooleanColumn", c instanceof BooleanColumn);
    }

    //**********************//
    //        Getters       //
    //**********************//

    @Test
    public void testGetByteByIndex(){
        assertTrue("Byte at index 2 should be 30", df.getByte(0, 2) == 30);
    }

    @Test
    public void testGetByteByName(){
        assertTrue("Byte at index 2 should be 30", df.getByte("byteCol", 2) == 30);
    }

    @Test
    public void testGetShortByIndex(){
        assertTrue("Short at index 3 should be 41", df.getShort(1, 3) == 41);
    }

    @Test
    public void testGetShortByName(){
        assertTrue("Short at index 3 should be 41", df.getShort("shortCol", 3) == 41);
    }

    @Test
    public void testGetIntByIndex(){
        assertTrue("Int at index 1 should be 22", df.getInt(2, 1) == 22);
    }

    @Test
    public void testGetIntByName(){
        assertTrue("Int at index 1 should be 22", df.getInt("intCol", 1) == 22);
    }

    @Test
    public void testGetLongByIndex(){
        assertTrue("Long at index 4 should be 53", df.getLong(3, 4) == 53);
    }

    @Test
    public void testGetLongByName(){
        assertTrue("Long at index 4 should be 53", df.getLong("longCol", 4) == 53);
    }

    @Test
    public void testGetStringByIndex(){
        assertEquals("String at index 0 should be \"10\"", "10", df.getString(4, 0));
    }

    @Test
    public void testGetStringByName(){
        assertEquals("String at index 0 should be \"10\"", "10", df.getString("stringCol", 0));
    }

    @Test
    public void testGetCharByIndex(){
        assertTrue("Char at index 2 should be \'c\'", df.getChar(5, 2) == 'c');
    }

    @Test
    public void testGetCharByName(){
        assertTrue("Char at index 2 should be \'c\'", df.getChar("charCol", 2) == 'c');
    }

    @Test
    public void testGetFloatByIndex(){
        assertTrue("Float at index 1 should be 20.2", df.getFloat(6, 1) == 20.2f);
    }

    @Test
    public void testGetFloatByName(){
        assertTrue("Float at index 1 should be 20.2", df.getFloat("floatCol", 1) == 20.2f);
    }

    @Test
    public void testGetDoubleByIndex(){
        assertTrue("Double at index 4 should be 51.5", df.getDouble(7, 4) == 51.5);
    }

    @Test
    public void testGetDoubleByName(){
        assertTrue("Double at index 4 should be 51.5", df.getDouble("doubleCol", 4) == 51.5);
    }

    @Test
    public void testGetBooleanByIndex(){
        assertFalse("Boolean at index 1 should be false", df.getBoolean(8, 1));
    }

    @Test
    public void testGetBooleanByName(){
        assertFalse("Boolean at index 1 should be false", df.getBoolean("booleanCol", 1));
    }

    @Test
    public void testGetNumberByIndex(){
        assertTrue("Number at index 0 should be 13", df.getNumber(3, 0).longValue() == 13l);
    }

    @Test
    public void testGetNumberByName(){
        assertTrue("Number at index 4 should be 53", df.getNumber("longCol", 4).longValue() == 53l);
    }

    //**********************//
    //        Setters       //
    //**********************//

    @Test
    public void testSetByteByIndex(){
        df.setByte(0, 2, (byte)35);
        assertTrue("Byte at index 2 should be set to 35", df.getByte(0, 2) == 35);
    }

    @Test
    public void testSetByteByName(){
        df.setByte("byteCol", 2, (byte)35);
        assertTrue("Byte at index 2 should be set to 35", df.getByte("byteCol", 2) == 35);
    }

    @Test
    public void testSetShortByIndex(){
        df.setShort(1, 3, (short)11);
        assertTrue("Short at index 3 should be set to 11", df.getShort(1, 3) == 11);
    }

    @Test
    public void testSetShortByName(){
        df.setShort("shortCol", 3, (short)11);
        assertTrue("Short at index 3 should be set to 11", df.getShort("shortCol", 3) == 11);
    }

    @Test
    public void testSetIntByIndex(){
        df.setInt(2, 1, 11);
        assertTrue("Int at index 1 should be set to 11", df.getInt(2, 1) == 11);
    }

    @Test
    public void testSetIntByName(){
        df.setInt("intCol", 1, 11);
        assertTrue("Int at index 1 should be set to 11", df.getInt("intCol", 1) == 11);
    }

    @Test
    public void testSetLongByIndex(){
        df.setLong(3, 4, 11l);
        assertTrue("Long at index 4 should be set to 11", df.getLong(3, 4) == 11l);
    }

    @Test
    public void testSetLongByName(){
        df.setLong("longCol", 4, 11l);
        assertTrue("Long at index 4 should be set to 11", df.getLong("longCol", 4) == 11l);
    }

    @Test
    public void testSetStringByIndex(){
        df.setString(4, 0, "coffee");
        assertEquals("String at index 0 should be set to \"coffee\"", "coffee",
                df.getString(4, 0));
    }

    @Test
    public void testSetStringByName(){
        df.setString("stringCol", 0, "coffee");
        assertEquals("String at index 0 should be set to \"coffee\"", 
                "coffee", 
                df.getString("stringCol", 0));
    }

    @Test
    public void testSetCharByIndex(){
        df.setChar(5, 2, 'T');
        assertTrue("Char at index 2 should be set to \'T\'", df.getChar(5, 2) == 'T');
    }

    @Test
    public void testSetCharByName(){
        df.setChar("charCol", 2, 'T');
        assertTrue("Char at index 2 should be set to \'T\'", df.getChar("charCol", 2) == 'T');
    }
    
    @Test(expected=DataFrameException.class)
    public void testSetInvalidChar(){
        df.setChar("charCol", 2, '€');
    }

    @Test
    public void testSetFloatByIndex(){
        df.setFloat(6, 1, 11.2f);
        assertTrue("Float at index 1 should be set to 11.2", df.getFloat(6, 1) == 11.2f);
    }

    @Test
    public void testSetFloatByName(){
        df.setFloat("floatCol", 1, 11.2f);
        assertTrue("Float at index 1 should be set to 11.2", df.getFloat("floatCol", 1) == 11.2f);
    }

    @Test
    public void testSetDoubleByIndex(){
        df.setDouble(7, 4, 11.3);
        assertTrue("Double at index 4 should be set to 11.3", df.getDouble(7, 4) == 11.3);
    }

    @Test
    public void testSetDoubleByName(){
        df.setDouble("doubleCol", 4, 11.3);
        assertTrue("Double at index 4 should be set to 11.3", df.getDouble("doubleCol", 4) == 11.3);
    }

    @Test
    public void testSetBooleanByIndex(){
        df.setBoolean(8, 1, true);
        assertTrue("Boolean at index 1 should be set to true", df.getBoolean(8, 1) == true);
    }

    @Test
    public void testSetBooleanByName(){
        df.setBoolean("booleanCol", 1, true);
        assertTrue("Boolean at index 1 should be set to true", 
                df.getBoolean("booleanCol", 1) == true);
    }
    
    @Test
    public void testSetNumberByIndex(){
        df.setNumber(3, 1, 456l);
        assertTrue("Number at index 1 should be set to 456", df.getNumber(3, 1).longValue() == 456l);
    }

    @Test
    public void testSetNumberByName(){
        df.setNumber("longCol", 1, 456l);
        assertTrue("Number at index 1 should be set to 456", df.getNumber(3, 1).longValue() == 456l);
    }

    //**********************************//
    //     Column names and indices     //
    //**********************************//

    @Test
    public void testGetColumnNames(){
        String[] names = df.getColumnNames();
        assertTrue("Array of column names should have length 9", names.length == 9);
        assertArrayEquals("Column names do not match array content", columnNames, names);
    }

    @Test
    public void testGetColumnName(){
        assertEquals("Column name for column at index 3 does not equal \"longCol\"",
                "longCol",
                df.getColumnName(3));

        assertEquals("Column name for column at index 3 does not equal \"longCol\"",
                "longCol",
                df.getColumn(3).getName());
    }

    @Test
    public void testGetColumnindex(){
        assertTrue("Column \"longCol\" is not at index 3", df.getColumnIndex("longCol") == 3);
    }

    @Test
    public void testSetColumnNames(){
        String[] names = new String[]{"A","B","C","D","E","F","G","H","I"};
        df.setColumnNames(names);
        assertArrayEquals("Column names do not match set names", 
                names, df.getColumnNames());

        assertTrue("Test-DataFrame should have column names set", df.hasColumnNames());
        int i = 0;
        for(Column col : df){
            assertEquals("Column name does not match",
                    names[i++], col.getName());
        }
    }

    @Test
    public void testSetColumnName(){
        df.setColumnName(3, "NEW_NAME");
        assertEquals("Column name does not match set name \"NEW_NAME\"",
                "NEW_NAME", df.getColumnName(3));

        assertEquals("Column name does not match set name \"NEW_NAME\"",
                "NEW_NAME", df.getColumn(3).getName());

        assertTrue("Test-DataFrame should have column names set", df.hasColumnNames());
    }
    
    @Test
    public void testRenameColumn(){
        df.setColumnName("longCol", "NEW_NAME");
        assertEquals("Column name does not match set name \"NEW_NAME\"",
                "NEW_NAME", df.getColumnName(df.getColumnIndex("NEW_NAME")));

        assertEquals("Column name does not match set name \"NEW_NAME\"",
                "NEW_NAME", df.getColumn(df.getColumnIndex("NEW_NAME")).getName());

        assertTrue("Test-DataFrame should have column names set", df.hasColumnNames());
    }

    @Test
    public void testRemoveColumnNames(){
        df.removeColumnNames();
        assertFalse("Test-DataFrame should not have column names set",
                df.hasColumnNames());

        for(Column col : df){
            assertNull("Column should not have a name set", col.getName());
        }
    }

    @Test
    public void testHasColumnNames(){
        DefaultDataFrame d = new DefaultDataFrame();
        assertFalse("Empty DataFrame should not have column names set", 
                d.hasColumnNames());

        assertTrue("Test-DataFrame should have column names set", 
                df.hasColumnNames());

        df.removeColumnNames();
        assertFalse("Test-DataFrame should not have column names set after removal", 
                df.hasColumnNames());
    }

    //***************************//
    //           Rows            //
    //***************************//

    @Test
    public void testGetRow(){
        Object[] row = df.getRow(1);
        assertArrayEquals("Row does not match set values", 
                new Object[]{(byte)20,(short)21,22,23l,"20",'b',20.2f,21.2d,false}, 
                row);
    }
    
    @Test
    public void testGetRows(){
        DataFrame res = df.getRows(1, 3);
        assertTrue("DataFrame should have 2 rows", res.rows() == 2);
        assertTrue("DataFrame should have 9 columns", res.columns() == 9);
        assertArrayEquals("Row does not match selected values", 
                new Object[]{(byte)20,(short)21,22,23l,"20",'b',20.2f,21.2d,false}, 
                res.getRow(0));
        
        assertArrayEquals("Row does not match selected values", 
                new Object[]{(byte)30,(short)31,32,33l,"30",'c',30.3f,31.3d,true}, 
                res.getRow(1));
    }

    @Test
    public void testSetRow(){
        Object[] row = new Object[]{(byte)42,(short)42,42,42l,"42",'A',42.2f,42.2d,true};
        df.setRow(1, row);
        assertArrayEquals("Row does not match set values", 
                new Object[]{(byte)42,(short)42,42,42l,"42",'A',42.2f,42.2d,true}, 
                row);
    }

    @Test
    public void testAddRow(){
        df.addRow(new Object[]{(byte)42,(short)42,42,42l,"42",'A',42.2f,42.2d,true});
        assertTrue("Row count should be 6", df.rows() == 6);
        Object[] row = df.getRow(5);
        assertArrayEquals("Row does not match added values", 
                new Object[]{(byte)42,(short)42,42,42l,"42",'A',42.2f,42.2d,true}, 
                row);
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testAddRowInvalidChar(){
        df.addRow(new Object[]{(byte)42,(short)42,42,42l,"42",'€',42.2f,42.2d,true});
    }

    @Test
    public void testInsertRow(){
        df.insertRow(2, new Object[]{(byte)42,(short)42,42,42l,"42",'A',42.2f,42.2d,true});
        assertTrue("Row count should be 6", df.rows() == 6);
        Object[] row = df.getRow(2);
        assertArrayEquals("Row does not match inserted values", 
                new Object[]{(byte)42,(short)42,42,42l,"42",'A',42.2f,42.2d,true}, 
                row);
    }

    @Test
    public void testInsertRowZero(){
        df.insertRow(0, new Object[]{(byte)42,(short)42,42,42l,"42",'A',42.2f,42.2d,true});
        assertTrue("Row count should be 6", df.rows() == 6);
        Object[] row = df.getRow(0);
        assertArrayEquals("Row does not match inserted values", 
                new Object[]{(byte)42,(short)42,42,42l,"42",'A',42.2f,42.2d,true}, 
                row);
    }

    @Test
    public void testInsertRowEnd(){
        df.insertRow(5, new Object[]{(byte)42,(short)42,42,42l,"42",'A',42.2f,42.2d,true});
        assertTrue("Row count should be 6", df.rows() == 6);
        Object[] row = df.getRow(5);
        assertArrayEquals("Row does not match inserted values", 
                new Object[]{(byte)42,(short)42,42,42l,"42",'A',42.2f,42.2d,true}, 
                row);
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testInsertRowInvalidChar(){
        df.insertRow(3, new Object[]{(byte)42,(short)42,42,42l,"42",'€',42.2f,42.2d,true});
    }

    @Test
    public void testRemoveRow(){
        df.removeRow(1);
        assertTrue("Row count should be 4", df.rows() == 4);
        Object[] row = df.getRow(1);
        assertArrayEquals("Row does not match expected values", 
                new Object[]{(byte)30,(short)31,32,33l,"30",'c',30.3f,31.3d,true}, 
                row);
    }

    @Test
    public void testRemoveRows(){
        df.removeRows(1, 3);
        assertTrue("Row count should be 3", df.rows() == 3);
        Object[] row = df.getRow(1);
        assertArrayEquals("Row does not match expected values after removal point", 
                new Object[]{(byte)40,(short)41,42,43l,"40",'d',40.4f,41.4d,false}, 
                row);

        row = df.getRow(0);
        assertArrayEquals("Row does not match expected values before removal point", 
                new Object[]{(byte)10,(short)11,12,13l,"10",'a',10.1f,11.1d,true}, 
                row);

    }
    
    @Test
    public void testRemoveRowsRegexMatch(){
        int removed = df.removeRows(2, "(1|3)2");
        assertTrue("Remove count should be 2", removed == 2);
        assertTrue("Row count should be 3", df.rows() == 3);
        removed = df.removeRows(8, "false");
        assertTrue("Remove count should be 2", removed == 2);
        assertTrue("Row count should be 1", df.rows() == 1);
        Object[] row = df.getRow(0);
        assertArrayEquals("Row does not match remaining values", 
                new Object[]{(byte)50,(short)51,52,53l,"50",'e',50.5f,51.5d,true},
                row);
    }
    
    @Test
    public void testRemoveRowsRegexMatchByName(){
        int removed = df.removeRows("intCol", "(1|3)2");
        assertTrue("Remove count should be 2", removed == 2);
        assertTrue("Row count should be 3", df.rows() == 3);
        removed = df.removeRows("booleanCol", "false");
        assertTrue("Remove count should be 2", removed == 2);
        assertTrue("Row count should be 1", df.rows() == 1);
        Object[] row = df.getRow(0);
        assertArrayEquals("Row does not match remaining values", 
                new Object[]{(byte)50,(short)51,52,53l,"50",'e',50.5f,51.5d,true},
                row);
    }

    @Test
    public void testRemoveRowsNullRegexMatch(){
        int r = df.removeRows("intCol", null);
        assertTrue("Return value should be zero", r == 0);
        assertTrue("Returned DataFrame should have 5 rows", df.rows() == 5);
        assertTrue("Returned DataFrame should have 9 columns", df.columns() == 9);
        assertTrue("Value should be 12", df.getInt("intCol", 0) == 12);
        assertTrue("Value should be 32", df.getInt("intCol", 1) == 22);
        assertTrue("Value should be 52", df.getInt("intCol", 2) == 32);
        assertTrue("Value should be 52", df.getInt("intCol", 3) == 42);
        assertTrue("Value should be 52", df.getInt("intCol", 4) == 52);
    }

    @Test
    public void testGetRowAnnotated(){
        DefaultDataFrame test = new DefaultDataFrame(
                new String[]{"BYTE","SHORT","INT","LONG","STRING"
                        ,"CHAR","FLOAT","DOUBLE","BOOLEAN"},
                new ByteColumn(new byte[]{1,2,3}),
                new ShortColumn(new short[]{1,2,3}),
                new IntColumn(new int[]{1,2,3}),
                new LongColumn(new long[]{1,2,3}),
                new StringColumn(new String[]{"1","2","3"}),
                new CharColumn(new char[]{'1','2','3'}),
                new FloatColumn(new float[]{1f,2f,3f}),
                new DoubleColumn(new double[]{1,2,3}),
                new BooleanColumn(new boolean[]{true,false,true}));

        RowDummyDefault dummy = test.getRow(0, RowDummyDefault.class);
        assertTrue("Row does not match expected data", dummy.getmByte() == 1);
        assertTrue("Row does not match expected data", dummy.getmShort() == 1);
        assertTrue("Row does not match expected data", dummy.getmInt() == 1);
        assertTrue("Row does not match expected data", dummy.getmLong() == 1);
        assertEquals("Row does not match expected data", dummy.getmString(), "1");
        assertTrue("Row does not match expected data", dummy.getmFloat() == 1.0f);
        assertTrue("Row does not match expected data", dummy.getmDouble() == 1.0d);
        assertTrue("Row does not match expected data", dummy.getmChar() == '1');
        assertTrue("Row does not match expected data", dummy.ismBoolean());
        dummy = test.getRow(1, RowDummyDefault.class);
        assertTrue("Row does not match expected data", dummy.getmByte() == 2);
        assertTrue("Row does not match expected data", dummy.getmShort() == 2);
        assertTrue("Row does not match expected data", dummy.getmInt() == 2);
        assertTrue("Row does not match expected data", dummy.getmLong() == 2);
        assertEquals("Row does not match expected data", dummy.getmString(), "2");
        assertTrue("Row does not match expected data", dummy.getmFloat() == 2.0f);
        assertTrue("Row does not match expected data", dummy.getmDouble() == 2.0d);
        assertTrue("Row does not match expected data", dummy.getmChar() == '2');
        assertFalse("Row does not match expected data", dummy.ismBoolean());
        dummy = test.getRow(2, RowDummyDefault.class);
        assertTrue("Row does not match expected data", dummy.getmByte() == 3);
        assertTrue("Row does not match expected data", dummy.getmShort() == 3);
        assertTrue("Row does not match expected data", dummy.getmInt() == 3);
        assertTrue("Row does not match expected data", dummy.getmLong() == 3);
        assertEquals("Row does not match expected data", dummy.getmString(), "3");
        assertTrue("Row does not match expected data", dummy.getmFloat() == 3.0f);
        assertTrue("Row does not match expected data", dummy.getmDouble() == 3.0d);
        assertTrue("Row does not match expected data", dummy.getmChar() == '3');
        assertTrue("Row does not match expected data", dummy.ismBoolean());
    }

    @Test
    public void testSetRowAnnotated(){
        DefaultDataFrame test = new DefaultDataFrame(
                new String[]{"BYTE","SHORT","INT","LONG","STRING"
                        ,"CHAR","FLOAT","DOUBLE","BOOLEAN"},
                new ByteColumn(new byte[]{1,2,3}),
                new ShortColumn(new short[]{1,2,3}),
                new IntColumn(new int[]{1,2,3}),
                new LongColumn(new long[]{1,2,3}),
                new StringColumn(new String[]{"1","2","3"}),
                new CharColumn(new char[]{'1','2','3'}),
                new FloatColumn(new float[]{1f,2f,3f}),
                new DoubleColumn(new double[]{1,2,3}),
                new BooleanColumn(new boolean[]{true,false,true}));

        RowDummyDefault dummy = new RowDummyDefault((byte)9, (short)9, 9, 9l, "TEST", 'T', 9.0f, 9.0, true);
        test.setRow(1, dummy);
        assertTrue("Row does not match expected data", test.getByte(0, 1) == 9);
        assertTrue("Row does not match expected data", test.getShort(1, 1) == 9);
        assertTrue("Row does not match expected data", test.getInt(2, 1) == 9);
        assertTrue("Row does not match expected data", test.getLong(3, 1) == 9l);
        assertEquals("Row does not match expected data", test.getString(4, 1), "TEST");
        assertTrue("Row does not match expected data", test.getChar(5, 1) == 'T');
        assertTrue("Row does not match expected data", test.getFloat(6, 1) == 9.0f);
        assertTrue("Row does not match expected data", test.getDouble(7, 1) == 9.0d);
        assertTrue("Row does not match expected data", test.getBoolean(8, 1));
    }

    @Test
    public void testAddRowAnnotated(){
        DefaultDataFrame test = new DefaultDataFrame(
                new String[]{"BYTE","SHORT","INT","LONG","STRING"
                        ,"CHAR","FLOAT","DOUBLE","BOOLEAN"},
                new ByteColumn(new byte[]{1,2,3}),
                new ShortColumn(new short[]{1,2,3}),
                new IntColumn(new int[]{1,2,3}),
                new LongColumn(new long[]{1,2,3}),
                new StringColumn(new String[]{"1","2","3"}),
                new CharColumn(new char[]{'1','2','3'}),
                new FloatColumn(new float[]{1f,2f,3f}),
                new DoubleColumn(new double[]{1,2,3}),
                new BooleanColumn(new boolean[]{true,false,true}));

        RowDummyDefault dummy = new RowDummyDefault((byte)9, (short)9, 9, 9l, "TEST", 'T', 9.0f, 9.0, true);
        test.addRow(dummy);
        assertFalse("DefaultDataFrame should not be empty", test.isEmpty());
        assertTrue("DefaultDataFrame row count should be 4", test.rows() == 4);
        assertTrue("DefaultDataFrame column count should be 9", test.columns() == 9);

        assertTrue("Row does not match expected data", test.getByte(0, 3) == 9);
        assertTrue("Row does not match expected data", test.getShort(1, 3) == 9);
        assertTrue("Row does not match expected data", test.getInt(2, 3) == 9);
        assertTrue("Row does not match expected data", test.getLong(3, 3) == 9l);
        assertEquals("Row does not match expected data", test.getString(4, 3), "TEST");
        assertTrue("Row does not match expected data", test.getChar(5, 3) == 'T');
        assertTrue("Row does not match expected data", test.getFloat(6, 3) == 9.0f);
        assertTrue("Row does not match expected data", test.getDouble(7, 3) == 9.0d);
        assertTrue("Row does not match expected data", test.getBoolean(8, 3));

    }

    @Test
    public void testInsertRowAnnotated(){
        DefaultDataFrame test = new DefaultDataFrame(
                new String[]{"BYTE","SHORT","INT","LONG","STRING"
                        ,"CHAR","FLOAT","DOUBLE","BOOLEAN"},
                new ByteColumn(new byte[]{1,2,3}),
                new ShortColumn(new short[]{1,2,3}),
                new IntColumn(new int[]{1,2,3}),
                new LongColumn(new long[]{1,2,3}),
                new StringColumn(new String[]{"1","2","3"}),
                new CharColumn(new char[]{'1','2','3'}),
                new FloatColumn(new float[]{1f,2f,3f}),
                new DoubleColumn(new double[]{1,2,3}),
                new BooleanColumn(new boolean[]{true,false,true}));

        RowDummyDefault dummy = new RowDummyDefault((byte)9, (short)9, 9, 9l, "TEST", 'T', 9.0f, 9.0, true);
        test.insertRow(1, dummy);
        assertFalse("DefaultDataFrame should not be empty", test.isEmpty());
        assertTrue("DefaultDataFrame row count should be 4", test.rows() == 4);
        assertTrue("DefaultDataFrame column count should be 9", test.columns() == 9);

        //test inserted row
        assertTrue("Row does not match expected data", test.getByte(0, 1) == 9);
        assertTrue("Row does not match expected data", test.getShort(1, 1) == 9);
        assertTrue("Row does not match expected data", test.getInt(2, 1) == 9);
        assertTrue("Row does not match expected data", test.getLong(3, 1) == 9l);
        assertEquals("Row does not match expected data", test.getString(4, 1), "TEST");
        assertTrue("Row does not match expected data", test.getChar(5, 1) == 'T');
        assertTrue("Row does not match expected data", test.getFloat(6, 1) == 9.0f);
        assertTrue("Row does not match expected data", test.getDouble(7, 1) == 9.0d);
        assertTrue("Row does not match expected data", test.getBoolean(8, 1));

        //test deferred row
        assertTrue("Row does not match expected data", test.getByte(0, 2) == 2);
        assertTrue("Row does not match expected data", test.getShort(1, 2) == 2);
        assertTrue("Row does not match expected data", test.getInt(2, 2) == 2);
        assertTrue("Row does not match expected data", test.getLong(3, 2) == 2l);
        assertEquals("Row does not match expected data", test.getString(4, 2), "2");
        assertTrue("Row does not match expected data", test.getChar(5, 2) == '2');
        assertTrue("Row does not match expected data", test.getFloat(6, 2) == 2.0f);
        assertTrue("Row does not match expected data", test.getDouble(7, 2) == 2.0d);
        assertFalse("Row does not match expected data", test.getBoolean(8, 2));

        //test row at index zero. Should not have moved
        assertTrue("Row does not match expected data", test.getByte(0, 0) == 1);
        assertTrue("Row does not match expected data", test.getShort(1, 0) == 1);
        assertTrue("Row does not match expected data", test.getInt(2, 0) == 1);
        assertTrue("Row does not match expected data", test.getLong(3, 0) == 1l);
        assertEquals("Row does not match expected data", test.getString(4, 0), "1");
        assertTrue("Row does not match expected data", test.getChar(5, 0) == '1');
        assertTrue("Row does not match expected data", test.getFloat(6, 0) == 1.0f);
        assertTrue("Row does not match expected data", test.getDouble(7, 0) == 1.0d);
        assertTrue("Row does not match expected data", test.getBoolean(8, 0));

    }
    
    @Test
    public void testAddRows(){
        DataFrame df2 = new DefaultDataFrame(
                columnNames,
                new ByteColumn(new byte[]{11,22}),
                new ShortColumn(new short[]{11,22}),
                new IntColumn(new int[]{11,22}),
                new LongColumn(new long[]{11l,22l}),
                new StringColumn(new String[]{"11","22"}),
                new CharColumn(new char[]{'A','B'}),
                new FloatColumn(new float[]{11.1f,22.2f}),
                new DoubleColumn(new double[]{11.1,22.2}),
                new BooleanColumn(new boolean[]{true,false}));

        df.addRows(df2);
        assertTrue("DataFrame should have 7 rows", df.rows() == 7);
        assertTrue("DataFrame should have 9 columns", df.columns() == 9);
        assertArrayEquals("Rows do not match", df.getRow(5), df2.getRow(0));
        assertArrayEquals("Rows do not match", df.getRow(6), df2.getRow(1));
        df2 = DataFrame.convert(df2, NullableDataFrame.class);
        df.addRows(df2);
        assertTrue("DataFrame should have 9 rows", df.rows() == 9);
        assertTrue("DataFrame should have 9 columns", df.columns() == 9);
        assertArrayEquals("Rows do not match", df.getRow(7), df2.getRow(0));
        assertArrayEquals("Rows do not match", df.getRow(8), df2.getRow(1));
    }
    
    @Test
    public void testAddRowsShuffledLabels(){
        DataFrame df2 = new DefaultDataFrame(
                new String[]{
                        "longCol",   // 3
                        "intCol",    // 2
                        "booleanCol",// 8
                        "charCol",   // 5
                        "floatCol",  // 6
                        "shortCol",  // 1
                        "stringCol", // 4
                        "byteCol",   // 0
                        "doubleCol"  // 7
                },
                new LongColumn(new long[]{11l,22l}),
                new IntColumn(new int[]{11,22}),
                new BooleanColumn(new boolean[]{true,false}),
                new CharColumn(new char[]{'A','B'}),
                new FloatColumn(new float[]{11.1f,22.2f}),
                new ShortColumn(new short[]{11,22}),
                new StringColumn(new String[]{"11","22"}),
                new ByteColumn(new byte[]{11,22}),
                new DoubleColumn(new double[]{11.1,22.2}));

        df.addRows(df2);
        assertTrue("DataFrame should have 7 rows", df.rows() == 7);
        assertTrue("DataFrame should have 9 columns", df.columns() == 9);
        assertArrayEquals("Rows do not match", df.getRow(5),
                new Object[]{(byte)11,(short)11,11,11l,"11",'A',11.1f,11.1,true});
        
        assertArrayEquals("Rows do not match", df.getRow(6),
                new Object[]{(byte)22,(short)22,22,22l,"22",'B',22.2f,22.2,false});
        
        df2 = DataFrame.convert(df2, NullableDataFrame.class);
        df.addRows(df2);
        assertTrue("DataFrame should have 9 rows", df.rows() == 9);
        assertTrue("DataFrame should have 9 columns", df.columns() == 9);
        assertArrayEquals("Rows do not match", df.getRow(7),
                new Object[]{(byte)11,(short)11,11,11l,"11",'A',11.1f,11.1,true});
        
        assertArrayEquals("Rows do not match", df.getRow(8),
                new Object[]{(byte)22,(short)22,22,22l,"22",'B',22.2f,22.2,false});
    }
    
    @Test
    public void testAddRowsUnlabeled(){
        DataFrame df2 = new DefaultDataFrame(
                new ByteColumn(new byte[]{11,22}),
                new ShortColumn(new short[]{11,22}),
                new IntColumn(new int[]{11,22}),
                new LongColumn(new long[]{11l,22l}),
                new StringColumn(new String[]{"11","22"}),
                new CharColumn(new char[]{'A','B'}),
                new FloatColumn(new float[]{11.1f,22.2f}),
                new DoubleColumn(new double[]{11.1,22.2}),
                new BooleanColumn(new boolean[]{true,false}));

        df.addRows(df2);
        assertTrue("DataFrame should have 7 rows", df.rows() == 7);
        assertTrue("DataFrame should have 9 columns", df.columns() == 9);
        assertArrayEquals("Rows do not match", df.getRow(5), df2.getRow(0));
        assertArrayEquals("Rows do not match", df.getRow(6), df2.getRow(1));
        df2 = DataFrame.convert(df2, NullableDataFrame.class);
        df.addRows(df2);
        assertTrue("DataFrame should have 9 rows", df.rows() == 9);
        assertTrue("DataFrame should have 9 columns", df.columns() == 9);
        assertArrayEquals("Rows do not match", df.getRow(7), df2.getRow(0));
        assertArrayEquals("Rows do not match", df.getRow(8), df2.getRow(1));
    }
    
    @Test
    public void testAddRowsUnlabeledFraction(){
        DataFrame df2 = new DefaultDataFrame(
                new ByteColumn(new byte[]{11,22}),
                new ShortColumn(new short[]{11,22}),
                new IntColumn(new int[]{11,22}));

        df.addRows(df2);
        assertTrue("DataFrame should have 7 rows", df.rows() == 7);
        assertTrue("DataFrame should have 9 columns", df.columns() == 9);
        assertArrayEquals("Rows do not match", df.getRow(5),
                new Object[]{(byte)11,(short)11,11,0l,
                StringColumn.DEFAULT_VALUE,CharColumn.DEFAULT_VALUE,0.0f,0.0,false});
        
        assertArrayEquals("Rows do not match", df.getRow(6),
                new Object[]{(byte)22,(short)22,22,0l,
                StringColumn.DEFAULT_VALUE,CharColumn.DEFAULT_VALUE,0.0f,0.0,false});
        
        df2 = DataFrame.convert(df2, NullableDataFrame.class);
        df.addRows(df2);
        assertTrue("DataFrame should have 9 rows", df.rows() == 9);
        assertTrue("DataFrame should have 9 columns", df.columns() == 9);
        assertArrayEquals("Rows do not match", df.getRow(7),
                new Object[]{(byte)11,(short)11,11,0l,
                StringColumn.DEFAULT_VALUE,CharColumn.DEFAULT_VALUE,0.0f,0.0,false});
        
        assertArrayEquals("Rows do not match", df.getRow(8),
                new Object[]{(byte)22,(short)22,22,0l,
                StringColumn.DEFAULT_VALUE,CharColumn.DEFAULT_VALUE,0.0f,0.0,false});
    }
    
    @Test
    public void testHead(){
        DataFrame res = df.head();
        assertTrue("DataFrames should be equal", res.equals(df));
        res = df.head(3);
        assertTrue("DataFrames should be equal", res.equals(df.getRows(0, 3)));
        res = df.head(9999);
        assertTrue("DataFrames should be equal", res.equals(df));
        res = df.head(0);
        df.clear();
        assertTrue("DataFrames should be equal", res.equals(df));
    }
    
    @Test
    public void testHeadUninitialized(){
        DataFrame res = new DefaultDataFrame().head();
        assertTrue("DataFrame should have 0 rows", res.rows() == 0);
        assertTrue("DataFrame should have 0 columns", res.columns() == 0);
        assertFalse("DataFrame should have no column names", res.hasColumnNames());
    }
    
    @Test
    public void testTail(){
        DataFrame res = df.tail();
        assertTrue("DataFrames should be equal", res.equals(df));
        res = df.tail(3);
        assertTrue("DataFrames should be equal", res.equals(df.getRows(2, 5)));
        res = df.tail(9999);
        assertTrue("DataFrames should be equal", res.equals(df));
        res = df.tail(0);
        df.clear();
        assertTrue("DataFrames should be equal", res.equals(df));
    }
    
    @Test
    public void testTailUninitialized(){
        DataFrame res = new DefaultDataFrame().tail();
        assertTrue("DataFrame should have 0 rows", res.rows() == 0);
        assertTrue("DataFrame should have 0 columns", res.columns() == 0);
        assertFalse("DataFrame should have no column names", res.hasColumnNames());
    }
    
    @Test(expected=DataFrameException.class)
    public void testHeadInvalidArg(){
        df.head(-1);
    }
    
    @Test(expected=DataFrameException.class)
    public void testTailInvalidArg(){
        df.tail(-1);
    }

    //***************************//
    //           Columns         //
    //***************************//

    @Test
    public void testAddColumn(){
        Column col = new IntColumn(new int[]{0,1,2,3,4}); 
        df.addColumn(col);
        assertTrue("Column count should be 10", df.columns() == 10);
        assertSame("Column reference should be the same", col, df.getColumn(9));
    }

    @Test
    public void testAddColumnWithName(){
        Column col = new IntColumn(new int[]{0,1,2,3,4}); 
        df.addColumn("INT", col);
        assertTrue("Column count should be 10", df.columns() == 10);
        assertSame("Column reference should be the same", col, df.getColumn(9));
        assertSame("Column reference should be the same", col, df.getColumn("INT"));
    }

    @Test
    public void testRemoveColumnByIndex(){
        Column c = df.getColumn(3);
        Column removed = df.removeColumn(3);
        assertTrue("Columns do not match", c == removed);
        assertTrue("Column count should be 8", df.columns() == 8);
        assertTrue("Column after removal point should be of type StringColumn", 
                df.getColumn(3) instanceof StringColumn);

        assertTrue("Column before removal point should be of type IntColumn", 
                df.getColumn(2) instanceof IntColumn);
    }

    @Test
    public void testRemoveColumnByName(){
        Column c = df.getColumn("longCol");
        Column removed = df.removeColumn("longCol");
        assertTrue("Columns do not match", c == removed);
        assertTrue("Column count should be 8", df.columns() == 8);
        assertTrue("Column after removal point should be of type StringColumn", 
                df.getColumn(3) instanceof StringColumn);

        assertTrue("Column before removal point should be of type IntColumn", 
                df.getColumn(2) instanceof IntColumn);
    }
    
    @Test
    public void testRemoveColumnByReference(){
        Column col = df.getColumn("floatCol");
        boolean res = df.removeColumn(col);
        assertTrue("Column should be removed", res);
        assertTrue("Column count should be 8", df.columns() == 8);
        assertTrue("Row count should be 5", df.rows() == 5);
        String[] names = df.getColumnNames();
        assertArrayEquals("Column names do not match",
                new String[]{"byteCol","shortCol","intCol","longCol",
                      "stringCol","charCol","doubleCol","booleanCol"},
                      names);
    }
    
    @Test
    public void testRemoveColumnByReferenceNoRemoval(){
        Column col = new FloatColumn("TEST", df.rows());
        boolean res = df.removeColumn(col);
        assertFalse("Column should not be removed", res);
        assertTrue("Column count should be 9", df.columns() == 9);
        assertTrue("Row count should be 5", df.rows() == 5);
        String[] names = df.getColumnNames();
        assertArrayEquals("Column names do not match",
                           columnNames, names);
    }

    @Test
    public void testInsertColumnAt(){
        Column col = new IntColumn(new int[]{0,1,2,3,4});
        df.insertColumn(2, col);
        assertTrue("Column count should be 10", df.columns() == 10);
        assertSame("Column reference should be the same", col, df.getColumn(2));

        assertTrue("Column after insertion point should be of type IntColumn", 
                df.getColumn(3) instanceof IntColumn);

        assertTrue("Column before insertion point should be of type ShortColumn", 
                df.getColumn(1) instanceof ShortColumn);

    }

    @Test
    public void testInsertColumnAtWithName(){
        Column col = new IntColumn(new int[]{0,1,2,3,4});
        df.insertColumn(2, "INT", col);
        assertTrue("Column count should be 10", df.columns() == 10);
        assertSame("Column reference should be the same", col, df.getColumn(2));
        assertSame("Column reference should be the same", col, df.getColumn("INT"));

        assertTrue("Column after insertion point should be of type IntColumn", 
                df.getColumn(3) instanceof IntColumn);

        assertTrue("Column before insertion point should be of type ShortColumn", 
                df.getColumn(1) instanceof ShortColumn);
    }

    @Test
    public void testGetColumn(){
        Column col = df.getColumn(2);
        assertTrue("Column at index 2 should be of type IntColumn", 
                col instanceof IntColumn);
    }

    @Test
    public void testGetColumnByName(){
        Column col = df.getColumn("stringCol");
        assertTrue("Column \"stringCol\" should be of type StringColumn", 
                col instanceof StringColumn);
    }
    
    @Test
    public void testGetColumns(){
        DataFrame res = df.getColumns(1, 3, 5, 8);
        assertTrue("DataFrame should have 4 columns", res.columns() == 4);
        assertTrue("DataFrame should have 5 rows", res.rows() == 5);
        assertTrue("DataFrame should be a DefaultDataFrame", res instanceof DefaultDataFrame);
        assertArrayEquals("Column names do not match",
                new String[]{"shortCol", "longCol", "charCol", "booleanCol"},
                res.getColumnNames());

        assertTrue("Column references do not match", res.getColumn(0) == df.getColumn(1));
        assertTrue("Column references do not match", res.getColumn(1) == df.getColumn(3));
        assertTrue("Column references do not match", res.getColumn(2) == df.getColumn(5));
        assertTrue("Column references do not match", res.getColumn(3) == df.getColumn(8));
    }

    @Test
    public void testGetColumnsByName(){
        DataFrame res = df.getColumns("shortCol", "longCol", "charCol", "booleanCol");
        assertTrue("DataFrame should have 4 columns", res.columns() == 4);
        assertTrue("DataFrame should have 5 rows", res.rows() == 5);
        assertTrue("DataFrame should be a DefaultDataFrame", res instanceof DefaultDataFrame);
        assertArrayEquals("Column names do not match",
                new String[]{"shortCol", "longCol", "charCol", "booleanCol"},
                res.getColumnNames());

        assertTrue("Column references do not match", res.getColumn(0) == df.getColumn(1));
        assertTrue("Column references do not match", res.getColumn(1) == df.getColumn(3));
        assertTrue("Column references do not match", res.getColumn(2) == df.getColumn(5));
        assertTrue("Column references do not match", res.getColumn(3) == df.getColumn(8));
    }
    
    @Test
    public void testGetColumnsByElementTypes(){
        DataFrame res = df.getColumns(Short.class, Long.class, Character.class, Boolean.class);
        assertTrue("DataFrame should have 4 columns", res.columns() == 4);
        assertTrue("DataFrame should have 5 rows", res.rows() == 5);
        assertTrue("DataFrame should be a DefaultDataFrame", res instanceof DefaultDataFrame);
        assertArrayEquals("Column names do not match",
                new String[]{"shortCol", "longCol", "charCol", "booleanCol"},
                res.getColumnNames());

        assertTrue("Column references do not match", res.getColumn(0) == df.getColumn(1));
        assertTrue("Column references do not match", res.getColumn(1) == df.getColumn(3));
        assertTrue("Column references do not match", res.getColumn(2) == df.getColumn(5));
        assertTrue("Column references do not match", res.getColumn(3) == df.getColumn(8));
    }

    @Test
    public void testGetColumnsByElementTypesNumericOnly(){
        DataFrame res = df.getColumns(Number.class);
        assertTrue("DataFrame should have 6 columns", res.columns() == 6);
        assertTrue("DataFrame should have 5 rows", res.rows() == 5);
        assertTrue("DataFrame should be a DefaultDataFrame", res instanceof DefaultDataFrame);
        assertArrayEquals("Column names do not match",
                new String[]{"byteCol", "shortCol", "intCol", "longCol", "floatCol", "doubleCol"},
                res.getColumnNames());

        assertTrue("Column references do not match", res.getColumn(0) == df.getColumn(0));
        assertTrue("Column references do not match", res.getColumn(1) == df.getColumn(1));
        assertTrue("Column references do not match", res.getColumn(2) == df.getColumn(2));
        assertTrue("Column references do not match", res.getColumn(3) == df.getColumn(3));
        assertTrue("Column references do not match", res.getColumn(4) == df.getColumn(6));
        assertTrue("Column references do not match", res.getColumn(5) == df.getColumn(7));
    }

    @Test
    public void testGetColumnsFromEmptyDataFrame(){
        df.clear();
        DataFrame res = df.getColumns(0,2,5);
        assertTrue("DataFrame should have 3 columns", res.columns() == 3);
        assertTrue("DataFrame should have 0 rows", res.rows() == 0);
        assertTrue("Capacity does not match", res.capacity() == df.capacity());
        assertTrue("DataFrame should be a DefaultDataFrame", res instanceof DefaultDataFrame);
        assertTrue("Column references do not match", res.getColumn(0) == df.getColumn(0));
        assertTrue("Column references do not match", res.getColumn(1) == df.getColumn(2));
        assertTrue("Column references do not match", res.getColumn(2) == df.getColumn(5));
        res = df.getColumns("byteCol","intCol","charCol");
        assertTrue("DataFrame should have 3 columns", res.columns() == 3);
        assertTrue("DataFrame should have 0 rows", res.rows() == 0);
        assertTrue("Capacity does not match", res.capacity() == df.capacity());
        assertTrue("DataFrame should be a DefaultDataFrame", res instanceof DefaultDataFrame);
        assertTrue("Column references do not match", res.getColumn(0) == df.getColumn(0));
        assertTrue("Column references do not match", res.getColumn(1) == df.getColumn(2));
        assertTrue("Column references do not match", res.getColumn(2) == df.getColumn(5));
        res = df.getColumns(Byte.class,Integer.class,Character.class);
        assertTrue("DataFrame should have 3 columns", res.columns() == 3);
        assertTrue("DataFrame should have 0 rows", res.rows() == 0);
        assertTrue("Capacity does not match", res.capacity() == df.capacity());
        assertTrue("DataFrame should be a DefaultDataFrame", res instanceof DefaultDataFrame);
        assertTrue("Column references do not match", res.getColumn(0) == df.getColumn(0));
        assertTrue("Column references do not match", res.getColumn(1) == df.getColumn(2));
        assertTrue("Column references do not match", res.getColumn(2) == df.getColumn(5));
    }

    @Test
    public void testSetColumn(){
        Column col = new IntColumn(new int[]{0,1,2,3,4});
        df.setColumn(3, col);
        Column col2 = df.getColumn(3);
        assertSame("References to columns should match", col, col2);
        assertTrue("Column count should be 9", df.columns() == 9);
    }
    
    @Test
    public void testSetColumnByName(){
        Column col = new IntColumn("shouldBeReplaced", new int[]{0,1,2,3,4});
        df.setColumn("longCol", col);
        Column col2 = df.getColumn(3);
        String name1 = df.getColumnName(3);
        String name2 = df.getColumn("longCol").name;
        assertEquals("Column names do not match", name1, "longCol");
        assertEquals("Column names do not match", name2, "longCol");
        assertSame("References to columns should match", col, col2);
        assertTrue("Column count should be 9", df.columns() == 9);
    }
    
    @Test
    public void testSetColumnByNameAdd(){
        Column col = new IntColumn("shouldBeReplaced", new int[]{0,1,2,3,4});
        df.setColumn("NEWCOL", col);
        Column col2 = df.getColumn(df.columns()-1);
        String name1 = df.getColumnName(df.columns()-1);
        String name2 = df.getColumn("NEWCOL").name;
        assertEquals("Column names do not match", name1, "NEWCOL");
        assertEquals("Column names do not match", name2, "NEWCOL");
        assertSame("References to columns should match", col, col2);
        assertTrue("Column count should be 10", df.columns() == 10);
    }
    
    @Test
    public void testHasColumn(){
        assertTrue("Column should be present", df.hasColumn("byteCol"));
        assertTrue("Column should be present", df.hasColumn("booleanCol"));
        assertFalse("Column should not be present", df.hasColumn("NoByteCol"));
        assertFalse("Column should not be present", df.hasColumn("NoBooleanCol"));
    }

    //****************************************************************************************//
    //           Search, Filter, Drop, Include, Exclude, Replace, Factor and Contains         //
    //****************************************************************************************//

    @Test
    public void testIndexOf(){
        int i = df.indexOf(2, "42");
        assertTrue("Found index should be 3", i == 3);
        i = df.indexOf(2, "nothing");
        assertTrue("Returned index should be -1", i == -1);
    }

    @Test
    public void testIndexOfByName(){
        int i = df.indexOf("intCol", "42");
        assertTrue("Found index should be 3", i == 3);
        i = df.indexOf("intCol", "nothing");
        assertTrue("Returned index should be -1", i == -1);
        i = df.indexOf("intCol", null);
        assertTrue("Returned index should be -1", i == -1);
    }

    @Test
    public void testIndexOfWithStartPoint(){
        int i = df.indexOf(2, 2, "42");
        assertTrue("Found index should be 3", i == 3);
        i = df.indexOf(2, 2, "nothing");
        assertTrue("Returned index should be -1", i == -1);
        i = df.indexOf(2, 1, "12");
        assertTrue("Returned index should be -1", i == -1);
    }

    @Test
    public void testIndexOfByNameWithStartPoint(){
        int i = df.indexOf("intCol", 2, "42");
        assertTrue("Found index should be 3", i == 3);
        i = df.indexOf("intCol", 2, "nothing");
        assertTrue("Returned index should be -1", i == -1);
        i = df.indexOf("intCol", 1, "12");
        assertTrue("Returned index should be -1", i == -1);
        i = df.indexOf("intCol", 1, null);
        assertTrue("Returned index should be -1", i == -1);
    }

    @Test
    public void testIndexOfAll(){
        int[] i = df.indexOfAll(2, "[1-4]2");
        assertTrue("Returned array should have length 4", i.length == 4);
        int[] truth = new int[]{0,1,2,3};
        assertArrayEquals("Content of the returned array does not match expected values", 
                truth,
                i);

        i = df.indexOfAll(2, "nothing");
        assertTrue("Returned array should be empty", i.length == 0);
    }

    @Test
    public void testIndexOfAllByName(){
        int[] i = df.indexOfAll("intCol", "[1-4]2");
        assertTrue("Returned array should have length 4", i.length == 4);
        int[] truth = new int[]{0,1,2,3};
        assertArrayEquals("Content of the returned array does not match expected values", 
                truth,
                i);

        i = df.indexOfAll("intCol", "nothing");
        assertTrue("Returned array should be empty", i.length == 0);
        i = df.indexOfAll("intCol", null);
        assertTrue("Returned array should be empty", i.length == 0);
    }

    @Test
    public void testFilter(){
        DataFrame filtered = df.filter(2, "[1-4]2");
        assertNotNull("API violation: Returned DataFrame should not be null", filtered);
        assertFalse("Returned DataFrame should not be empty", filtered.isEmpty());
        assertTrue("Returned DataFrame should be of type DefaultDataFrame", 
                filtered instanceof DefaultDataFrame);

        assertTrue("Returned DataFrame should have 4 rows", filtered.rows() == 4);
        assertTrue("Returned DataFrame should have 9 columns", filtered.columns() == 9);

        assertTrue("", filtered.getInt("intCol", 1) == 22);
        assertArrayEquals("Row does not match expected values", 
                new Object[]{(byte)10,(short)11,12,13l,"10",'a',10.1f,11.1d,true}, 
                filtered.getRow(0));
    }

    @Test
    public void testFilterByName(){
        DataFrame filtered = df.filter("intCol", "[1-4]2");
        assertNotNull("API violation: Returned DataFrame should not be null", filtered);
        assertFalse("Returned DataFrame should not be empty", filtered.isEmpty());
        assertTrue("Returned DataFrame should be of type DefaultDataFrame", 
                filtered instanceof DefaultDataFrame);

        assertTrue("Returned DataFrame should have 4 rows", filtered.rows() == 4);
        assertTrue("Returned DataFrame should have 9 columns", filtered.columns() == 9);

        assertTrue("", filtered.getInt("intCol", 1) == 22);
        assertArrayEquals("Row does not match expected values", 
                new Object[]{(byte)10,(short)11,12,13l,"10",'a',10.1f,11.1d,true}, 
                filtered.getRow(0));
        
        assertTrue("Returned DataFrame should be empty", df.filter("intCol", null).isEmpty());
    }

    @Test
    public void testFilterNoMatch(){
        DataFrame filtered = df.filter(2, "[1-4]2Digit");
        assertNotNull("API violation: Returned DataFrame should not be null", filtered);
        assertTrue("Returned DataFrame should be empty", filtered.isEmpty());
        assertTrue("Returned DataFrame should be of type DefaultDataFrame", 
                filtered instanceof DefaultDataFrame);

        assertTrue("Returned DataFrame should have 0 rows", filtered.rows() == 0);
        assertTrue("Returned DataFrame should have 9 columns", filtered.columns() == 9);
    }
    
    @Test
    public void testDrop(){
        DataFrame filtered = df.drop(2, "[1-3]2");
        assertNotNull("API violation: Returned DataFrame should not be null", filtered);
        assertFalse("Returned DataFrame should not be empty", filtered.isEmpty());
        assertTrue("Returned DataFrame should be of type DefaultDataFrame",
                filtered instanceof DefaultDataFrame);

        assertTrue("Returned DataFrame should have 2 rows", filtered.rows() == 2);
        assertTrue("Returned DataFrame should have 9 columns", filtered.columns() == 9);

        assertTrue("", filtered.getInt("intCol", 1) == 52);
        assertArrayEquals("Row does not match expected values", 
                new Object[]{(byte)40,(short)41,42,43l,"40",'d',40.4f,41.4d,false}, 
                filtered.getRow(0));
    }

    @Test
    public void testDropByName(){
        DataFrame filtered = df.drop("intCol", "[1-3]2");
        assertNotNull("API violation: Returned DataFrame should not be null", filtered);
        assertFalse("Returned DataFrame should not be empty", filtered.isEmpty());
        assertTrue("Returned DataFrame should be of type DefaultDataFrame",
                filtered instanceof DefaultDataFrame);

        assertTrue("Returned DataFrame should have 2 rows", filtered.rows() == 2);
        assertTrue("Returned DataFrame should have 9 columns", filtered.columns() == 9);

        assertTrue("", filtered.getInt("intCol", 1) == 52);
        assertArrayEquals("Row does not match expected values", 
                new Object[]{(byte)40,(short)41,42,43l,"40",'d',40.4f,41.4d,false}, 
                filtered.getRow(0));
    }

    @Test
    public void testDropEverything(){
        DataFrame filtered = df.drop(2, ".*");
        assertNotNull("API violation: Returned DataFrame should not be null", filtered);
        assertTrue("Returned DataFrame should be empty", filtered.isEmpty());
        assertTrue("Returned DataFrame should be of type DefaultDataFrame",
                filtered instanceof DefaultDataFrame);

        assertTrue("Returned DataFrame should have 0 rows", filtered.rows() == 0);
        assertTrue("Returned DataFrame should have 9 columns", filtered.columns() == 9);
    }
    
    @Test
    public void testInclude(){
        df.include(2, "[1-4]2");
        assertTrue("DataFrame should have 4 rows", df.rows() == 4);
        assertTrue("DataFrame should have 9 columns", df.columns() == 9);

        assertTrue("", df.getInt("intCol", 1) == 22);
        assertArrayEquals("Row does not match expected values", 
                new Object[]{(byte)10,(short)11,12,13l,"10",'a',10.1f,11.1d,true}, 
                df.getRow(0));
    }

    @Test
    public void testIncludeByName(){
        df.include("intCol", "[1-4]2");
        assertTrue("DataFrame should have 4 rows", df.rows() == 4);
        assertTrue("DataFrame should have 9 columns", df.columns() == 9);
        assertTrue("", df.getInt("intCol", 1) == 22);
        assertArrayEquals("Row does not match expected values", 
                new Object[]{(byte)10,(short)11,12,13l,"10",'a',10.1f,11.1d,true}, 
                df.getRow(0));
    }
    
    @Test
    public void testExclude(){
        df.exclude(2, "[1-3]2");
        assertTrue("DataFrame should have 2 rows", df.rows() == 2);
        assertTrue("DataFrame should have 9 columns", df.columns() == 9);
        assertTrue("", df.getInt("intCol", 1) == 52);
        assertArrayEquals("Row does not match expected values", 
                new Object[]{(byte)40,(short)41,42,43l,"40",'d',40.4f,41.4d,false}, 
                df.getRow(0));
    }

    @Test
    public void testExcludeByName(){
        df.exclude("intCol", "[1-3]2");
        assertTrue("DataFrame should have 2 rows", df.rows() == 2);
        assertTrue("DataFrame should have 9 columns", df.columns() == 9);
        assertTrue("", df.getInt("intCol", 1) == 52);
        assertArrayEquals("Row does not match expected values", 
                new Object[]{(byte)40,(short)41,42,43l,"40",'d',40.4f,41.4d,false}, 
                df.getRow(0));
    }
    
    @Test
    public void testReplace(){
        int replacedLongs = df.replace(3, "(1|2|3)3", 666l);
        int replacedStrings = df.replace(4, "(4|5)0", "TEST");
        int replacedBooleans = df.replace(8, "false", true);
        assertTrue("Replaced number should be 3", replacedLongs == 3);
        assertTrue("Replaced number should be 2", replacedStrings == 2);
        assertTrue("Replaced number should be 2", replacedBooleans == 2);
        
        assertTrue("Value does not match replaced value", df.getLong(3, 0) == 666l);
        assertTrue("Value does not match replaced value", df.getLong(3, 1) == 666l);
        assertTrue("Value does not match replaced value", df.getLong(3, 2) == 666l);
        assertTrue("Value does not match replaced value", df.getLong(3, 3) == 43l);
        assertTrue("Value does not match replaced value", df.getLong(3, 4) == 53l);
        
        assertTrue("Value does not match replaced value", df.getString(4, 0).equals("10"));
        assertTrue("Value does not match replaced value", df.getString(4, 1).equals("20"));
        assertTrue("Value does not match replaced value", df.getString(4, 2).equals("30"));
        assertTrue("Value does not match replaced value", df.getString(4, 3).equals("TEST"));
        assertTrue("Value does not match replaced value", df.getString(4, 4).equals("TEST"));
        
        assertTrue("Value does not match replaced value", df.getBoolean(8, 0));
        assertTrue("Value does not match replaced value", df.getBoolean(8, 1));
        assertTrue("Value does not match replaced value", df.getBoolean(8, 2));
        assertTrue("Value does not match replaced value", df.getBoolean(8, 3));
        assertTrue("Value does not match replaced value", df.getBoolean(8, 4));
    }
    
    @Test
    public void testReplaceByName(){
        int replacedLongs = df.replace("longCol", "(1|2|3)3", 666l);
        int replacedStrings = df.replace("stringCol", "(4|5)0", "TEST");
        int replacedBooleans = df.replace("booleanCol", "false", true);
        assertTrue("Replaced number should be 3", replacedLongs == 3);
        assertTrue("Replaced number should be 2", replacedStrings == 2);
        assertTrue("Replaced number should be 2", replacedBooleans == 2);
        
        assertTrue("Value does not match replaced value", df.getLong("longCol", 0) == 666l);
        assertTrue("Value does not match replaced value", df.getLong("longCol", 1) == 666l);
        assertTrue("Value does not match replaced value", df.getLong("longCol", 2) == 666l);
        assertTrue("Value does not match replaced value", df.getLong("longCol", 3) == 43l);
        assertTrue("Value does not match replaced value", df.getLong("longCol", 4) == 53l);
        
        assertTrue("Value does not match replaced value", df.getString("stringCol", 0).equals("10"));
        assertTrue("Value does not match replaced value", df.getString("stringCol", 1).equals("20"));
        assertTrue("Value does not match replaced value", df.getString("stringCol", 2).equals("30"));
        assertTrue("Value does not match replaced value", df.getString("stringCol", 3).equals("TEST"));
        assertTrue("Value does not match replaced value", df.getString("stringCol", 4).equals("TEST"));
        
        assertTrue("Value does not match replaced value", df.getBoolean("booleanCol", 0));
        assertTrue("Value does not match replaced value", df.getBoolean("booleanCol", 1));
        assertTrue("Value does not match replaced value", df.getBoolean("booleanCol", 2));
        assertTrue("Value does not match replaced value", df.getBoolean("booleanCol", 3));
        assertTrue("Value does not match replaced value", df.getBoolean("booleanCol", 4));
    }
    
    @Test
    public void testReplaceLambda(){
        int replacedLongs = df.replace(3, (i, l) -> (long)i);
        int replacedStrings = df.replace(4, (i, s) -> "TEST" + i);
        int replacedBooleans = df.replace(8, (i, b) -> false);
        assertTrue("Replaced number should be 5", replacedLongs == 5);
        assertTrue("Replaced number should be 5", replacedStrings == 5);
        assertTrue("Replaced number should be 3", replacedBooleans == 3);
        
        assertTrue("Value does not match replaced value", df.getLong(3, 0) == 0l);
        assertTrue("Value does not match replaced value", df.getLong(3, 1) == 1l);
        assertTrue("Value does not match replaced value", df.getLong(3, 2) == 2l);
        assertTrue("Value does not match replaced value", df.getLong(3, 3) == 3l);
        assertTrue("Value does not match replaced value", df.getLong(3, 4) == 4l);
        
        assertTrue("Value does not match replaced value", df.getString(4, 0).equals("TEST0"));
        assertTrue("Value does not match replaced value", df.getString(4, 1).equals("TEST1"));
        assertTrue("Value does not match replaced value", df.getString(4, 2).equals("TEST2"));
        assertTrue("Value does not match replaced value", df.getString(4, 3).equals("TEST3"));
        assertTrue("Value does not match replaced value", df.getString(4, 4).equals("TEST4"));
       
        assertFalse("Value does not match replaced value", df.getBoolean(8, 0));
        assertFalse("Value does not match replaced value", df.getBoolean(8, 1));
        assertFalse("Value does not match replaced value", df.getBoolean(8, 2));
        assertFalse("Value does not match replaced value", df.getBoolean(8, 3));
        assertFalse("Value does not match replaced value", df.getBoolean(8, 4));
    }
    
    @Test
    public void testReplaceByNameLambda(){
        int replacedLongs = df.replace("longCol", (i, l) -> (long)i);
        int replacedStrings = df.replace("stringCol", (i, s) -> "TEST" + i);
        int replacedBooleans = df.replace("booleanCol", (i, b) -> false);
        assertTrue("Replaced number should be 5", replacedLongs == 5);
        assertTrue("Replaced number should be 5", replacedStrings == 5);
        assertTrue("Replaced number should be 3", replacedBooleans == 3);
        
        assertTrue("Value does not match replaced value", df.getLong("longCol", 0) == 0l);
        assertTrue("Value does not match replaced value", df.getLong("longCol", 1) == 1l);
        assertTrue("Value does not match replaced value", df.getLong("longCol", 2) == 2l);
        assertTrue("Value does not match replaced value", df.getLong("longCol", 3) == 3l);
        assertTrue("Value does not match replaced value", df.getLong("longCol", 4) == 4l);
        
        assertTrue("Value does not match replaced value", df.getString("stringCol", 0).equals("TEST0"));
        assertTrue("Value does not match replaced value", df.getString("stringCol", 1).equals("TEST1"));
        assertTrue("Value does not match replaced value", df.getString("stringCol", 2).equals("TEST2"));
        assertTrue("Value does not match replaced value", df.getString("stringCol", 3).equals("TEST3"));
        assertTrue("Value does not match replaced value", df.getString("stringCol", 4).equals("TEST4"));
        
        assertFalse("Value does not match replaced value", df.getBoolean("booleanCol", 0));
        assertFalse("Value does not match replaced value", df.getBoolean("booleanCol", 1));
        assertFalse("Value does not match replaced value", df.getBoolean("booleanCol", 2));
        assertFalse("Value does not match replaced value", df.getBoolean("booleanCol", 3));
        assertFalse("Value does not match replaced value", df.getBoolean("booleanCol", 4));
    }
    
    @Test
    public void testReplaceRegexLambda(){
        int replacedLongs = df.replace(3, "(1|2|3)3", (i, value) -> 666l);
        int replacedStrings = df.replace(4, "(4|5)0", (i, value) -> "TEST");
        int replacedBooleans = df.replace(8, "false", (i, value) -> true);
        assertTrue("Replaced number should be 3", replacedLongs == 3);
        assertTrue("Replaced number should be 2", replacedStrings == 2);
        assertTrue("Replaced number should be 2", replacedBooleans == 2);
        
        assertTrue("Value does not match replaced value", df.getLong(3, 0) == 666l);
        assertTrue("Value does not match replaced value", df.getLong(3, 1) == 666l);
        assertTrue("Value does not match replaced value", df.getLong(3, 2) == 666l);
        assertTrue("Value does not match replaced value", df.getLong(3, 3) == 43l);
        assertTrue("Value does not match replaced value", df.getLong(3, 4) == 53l);
        
        assertTrue("Value does not match replaced value", df.getString(4, 0).equals("10"));
        assertTrue("Value does not match replaced value", df.getString(4, 1).equals("20"));
        assertTrue("Value does not match replaced value", df.getString(4, 2).equals("30"));
        assertTrue("Value does not match replaced value", df.getString(4, 3).equals("TEST"));
        assertTrue("Value does not match replaced value", df.getString(4, 4).equals("TEST"));
        
        assertTrue("Value does not match replaced value", df.getBoolean(8, 0));
        assertTrue("Value does not match replaced value", df.getBoolean(8, 1));
        assertTrue("Value does not match replaced value", df.getBoolean(8, 2));
        assertTrue("Value does not match replaced value", df.getBoolean(8, 3));
        assertTrue("Value does not match replaced value", df.getBoolean(8, 4));
    }
    
    @Test
    public void testReplaceByNameRegexLambda(){
        int replacedLongs = df.replace("longCol", "(1|2|3)3", (i, value) -> 666l);
        int replacedStrings = df.replace("stringCol", "(4|5)0", (i, value) -> "TEST");
        int replacedBooleans = df.replace("booleanCol", "false", (i, value) -> true);
        assertTrue("Replaced number should be 3", replacedLongs == 3);
        assertTrue("Replaced number should be 2", replacedStrings == 2);
        assertTrue("Replaced number should be 2", replacedBooleans == 2);
        
        assertTrue("Value does not match replaced value", df.getLong("longCol", 0) == 666l);
        assertTrue("Value does not match replaced value", df.getLong("longCol", 1) == 666l);
        assertTrue("Value does not match replaced value", df.getLong("longCol", 2) == 666l);
        assertTrue("Value does not match replaced value", df.getLong("longCol", 3) == 43l);
        assertTrue("Value does not match replaced value", df.getLong("longCol", 4) == 53l);
        
        assertTrue("Value does not match replaced value", df.getString("stringCol", 0).equals("10"));
        assertTrue("Value does not match replaced value", df.getString("stringCol", 1).equals("20"));
        assertTrue("Value does not match replaced value", df.getString("stringCol", 2).equals("30"));
        assertTrue("Value does not match replaced value", df.getString("stringCol", 3).equals("TEST"));
        assertTrue("Value does not match replaced value", df.getString("stringCol", 4).equals("TEST"));
        
        assertTrue("Value does not match replaced value", df.getBoolean("booleanCol", 0));
        assertTrue("Value does not match replaced value", df.getBoolean("booleanCol", 1));
        assertTrue("Value does not match replaced value", df.getBoolean("booleanCol", 2));
        assertTrue("Value does not match replaced value", df.getBoolean("booleanCol", 3));
        assertTrue("Value does not match replaced value", df.getBoolean("booleanCol", 4));
    }
    
    @Test
    public void testReplaceDataFrame(){
        DataFrame df2 = new DefaultDataFrame(
                new String[]{"TEST1","floatCol","TEST2","booleanCol"},
                new IntColumn(new int[]{
                        44,44,44,44,44
                }),
                new FloatColumn(new float[]{
                        44.4f,44.4f,44.4f,44.4f,44.4f
                }),
                new DoubleColumn(new double[]{
                        44.4,44.4,44.4,44.4,44.4
                }),
                new BooleanColumn(new boolean[]{
                        false,true,false,true,false
                }));
        
        int replaced = df.replace(df2);
        assertTrue("Replace count should be 2", replaced == 2);
        assertTrue("Column reference does not match",
                df.getColumn("floatCol") == df2.getColumn("floatCol"));
        
        assertTrue("Column reference does not match",
                df.getColumn("booleanCol") == df2.getColumn("booleanCol"));
    }
    
    @Test
    public void testReplaceDataFrameNoColumnNames(){
        DataFrame df2 = new DefaultDataFrame(
                new IntColumn(new int[]{
                        44,44,44,44,44
                }),
                new FloatColumn(new float[]{
                        44.4f,44.4f,44.4f,44.4f,44.4f
                }),
                new DoubleColumn(new double[]{
                        44.4,44.4,44.4,44.4,44.4
                }),
                new BooleanColumn(new boolean[]{
                        false,true,false,true,false
                }));
        
        df.removeColumnNames();
        int replaced = df.replace(df2);
        assertTrue("Replace count should be 4", replaced == 4);
        assertTrue("Column reference does not match", df.getColumn(0) == df2.getColumn(0));
        assertTrue("Column reference does not match", df.getColumn(1) == df2.getColumn(1));
        assertTrue("Column reference does not match", df.getColumn(2) == df2.getColumn(2));
        assertTrue("Column reference does not match", df.getColumn(3) == df2.getColumn(3));
    }
    
    @Test
    public void testFactor(){
        df.setString(4, 0, df.getString(4, 4));
        df.setChar(5, 0, df.getChar(5, 4));
        Map<Object, Integer> map1 = df.factor(4);
        Map<Object, Integer> map2 = df.factor(5);
        Map<Object, Integer> map3 = df.factor(8);
        assertTrue("Factor map should have a size of 4", map1.size() == 4);
        assertTrue("Factor map should have a size of 4", map2.size() == 4);
        assertTrue("Factor map should have a size of 2", map3.size() == 2);
        assertTrue("Column should be an IntColumn", df.getColumn(4).typeCode() == IntColumn.TYPE_CODE);
        assertTrue("Column should be an IntColumn", df.getColumn(5).typeCode() == IntColumn.TYPE_CODE);
        assertTrue("Column should be an IntColumn", df.getColumn(8).typeCode() == IntColumn.TYPE_CODE);
        assertArrayEquals("Column content does not match", new int[]{1,2,3,4,1},
                ((IntColumn)df.getColumn(4)).asArray());
        assertArrayEquals("Column content does not match", new int[]{1,2,3,4,1},
                ((IntColumn)df.getColumn(5)).asArray());
        assertArrayEquals("Column content does not match", new int[]{1,2,1,2,1},
                ((IntColumn)df.getColumn(8)).asArray());
    }
    
    @Test
    public void testFactorByName(){
        df.setString("stringCol", 0, df.getString("stringCol", 4));
        df.setChar("charCol", 0, df.getChar("charCol", 4));
        Map<Object, Integer> map1 = df.factor("stringCol");
        Map<Object, Integer> map2 = df.factor("charCol");
        Map<Object, Integer> map3 = df.factor("booleanCol");
        assertTrue("Factor map should have a size of 4", map1.size() == 4);
        assertTrue("Factor map should have a size of 4", map2.size() == 4);
        assertTrue("Factor map should have a size of 2", map3.size() == 2);
        assertTrue("Column should be an IntColumn", df.getColumn("stringCol").typeCode() == IntColumn.TYPE_CODE);
        assertTrue("Column should be an IntColumn", df.getColumn("charCol").typeCode() == IntColumn.TYPE_CODE);
        assertTrue("Column should be an IntColumn", df.getColumn("booleanCol").typeCode() == IntColumn.TYPE_CODE);
        assertArrayEquals("Column content does not match", new int[]{1,2,3,4,1},
                ((IntColumn)df.getColumn("stringCol")).asArray());
        assertArrayEquals("Column content does not match", new int[]{1,2,3,4,1},
                ((IntColumn)df.getColumn("charCol")).asArray());
        assertArrayEquals("Column content does not match", new int[]{1,2,1,2,1},
                ((IntColumn)df.getColumn("booleanCol")).asArray());
    }
    
    @Test
    public void testFactorNumericColumn(){
        Map<Object, Integer> map = df.factor("byteCol");
        assertTrue("Factor map should be empty", map.isEmpty());
        map = df.factor("shortCol");
        assertTrue("Factor map should be empty", map.isEmpty());
        map = df.factor("intCol");
        assertTrue("Factor map should be empty", map.isEmpty());
        map = df.factor("longCol");
        assertTrue("Factor map should be empty", map.isEmpty());
        map = df.factor("floatCol");
        assertTrue("Factor map should be empty", map.isEmpty());
        map = df.factor("doubleCol");
        assertTrue("Factor map should be empty", map.isEmpty());
        assertTrue("Column should be a ByteColumn",
                df.getColumn("byteCol").typeCode() == ByteColumn.TYPE_CODE);
        assertTrue("Column should be a ShortColumn",
                df.getColumn("shortCol").typeCode() == ShortColumn.TYPE_CODE);
        assertTrue("Column should be an IntColumn",
                df.getColumn("intCol").typeCode() == IntColumn.TYPE_CODE);
        assertTrue("Column should be a LongColumn",
                df.getColumn("longCol").typeCode() == LongColumn.TYPE_CODE);
        assertTrue("Column should be a FloatColumn",
                df.getColumn("floatCol").typeCode() == FloatColumn.TYPE_CODE);
        assertTrue("Column should be a DoubleColumn",
                df.getColumn("doubleCol").typeCode() == DoubleColumn.TYPE_CODE);
    }
    
    @Test(expected=DataFrameException.class)
    public void testReplaceFailType(){
        df.replace("longCol", "(1|2|3)3", "NOT_A_LONG");
    }
    
    @Test(expected=DataFrameException.class)
    public void testReplaceLambdaFailType(){
        df.replace("longCol", (i, value) -> "NOT_A_LONG");
    }
    
    @Test(expected=DataFrameException.class)
    public void testReplaceRegexLambdaFailType(){
        df.replace("longCol", "(1|2|3)3", (i, value) -> "NOT_A_LONG");
    }
    
    @Test
    public void testReplaceIdentity(){
        int count = df.replace(3, (i, value) -> value);
        assertTrue("Replacement count should be zero", count == 0);
    }
    
    @Test
    public void testReplaceRegexIdentity(){
        int count = df.replace(3, "(1|2|3)3", (i, value) -> value);
        assertTrue("Replacement count should be zero", count == 0);
    }
    
    @Test(expected=DataFrameException.class)
    public void testReplaceFailWithNull(){
        df.replace(3, "(1|2|3)3", (Object)null);
    }
    
    @Test(expected=DataFrameException.class)
    public void testReplaceByNameFailWithNull(){
        df.replace("longCol", "(1|2|3)3", (Object)null);
    }
    
    @Test(expected=DataFrameException.class)
    public void testReplaceLambdaFailWithNull(){
        df.replace(3, (i, value) -> null);
    }
    
    @Test(expected=DataFrameException.class)
    public void testReplaceRegexLambdaFailWithNull(){
        df.replace(3, "(1|2|3)3", (i, value) -> null);
    }
    
    @Test
    public void testContains(){
        boolean res = df.contains(3, "53");
        assertTrue("Contains should return true", res);
        res = df.contains(8, "null");
        assertFalse("Contains should return false", res);
    }
    
    @Test
    public void testContainsByName(){
        boolean res = df.contains("longCol", "53");
        assertTrue("Contains should return true", res);
        res = df.contains("booleanCol", "null");
        assertFalse("Contains should return false", res);
    }
    
    //**************************************************************//
    //           Count, CountUnique and Unique operations           //
    //**************************************************************//
    
    @Test
    public void testCount(){
        DataFrame count = df.count(4);
        assertTrue("Count should have 5 rows", count.rows() == 5);
        assertTrue("Count should have 3 columns", count.columns() == 3);
        assertTrue("Counts sum is incorrect", (int)count.sum(1) == df.rows());
        assertTrue("Value column should be a StringColumn",
                count.getColumn(0) instanceof StringColumn);
        
        assertTrue("Count column should be an IntColumn",
                count.getColumn(1) instanceof IntColumn);
        
        assertTrue("Rate column should be a FloatColumn",
                count.getColumn(2) instanceof FloatColumn);
        
        assertTrue("Count DataFrame should be a DefaultDataFrame",
                count instanceof DefaultDataFrame);
        
        for(int i=0; i<count.rows(); ++i){
            assertTrue("Value should have a count of 1",
                    count.getInt("count", i) == 1);
        }
        count = df.count(8);
        assertTrue("Count should have 2 rows", count.rows() == 2);
        assertTrue("Count should have 3 columns", count.columns() == 3);
        assertTrue("Counts sum is incorrect", (int)count.sum(1) == df.rows());
        assertTrue("Value column should be a BooleanColumn",
                count.getColumn(0) instanceof BooleanColumn);
    }
    
    @Test
    public void testCountByName(){
        DataFrame count = df.count("stringCol");
        assertTrue("Count should have 5 rows", count.rows() == 5);
        assertTrue("Count should have 3 columns", count.columns() == 3);
        assertTrue("Counts sum is incorrect", (int)count.sum("count") == df.rows());
        assertTrue("Value column should be a StringColumn",
                count.getColumn(0) instanceof StringColumn);
        
        assertTrue("Count column should be an IntColumn",
                count.getColumn(1) instanceof IntColumn);
        
        assertTrue("Rate column should be a FloatColumn",
                count.getColumn(2) instanceof FloatColumn);
        
        assertTrue("Count DataFrame should be a DefaultDataFrame",
                count instanceof DefaultDataFrame);
        
        for(int i=0; i<count.rows(); ++i){
            assertTrue("Value should have a count of 1",
                    count.getInt("count", i) == 1);
        }
        count = df.count("booleanCol");
        assertTrue("Count should have 2 rows", count.rows() == 2);
        assertTrue("Count should have 3 columns", count.columns() == 3);
        assertTrue("Counts sum is incorrect", (int)count.sum("count") == df.rows());
        assertTrue("Value column should be a BooleanColumn",
                count.getColumn(0) instanceof BooleanColumn);
    }
    
    @Test
    public void testCountRegex(){
        int count = df.count(2, "[1-4]2");
        assertTrue("Count should be 4", count == 4);
        count = df.count(4, "Blabla");
        assertTrue("Count should be 0", count == 0);
    }
    
    @Test
    public void testCountRegexByName(){
        int count = df.count("intCol", "[1-4]2");
        assertTrue("Count should be 4", count == 4);
        count = df.count("stringCol", "Blabla");
        assertTrue("Count should be 0", count == 0);
    }

    @Test
    public void testCountNullRegex(){
        int count = df.count("intCol", null);
        assertTrue("Count should be 0", count == 0);
        count = df.count("stringCol", "");
        assertTrue("Count should be 0", count == 0);
    }

    @Test
    public void testCountUnique(){
        int count = df.countUnique(2);
        assertTrue("Unique count should be 5", count == 5);
        count = df.countUnique(8);
        assertTrue("Unique count should be 2", count == 2);
    }
    
    @Test
    public void testCountUniqueByName(){
        int count = df.countUnique("intCol");
        assertTrue("Unique count should be 5", count == 5);
        count = df.countUnique("booleanCol");
        assertTrue("Unique count should be 2", count == 2);
    }
    
    @Test
    public void testUnique(){
        Set<Integer> set1 = df.unique(2);
        assertTrue("Unique set size should be 5", set1.size() == 5);
        Set<Integer> truthInt = new HashSet<Integer>(
                Arrays.asList(new Integer[]{12,22,32,42,52}));
        
        assertTrue("Sets should be equal", set1.equals(truthInt));
        
        Set<Integer> set2 = df.unique(4);
        assertTrue("Unique set size should be 5", set2.size() == 5);
        Set<String> truthString = new HashSet<String>(
                Arrays.asList(new String[]{"10","20","30","40","50"}));
        
        assertTrue("Sets should be equal", set2.equals(truthString));
        
        Set<Boolean> set3 = df.unique(8);
        assertTrue("Unique set size should be 2", set3.size() == 2);
        Set<Boolean> truthBoolean = new HashSet<Boolean>(
                Arrays.asList(new Boolean[]{true,false}));
        
        assertTrue("Sets should be equal", set3.equals(truthBoolean));
    }
    
    @Test
    public void testUniqueByName(){
        Set<Integer> set1 = df.unique("intCol");
        assertTrue("Unique set size should be 5", set1.size() == 5);
        Set<Integer> truthInt = new HashSet<Integer>(
                Arrays.asList(new Integer[]{12,22,32,42,52}));
        
        assertTrue("Sets should be equal", set1.equals(truthInt));
        
        Set<Integer> set2 = df.unique("stringCol");
        assertTrue("Unique set size should be 5", set2.size() == 5);
        Set<String> truthString = new HashSet<String>(
                Arrays.asList(new String[]{"10","20","30","40","50"}));
        
        assertTrue("Sets should be equal", set2.equals(truthString));
        
        Set<Boolean> set3 = df.unique("booleanCol");
        assertTrue("Unique set size should be 2", set3.size() == 2);
        Set<Boolean> truthBoolean = new HashSet<Boolean>(
                Arrays.asList(new Boolean[]{true,false}));
        
        assertTrue("Sets should be equal", set3.equals(truthBoolean));
    }
    
    //*******************************************************************//
    //           Difference, Union and Intersection operations           //
    //*******************************************************************//
    
    @Test
    public void testDifferenceColumns(){
        DataFrame df1 = new DefaultDataFrame(
                Column.create("A", "aaa","aab","aac"),
                Column.create("B", 1, 2, 3),
                Column.create("E", 1, 2, 3));

        DataFrame df2 = new DefaultDataFrame(
                Column.create("A", "bba","bbb","bbc"),
                Column.create("C", 1.1f,2.2f,3.3f),
                Column.create("B", 11, 22, 33),
                Column.create("D", 'a', 'b', 'c'));
        
        DataFrame df3 = df1.differenceColumns(df2);
        assertFalse("DataFrame has an invalid type", df3.isNullable());
        assertTrue("DataFrame should have 3 columns", df3.columns() == 3);
        assertTrue("DataFrame should have 3 rows", df3.rows() == 3);
        assertArrayEquals("Columns do not match", new String[]{"E","C","D"}, df3.getColumnNames());
        assertTrue("Columns reference does not match", df3.getColumn("E") == df1.getColumn("E"));
        assertTrue("Columns reference does not match", df3.getColumn("C") == df2.getColumn("C"));
        assertTrue("Columns reference does not match", df3.getColumn("D") == df2.getColumn("D"));
    }
    
    @Test
    public void testDifferenceColumnsSameArg(){
        DataFrame df1 = new DefaultDataFrame(
                Column.create("A", "aaa","aab","aac"),
                Column.create("B", 1, 2, 3),
                Column.create("E", 1, 2, 3));

        DataFrame df3 = df1.differenceColumns(df1);
        assertFalse("DataFrame has an invalid type", df3.isNullable());
        assertTrue("DataFrame should have 0 columns", df3.columns() == 0);
        assertTrue("DataFrame should have 0 rows", df3.rows() == 0);
        assertTrue("Column names should be empty", df3.getColumnNames() == null);
    }

    @Test
    public void testUnionColumns(){
        DataFrame df1 = new DefaultDataFrame(
                Column.create("A", "aaa","aab","aac"),
                Column.create("B", 1, 2, 3),
                Column.create("E", 1, 2, 3));

        DataFrame df2 = new DefaultDataFrame(
                Column.create("A", "bba","bbb","bbc"),
                Column.create("C", 1.1f,2.2f,3.3f),
                Column.create("B", 11, 22, 33),
                Column.create("D", 'a', 'b', 'c'));
        
        DataFrame df3 = df1.unionColumns(df2);
        assertFalse("DataFrame has an invalid type", df3.isNullable());
        assertTrue("DataFrame should have 5 columns", df3.columns() == 5);
        assertTrue("DataFrame should have 3 rows", df3.rows() == 3);
        assertArrayEquals("Columns do not match", new String[]{"A","B","E","C","D"}, df3.getColumnNames());
        assertTrue("Columns reference does not match", df3.getColumn("A") == df1.getColumn("A"));
        assertTrue("Columns reference does not match", df3.getColumn("B") == df1.getColumn("B"));
        assertTrue("Columns reference does not match", df3.getColumn("E") == df1.getColumn("E"));
        assertTrue("Columns reference does not match", df3.getColumn("C") == df2.getColumn("C"));
        assertTrue("Columns reference does not match", df3.getColumn("D") == df2.getColumn("D"));
    }
    
    @Test
    public void testUnionColumnsSameArg(){
        DataFrame df1 = new DefaultDataFrame(
                Column.create("A", "aaa","aab","aac"),
                Column.create("B", 1, 2, 3),
                Column.create("E", 1, 2, 3));

        DataFrame df3 = df1.unionColumns(df1);
        assertFalse("DataFrame has an invalid type", df3.isNullable());
        assertTrue("DataFrame should have 3 columns", df3.columns() == 3);
        assertTrue("DataFrame should have 3 rows", df3.rows() == 3);
        assertArrayEquals("Columns do not match", new String[]{"A","B","E"}, df3.getColumnNames());
        assertTrue("Columns reference does not match", df3.getColumn("A") == df1.getColumn("A"));
        assertTrue("Columns reference does not match", df3.getColumn("B") == df1.getColumn("B"));
        assertTrue("Columns reference does not match", df3.getColumn("E") == df1.getColumn("E"));
    }
    
    @Test
    public void testIntersectionColumns(){
        DataFrame df1 = new DefaultDataFrame(
                Column.create("A", "aaa","aab","aac"),
                Column.create("B", 1, 2, 3),
                Column.create("E", 1, 2, 3));

        DataFrame df2 = new DefaultDataFrame(
                Column.create("C", "bba","bbb","bbc"),
                Column.create("A", 1.1f,2.2f,3.3f),
                Column.create("D", 11, 22, 33),
                Column.create("B", 'a', 'b', 'c'));
        
        DataFrame df3 = df1.intersectionColumns(df2);
        assertFalse("DataFrame has an invalid type", df3.isNullable());
        assertTrue("DataFrame should have 2 columns", df3.columns() == 2);
        assertTrue("DataFrame should have 3 rows", df3.rows() == 3);
        assertArrayEquals("Columns do not match", new String[]{"A","B"}, df3.getColumnNames());
        assertTrue("Columns reference does not match", df3.getColumn("A") == df1.getColumn("A"));
        assertTrue("Columns reference does not match", df3.getColumn("B") == df1.getColumn("B"));
    }
    
    @Test
    public void testIntersectionColumnsSameArg(){
        DataFrame df1 = new DefaultDataFrame(
                Column.create("A", "aaa","aab","aac"),
                Column.create("B", 1, 2, 3),
                Column.create("E", 1, 2, 3));

        DataFrame df3 = df1.intersectionColumns(df1);
        assertFalse("DataFrame has an invalid type", df3.isNullable());
        assertTrue("DataFrame should have 3 columns", df3.columns() == 3);
        assertTrue("DataFrame should have 3 rows", df3.rows() == 3);
        assertArrayEquals("Columns do not match", new String[]{"A","B","E"}, df3.getColumnNames());
        assertTrue("Columns reference does not match", df3.getColumn("A") == df1.getColumn("A"));
        assertTrue("Columns reference does not match", df3.getColumn("B") == df1.getColumn("B"));
        assertTrue("Columns reference does not match", df3.getColumn("E") == df1.getColumn("E"));
    }
    
    @Test(expected=DataFrameException.class)
    public void testDifferenceColumnsInvalidArg(){
        DataFrame df1 = new NullableDataFrame(
                Column.nullable("A", "aaa","aab","aac"));

        df.differenceColumns(df1);
    }
    
    @Test(expected=DataFrameException.class)
    public void testDifferenceColumnsEmptyArg(){
        DataFrame df1 = new DefaultDataFrame(
                Column.create("A", "aaa","aab","aac"),
                Column.create("B", 1, 2, 3),
                Column.create("E", 1, 2, 3));

        DataFrame df2 = new DefaultDataFrame();
        df1.differenceColumns(df2);
    }
    
    @Test(expected=DataFrameException.class)
    public void testUnionColumnsInvalidArg(){
        DataFrame df1 = new NullableDataFrame(
                Column.nullable("A", "aaa","aab","aac"));

        df.unionColumns(df1);
    }
    
    @Test(expected=DataFrameException.class)
    public void testUnionColumnsEmptyArg(){
        DataFrame df1 = new DefaultDataFrame(
                Column.create("A", "aaa","aab","aac"),
                Column.create("B", 1, 2, 3),
                Column.create("E", 1, 2, 3));

        DataFrame df2 = new DefaultDataFrame();
        df1.unionColumns(df2);
    }
    
    @Test(expected=DataFrameException.class)
    public void testIntersectionColumnsInvalidArg(){
        DataFrame df1 = new NullableDataFrame(
                Column.nullable("A", "aaa","aab","aac"));

        df.intersectionColumns(df1);
    }
    
    @Test(expected=DataFrameException.class)
    public void testIntersectionColumnsEmptyArg(){
        DataFrame df1 = new DefaultDataFrame(
                Column.create("A", "aaa","aab","aac"),
                Column.create("B", 1, 2, 3),
                Column.create("E", 1, 2, 3));

        DataFrame df2 = new DefaultDataFrame();
        df1.intersectionColumns(df2);
    }

    @Test
    public void testDifferenceRows(){
        DataFrame df1 = new DefaultDataFrame(
                Column.create("A", "aaa","aab","aac","aab"),
                Column.create("B", 1,2,3,2),
                Column.create("C", 1,2,3,2));

        DataFrame df2 = new DefaultDataFrame(
                Column.create("A", "bba","aab","bbc","aab"),
                Column.create("B", 1,2,3,2),
                Column.create("C", 1,2,3,2));
        
        DataFrame df3 = df1.differenceRows(df2);
        assertFalse("DataFrame has an invalid type", df3.isNullable());
        assertTrue("DataFrame should have 3 columns", df3.columns() == 3);
        assertTrue("DataFrame should have 4 rows", df3.rows() == 4);
        assertArrayEquals("Columns do not match", new String[]{"A","B","C"}, df3.getColumnNames());
        assertArrayEquals("Invalid row", new Object[]{"aaa",1,1}, df3.getRow(0));
        assertArrayEquals("Invalid row", new Object[]{"aac",3,3}, df3.getRow(1));
        assertArrayEquals("Invalid row", new Object[]{"bba",1,1}, df3.getRow(2));
        assertArrayEquals("Invalid row", new Object[]{"bbc",3,3}, df3.getRow(3));
    }
    
    @Test
    public void testDifferenceRowsUnlabeled(){
        DataFrame df1 = new DefaultDataFrame(
                new StringColumn(new String[]{"aaa","aab","aac","aab"}),
                new IntColumn(new int[]{1,2,3,2}),
                new IntColumn(new int[]{1,2,3,2}));

        DataFrame df2 = new DefaultDataFrame(
                new StringColumn(new String[]{"bba","aab","bbc","aab"}),
                new IntColumn(new int[]{1,2,3,2}),
                new IntColumn(new int[]{1,2,3,2}));
        
        DataFrame df3 = df1.differenceRows(df2);
        assertFalse("DataFrame has an invalid type", df3.isNullable());
        assertTrue("DataFrame should have 3 columns", df3.columns() == 3);
        assertTrue("DataFrame should have 4 rows", df3.rows() == 4);
        assertFalse("DataFrame should not have column names", df3.hasColumnNames());
        assertArrayEquals("Invalid row", new Object[]{"aaa",1,1}, df3.getRow(0));
        assertArrayEquals("Invalid row", new Object[]{"aac",3,3}, df3.getRow(1));
        assertArrayEquals("Invalid row", new Object[]{"bba",1,1}, df3.getRow(2));
        assertArrayEquals("Invalid row", new Object[]{"bbc",3,3}, df3.getRow(3));
    }
    
    @Test
    public void testDifferenceRowsSameArg(){
        DataFrame df1 = new DefaultDataFrame(
                Column.create("A", "aaa","aab","aac","aab"),
                Column.create("B", 1,2,3,2),
                Column.create("C", 1,2,3,2));

        DataFrame df3 = df1.differenceRows(df1);
        assertFalse("DataFrame has an invalid type", df3.isNullable());
        assertTrue("DataFrame should have 3 columns", df3.columns() == 3);
        assertTrue("DataFrame should have 0 rows", df3.rows() == 0);
        assertArrayEquals("Columns do not match", new String[]{"A","B","C"}, df3.getColumnNames());
    }
    
    @Test
    public void testUnionRows(){
        DataFrame df1 = new DefaultDataFrame(
                Column.create("A", "aaa","aab","aac","aab"),
                Column.create("B", 1,2,3,2),
                Column.create("C", 1,2,3,2));

        DataFrame df2 = new DefaultDataFrame(
                Column.create("A", "bba","aab","bbc"),
                Column.create("B", 1,2,3),
                Column.create("C", 1,2,3));
        
        DataFrame df3 = df1.unionRows(df2);
        assertFalse("DataFrame has an invalid type", df3.isNullable());
        assertTrue("DataFrame should have 3 columns", df3.columns() == 3);
        assertTrue("DataFrame should have 5 rows", df3.rows() == 5);
        assertArrayEquals("Columns do not match", new String[]{"A","B","C"}, df3.getColumnNames());
        assertArrayEquals("Invalid row", new Object[]{"aaa",1,1}, df3.getRow(0));
        assertArrayEquals("Invalid row", new Object[]{"aab",2,2}, df3.getRow(1));
        assertArrayEquals("Invalid row", new Object[]{"aac",3,3}, df3.getRow(2));
        assertArrayEquals("Invalid row", new Object[]{"bba",1,1}, df3.getRow(3));
        assertArrayEquals("Invalid row", new Object[]{"bbc",3,3}, df3.getRow(4));
    }
    
    @Test
    public void testUnionRowsUnlabeled(){
        DataFrame df1 = new DefaultDataFrame(
                new StringColumn(new String[]{"aaa","aab","aac","aab"}),
                new IntColumn(new int[]{1,2,3,2}),
                new IntColumn(new int[]{1,2,3,2}));

        DataFrame df2 = new DefaultDataFrame(
                new StringColumn(new String[]{"bba","aab","bbc"}),
                new IntColumn(new int[]{1,2,3}),
                new IntColumn(new int[]{1,2,3}));
        
        DataFrame df3 = df1.unionRows(df2);
        assertFalse("DataFrame has an invalid type", df3.isNullable());
        assertTrue("DataFrame should have 3 columns", df3.columns() == 3);
        assertTrue("DataFrame should have 5 rows", df3.rows() == 5);
        assertFalse("DataFrame should not have column names", df3.hasColumnNames());
        assertArrayEquals("Invalid row", new Object[]{"aaa",1,1}, df3.getRow(0));
        assertArrayEquals("Invalid row", new Object[]{"aab",2,2}, df3.getRow(1));
        assertArrayEquals("Invalid row", new Object[]{"aac",3,3}, df3.getRow(2));
        assertArrayEquals("Invalid row", new Object[]{"bba",1,1}, df3.getRow(3));
        assertArrayEquals("Invalid row", new Object[]{"bbc",3,3}, df3.getRow(4));
    }
    
    @Test
    public void testUnionRowsSameArg(){
        DataFrame df1 = new DefaultDataFrame(
                Column.create("A", "aaa","aab","aac","aab"),
                Column.create("B", 1,2,3,2),
                Column.create("C", 1,2,3,2));

        DataFrame df3 = df1.unionRows(df1);
        assertFalse("DataFrame has an invalid type", df3.isNullable());
        assertTrue("DataFrame should have 3 columns", df3.columns() == 3);
        assertTrue("DataFrame should have 3 rows", df3.rows() == 3);
        assertArrayEquals("Columns do not match", new String[]{"A","B","C"}, df3.getColumnNames());
        assertArrayEquals("Invalid row", new Object[]{"aaa",1,1}, df3.getRow(0));
        assertArrayEquals("Invalid row", new Object[]{"aab",2,2}, df3.getRow(1));
        assertArrayEquals("Invalid row", new Object[]{"aac",3,3}, df3.getRow(2));
    }
    
    @Test
    public void testIntersectionRows(){
        DataFrame df1 = new DefaultDataFrame(
                Column.create("A", "aaa","aab","aac","aab"),
                Column.create("B", 1,2,3,2),
                Column.create("C", 1,2,3,2));

        DataFrame df2 = new DefaultDataFrame(
                Column.create("A", "bba","aab","bbc","aab"),
                Column.create("B", 1,2,3,2),
                Column.create("C", 1,2,3,2));
        
        DataFrame df3 = df1.intersectionRows(df2);
        assertFalse("DataFrame has an invalid type", df3.isNullable());
        assertTrue("DataFrame should have 3 columns", df3.columns() == 3);
        assertTrue("DataFrame should have 1 rows", df3.rows() == 1);
        assertArrayEquals("Columns do not match", new String[]{"A","B","C"}, df3.getColumnNames());
        assertArrayEquals("Invalid row", new Object[]{"aab",2,2}, df3.getRow(0));
    }
    
    @Test
    public void testIntersectionRowsUnlabeled(){
        DataFrame df1 = new DefaultDataFrame(
                new StringColumn(new String[]{"aaa","aab","aac","aab"}),
                new IntColumn(new int[]{1,2,3,2}),
                new IntColumn(new int[]{1,2,3,2}));

        DataFrame df2 = new DefaultDataFrame(
                new StringColumn(new String[]{"bba","aab","bbc","aab"}),
                new IntColumn(new int[]{1,2,3,2}),
                new IntColumn(new int[]{1,2,3,2}));
        
        DataFrame df3 = df1.intersectionRows(df2);
        assertFalse("DataFrame has an invalid type", df3.isNullable());
        assertTrue("DataFrame should have 3 columns", df3.columns() == 3);
        assertTrue("DataFrame should have 1 rows", df3.rows() == 1);
        assertFalse("DataFrame should not have column names", df3.hasColumnNames());
        assertArrayEquals("Invalid row", new Object[]{"aab",2,2}, df3.getRow(0));
    }
    
    @Test
    public void testIntersectionRowsSameArg(){
        DataFrame df1 = new DefaultDataFrame(
                Column.create("A", "aaa","aab","aac","aab"),
                Column.create("B", 1,2,3,2),
                Column.create("C", 1,2,3,2));

        DataFrame df3 = df1.intersectionRows(df1);
        assertFalse("DataFrame has an invalid type", df3.isNullable());
        assertTrue("DataFrame should have 3 columns", df3.columns() == 3);
        assertTrue("DataFrame should have 3 rows", df3.rows() == 3);
        assertArrayEquals("Columns do not match", new String[]{"A","B","C"}, df3.getColumnNames());
        assertArrayEquals("Invalid row", new Object[]{"aaa",1,1}, df3.getRow(0));
        assertArrayEquals("Invalid row", new Object[]{"aab",2,2}, df3.getRow(1));
        assertArrayEquals("Invalid row", new Object[]{"aac",3,3}, df3.getRow(2));
    }
    
    @Test(expected=DataFrameException.class)
    public void testDifferenceRowsEmptyArg(){
        DataFrame df1 = new DefaultDataFrame(
                Column.create("A", "aaa","aab","aac"),
                Column.create("B", 1, 2, 3),
                Column.create("E", 1, 2, 3));

        DataFrame df2 = new DefaultDataFrame();
        df1.differenceRows(df2);
    }
    
    @Test(expected=DataFrameException.class)
    public void testUnionRowsEmptyArg(){
        DataFrame df1 = new DefaultDataFrame(
                Column.create("A", "aaa","aab","aac"),
                Column.create("B", 1, 2, 3),
                Column.create("E", 1, 2, 3));

        DataFrame df2 = new DefaultDataFrame();
        df1.unionRows(df2);
    }
    
    @Test(expected=DataFrameException.class)
    public void testIntersectionRowsEmptyArg(){
        DataFrame df1 = new DefaultDataFrame(
                Column.create("A", "aaa","aab","aac"),
                Column.create("B", 1, 2, 3),
                Column.create("E", 1, 2, 3));

        DataFrame df2 = new DefaultDataFrame();
        df1.intersectionRows(df2);
    }

    //****************************************//
    //           GroupBy operations           //
    //****************************************//

    @Test
    public void testGroupMinimumBy(){
        DataFrame df1 = new DefaultDataFrame(
                Column.create("A", 'a', 'b', 'c', 'b', 'b', 'a'),
                Column.create("B", "aaa","aab","aac","aab","aab","aaa"),
                Column.create("C", 5.5f,2.2f,3.3f,4.4f,1.1f,6.6f),
                Column.create("D", "bba","bbb","bbc","bbb","bbb","bba"),
                Column.create("E", 5, 2, 3, 4, 1, 6),
                Column.create("F", 5L, 2L, 3L, 4L, 1L, 6L));
        
        DataFrame df2 = df1.groupMinimumBy("B");
        assertFalse("DataFrame has an invalid type", df2.isNullable());
        assertTrue("DataFrame should have 4 columns", df2.columns() == 4);
        assertTrue("DataFrame should have 3 rows", df2.rows() == 3);
        assertArrayEquals("Columns do not match",
                new String[]{"B","C","E","F"}, df2.getColumnNames());

        df2.sortBy(0);
        DataFrame df3 = new DefaultDataFrame(
                Column.create("B", "aaa","aab","aac"),
                Column.create("C", 5.5f,1.1f,3.3f),
                Column.create("E", 5, 1, 3),
                Column.create("F", 5L, 1L, 3L));
        
        assertTrue("DataFrames are not equal", df2.equals(df3));
    }

    @Test
    public void testGroupMaximumBy(){
        DataFrame df1 = new DefaultDataFrame(
                Column.create("A", 'a', 'b', 'c', 'b', 'b', 'a'),
                Column.create("B", "aaa","aab","aac","aab","aab","aaa"),
                Column.create("C", 5.5f,2.2f,3.3f,4.4f,1.1f,6.6f),
                Column.create("D", "bba","bbb","bbc","bbb","bbb","bba"),
                Column.create("E", 5, 2, 3, 4, 1, 6),
                Column.create("F", 5L, 2L, 3L, 4L, 1L, 6L));
        
        DataFrame df2 = df1.groupMaximumBy("A");
        assertFalse("DataFrame has an invalid type", df2.isNullable());
        assertTrue("DataFrame should have 4 columns", df2.columns() == 4);
        assertTrue("DataFrame should have 3 rows", df2.rows() == 3);
        assertArrayEquals("Columns do not match",
                new String[]{"A","C","E","F"}, df2.getColumnNames());

        df2.sortBy(0);
        DataFrame df3 = new DefaultDataFrame(
                Column.create("A", 'a','b','c'),
                Column.create("C", 6.6f,4.4f,3.3f),
                Column.create("E", 6, 4, 3),
                Column.create("F", 6L, 4L, 3L));
        
        assertTrue("DataFrames are not equal", df2.equals(df3));
    }

    @Test
    public void testGroupAverageBy(){
        DataFrame df1 = new DefaultDataFrame(
                Column.create("A", 'a', 'b', 'c', 'b', 'b', 'a'),
                Column.create("B", "aaa","aab","aac","aab","aab","aaa"),
                Column.create("C", 5.5f,2.2f,3.3f,4.4f,1.1f,6.6f),
                Column.create("D", "bba","bbb","bbc","bbb","bbb","bba"),
                Column.create("E", 5, 2, 3, 4, 1, 6),
                Column.create("F", 5L, 2L, 3L, 4L, 1L, 6L));
        
        DataFrame df2 = df1.groupAverageBy("D");
        assertFalse("DataFrame has an invalid type", df2.isNullable());
        assertTrue("DataFrame should have 4 columns", df2.columns() == 4);
        assertTrue("DataFrame should have 3 rows", df2.rows() == 3);
        assertArrayEquals("Columns do not match",
                new String[]{"D","C","E","F"}, df2.getColumnNames());

        df2.sortBy(0);
        df2.round("C", 2);
        df2.round("E", 2);
        df2.round("F", 2);
        DataFrame df3 = new DefaultDataFrame(
                Column.create("D", "bba","bbb","bbc"),
                Column.create("C", 6.05,2.57,3.3),
                Column.create("E", 5.5, 2.33, 3.0),
                Column.create("F", 5.5, 2.33, 3.0));
        
        assertTrue("DataFrames are not equal", df2.equals(df3));
    }

    @Test
    public void testGroupSumBy(){
        DataFrame df1 = new DefaultDataFrame(
                Column.create("A", 'a', 'b', 'c', 'b', 'b', 'a'),
                Column.create("B", "aaa","aab","aac","aab","aab","aaa"),
                Column.create("C", 5.5f,2.2f,3.3f,4.4f,1.1f,6.6f),
                Column.create("D", "bba","bbb","bbc","bbb","bbb","bba"),
                Column.create("E", 5, 2, 3, 4, 1, 6),
                Column.create("F", 5L, 2L, 3L, 4L, 1L, 6L));
        
        DataFrame df2 = df1.groupSumBy("A");
        assertFalse("DataFrame has an invalid type", df2.isNullable());
        assertTrue("DataFrame should have 4 columns", df2.columns() == 4);
        assertTrue("DataFrame should have 3 rows", df2.rows() == 3);
        assertArrayEquals("Columns do not match",
                new String[]{"A","C","E","F"}, df2.getColumnNames());

        df2.sortBy(0);
        df2.round("C", 2);
        DataFrame df3 = new DefaultDataFrame(
                Column.create("A", 'a','b','c'),
                Column.create("C", 12.1,7.7,3.3),
                Column.create("E", 11.0, 7.0, 3.0),
                Column.create("F", 11.0, 7.0, 3.0));
        
        assertTrue("DataFrames are not equal", df2.equals(df3));
    }

    @Test
    public void testGroupMinimumEmpty(){
        DataFrame df1 = new DefaultDataFrame(
                Column.create("A", 'a', 'b', 'c', 'b', 'b', 'a'));
        
        DataFrame df2 = df1.groupMinimumBy("A");
        assertFalse("DataFrame has an invalid type", df2.isNullable());
        assertTrue("DataFrame should have 1 columns", df2.columns() == 1);
        assertTrue("DataFrame should have 3 rows", df2.rows() == 3);
        assertArrayEquals("Columns do not match",
                new String[]{"A"}, df2.getColumnNames());
    }

    @Test
    public void testGroupMaximumEmpty(){
        DataFrame df1 = new DefaultDataFrame(
                Column.create("A", 'a', 'b', 'c', 'b', 'b', 'a'));
        
        DataFrame df2 = df1.groupMaximumBy("A");
        assertFalse("DataFrame has an invalid type", df2.isNullable());
        assertTrue("DataFrame should have 1 columns", df2.columns() == 1);
        assertTrue("DataFrame should have 3 rows", df2.rows() == 3);
        assertArrayEquals("Columns do not match",
                new String[]{"A"}, df2.getColumnNames());
    }

    @Test
    public void testGroupAverageEmpty(){
        DataFrame df1 = new DefaultDataFrame(
                Column.create("A", 'a', 'b', 'c', 'b', 'b', 'a'));
        
        DataFrame df2 = df1.groupAverageBy("A");
        assertFalse("DataFrame has an invalid type", df2.isNullable());
        assertTrue("DataFrame should have 1 columns", df2.columns() == 1);
        assertTrue("DataFrame should have 3 rows", df2.rows() == 3);
        assertArrayEquals("Columns do not match",
                new String[]{"A"}, df2.getColumnNames());
    }

    @Test
    public void testGroupSumEmpty(){
        DataFrame df1 = new DefaultDataFrame(
                Column.create("A", 'a', 'b', 'c', 'b', 'b', 'a'));
        
        DataFrame df2 = df1.groupSumBy("A");
        assertFalse("DataFrame has an invalid type", df2.isNullable());
        assertTrue("DataFrame should have 1 columns", df2.columns() == 1);
        assertTrue("DataFrame should have 3 rows", df2.rows() == 3);
        assertArrayEquals("Columns do not match",
                new String[]{"A"}, df2.getColumnNames());
    }

    //*******************************************************************************************//
    //           Minimum, Maximum, Average, Median, Sum, absolute, Ceil, Floor, Round            //
    //*******************************************************************************************//

    @Test
    public void testMinimum(){
        assertTrue("Computed minimum should be 10", df.minimum(0) == 10.0);
        assertTrue("Computed minimum should be 11", df.minimum(1) == 11.0);
        assertTrue("Computed minimum should be 12", df.minimum(2) == 12.0);
        assertTrue("Computed minimum should be 13", df.minimum(3) == 13.0);
        assertEquals("Computed minimum should be 10.1", 10.1, df.minimum(6), 0.005);
        assertEquals("Computed minimum should be 11.1", 11.1, df.minimum(7), 0.005);
    }

    @Test
    public void testMinimumByName(){
        assertTrue("Computed minimum should be 10", df.minimum("byteCol") == 10.0);
        assertTrue("Computed minimum should be 11", df.minimum("shortCol") == 11.0);
        assertTrue("Computed minimum should be 12", df.minimum("intCol") == 12.0);
        assertTrue("Computed minimum should be 13", df.minimum("longCol") == 13.0);
        assertEquals("Computed minimum should be 10.1", 10.1, df.minimum("floatCol"), 0.005);
        assertEquals("Computed minimum should be 11.1", 11.1, df.minimum("doubleCol"), 0.005);
    }

    @Test
    public void testMinimumWithNaN(){
        df.clear();
        assertTrue("Computed minimum should be NaN", Double.isNaN(df.minimum("byteCol")));
    }

    @Test
    public void testMaximum(){
        assertTrue("Computed maximum should be 50", df.maximum(0) == 50.0);
        assertTrue("Computed maximum should be 51", df.maximum(1) == 51.0);
        assertTrue("Computed maximum should be 52", df.maximum(2) == 52.0);
        assertTrue("Computed maximum should be 53", df.maximum(3) == 53.0);
        assertEquals("Computed maximum should be 50.5", 50.5, df.maximum(6), 0.005);
        assertEquals("Computed maximum should be 51.5", 51.5, df.maximum(7), 0.005);
    }

    @Test
    public void testMaximumByName(){
        assertTrue("Computed maximum should be 50", df.maximum("byteCol") == 50.0);
        assertTrue("Computed maximum should be 51", df.maximum("shortCol") == 51.0);
        assertTrue("Computed maximum should be 52", df.maximum("intCol") == 52.0);
        assertTrue("Computed maximum should be 53", df.maximum("longCol") == 53.0);
        assertEquals("Computed maximum should be 50.5", 50.5, df.maximum("floatCol"), 0.005);
        assertEquals("Computed maximum should be 51.5", 51.5, df.maximum("doubleCol"), 0.005);
    }

    @Test
    public void testMaximumWithNaN(){
        df.clear();
        assertTrue("Computed maximum should be NaN", Double.isNaN(df.maximum("byteCol")));
    }

    @Test
    public void testAverage(){
        assertTrue("Computed average should be 30", df.average(0) == 30.0);
        assertTrue("Computed average should be 31", df.average(1) == 31.0);
        assertTrue("Computed average should be 32", df.average(2) == 32.0);
        assertTrue("Computed average should be 33", df.average(3) == 33.0);
        assertEquals("Computed average should be 30.3", 30.3, df.average(6), 0.005);
        assertEquals("Computed average should be 31.3", 31.3, df.average(7), 0.005);
    }

    @Test
    public void testAverageByName(){
        assertTrue("Computed average should be 30", df.average("byteCol") == 30.0);
        assertTrue("Computed average should be 31", df.average("shortCol") == 31.0);
        assertTrue("Computed average should be 32", df.average("intCol") == 32.0);
        assertTrue("Computed average should be 33", df.average("longCol") == 33.0);
        assertEquals("Computed average should be 30.3", 30.3, df.average("floatCol"), 0.005);
        assertEquals("Computed average should be 31.3", 31.3, df.average("doubleCol"), 0.005);
    }
    
    @Test
    public void testAverageWithNaN(){
        df.clear();
        assertTrue("Computed average should be NaN", Double.isNaN(df.average("byteCol")));
    }
    
    @Test
    public void testMedian(){
        assertTrue("Computed median should be 30", df.median(0) == 30.0);
        assertTrue("Computed median should be 31", df.median(1) == 31.0);
        assertTrue("Computed median should be 32", df.median(2) == 32.0);
        assertTrue("Computed median should be 33", df.median(3) == 33.0);
        assertEquals("Computed median should be 30.3", 30.3, df.median(6), 0.005);
        assertEquals("Computed median should be 31.3", 31.3, df.median(7), 0.005);
        
        df.addRow(new Object[]{(byte)127,(short)420,402,420l,"42",'A',420.2f,420.2d,true});
        
        assertTrue("Computed median should be 35", df.median(0) == 35.0);
        assertTrue("Computed median should be 36", df.median(1) == 36.0);
        assertTrue("Computed median should be 37", df.median(2) == 37.0);
        assertTrue("Computed median should be 38", df.median(3) == 38.0);
        assertEquals("Computed median should be 35.35", 35.35, df.median(6), 0.005);
        assertEquals("Computed median should be 36.35", 36.35, df.median(7), 0.005);
    }

    @Test
    public void testMedianByName(){
        assertTrue("Computed median should be 30", df.median("byteCol") == 30.0);
        assertTrue("Computed median should be 31", df.median("shortCol") == 31.0);
        assertTrue("Computed median should be 32", df.median("intCol") == 32.0);
        assertTrue("Computed median should be 33", df.median("longCol") == 33.0);
        assertEquals("Computed median should be 30.3", 30.3, df.median("floatCol"), 0.005);
        assertEquals("Computed median should be 31.3", 31.3, df.median("doubleCol"), 0.005);
        
        df.addRow(new Object[]{(byte)127,(short)420,420,420l,"42",'A',420.2f,420.2d,true});
        
        assertTrue("Computed median should be 35", df.median("byteCol") == 35.0);
        assertTrue("Computed median should be 36", df.median("shortCol") == 36.0);
        assertTrue("Computed median should be 37", df.median("intCol") == 37.0);
        assertTrue("Computed median should be 38", df.median("longCol") == 38.0);
        assertEquals("Computed median should be 35.35", 35.35, df.median("floatCol"), 0.005);
        assertEquals("Computed median should be 36.35", 36.35, df.median("doubleCol"), 0.005);
    }
    
    @Test
    public void testMedianWithNaN(){
        df.clear();
        assertTrue("Computed median should be NaN", Double.isNaN(df.median("byteCol")));
    }

    @Test
    public void testSum(){
        assertTrue("Computed sum should be 150", df.sum(0) == 150.0);
        assertTrue("Computed sum should be 155", df.sum(1) == 155.0);
        assertTrue("Computed sum should be 160", df.sum(2) == 160.0);
        assertTrue("Computed sum should be 165", df.sum(3) == 165.0);
        assertEquals("Computed sum should be 151.5", 151.5, df.sum(6), 0.005);
        assertEquals("Computed sum should be 156.5", 156.5, df.sum(7), 0.005);
    }

    @Test
    public void testSumByName(){
        assertTrue("Computed sum should be 150", df.sum("byteCol") == 150.0);
        assertTrue("Computed sum should be 155", df.sum("shortCol") == 155.0);
        assertTrue("Computed sum should be 160", df.sum("intCol") == 160.0);
        assertTrue("Computed sum should be 165", df.sum("longCol") == 165.0);
        assertEquals("Computed sum should be 151.5", 151.5, df.sum("floatCol"), 0.005);
        assertEquals("Computed sum should be 156.5", 156.5, df.sum("doubleCol"), 0.005);
    }

    @Test
    public void testSumWithNaN(){
        df.clear();
        assertTrue("Computed sum should be NaN", Double.isNaN(df.sum("byteCol")));
    }

    @Test
    public void testMinimumRank(){
        DataFrame res1 = toBeSorted.minimum(0, 1);
        DataFrame res2 = toBeSorted.minimum(1, 1);
        DataFrame res3 = toBeSorted.minimum(2, 1);
        DataFrame res4 = toBeSorted.minimum(3, 1);
        DataFrame res5 = toBeSorted.minimum(6, 1);
        DataFrame res6 = toBeSorted.minimum(7, 1);
        assertTrue("DataFrame should have 1 row", res1.rows() == 1);
        assertTrue("DataFrame should have 1 row", res2.rows() == 1);
        assertTrue("DataFrame should have 1 row", res3.rows() == 1);
        assertTrue("DataFrame should have 1 row", res4.rows() == 1);
        assertTrue("DataFrame should have 1 row", res5.rows() == 1);
        assertTrue("DataFrame should have 1 row", res6.rows() == 1);
        DataFrame truth = toBeSorted.clone().getRows(2, 3);
        assertTrue("DataFrames should be equal", res1.equals(truth));
        assertTrue("DataFrames should be equal", res2.equals(truth));
        assertTrue("DataFrames should be equal", res3.equals(truth));
        assertTrue("DataFrames should be equal", res4.equals(truth));
        assertTrue("DataFrames should be equal", res5.equals(truth));
        assertTrue("DataFrames should be equal", res6.equals(truth));
        assertArrayEquals("Column names should be equal",
                res1.getColumnNames(), toBeSorted.getColumnNames());
        
        res1 = toBeSorted.minimum(0, 3);
        res2 = toBeSorted.minimum(1, 3);
        res3 = toBeSorted.minimum(2, 3);
        res4 = toBeSorted.minimum(3, 3);
        res5 = toBeSorted.minimum(6, 3);
        res6 = toBeSorted.minimum(7, 3);
        assertTrue("DataFrame should have 3 row", res1.rows() == 3);
        assertTrue("DataFrame should have 3 row", res2.rows() == 3);
        assertTrue("DataFrame should have 3 row", res3.rows() == 3);
        assertTrue("DataFrame should have 3 row", res4.rows() == 3);
        assertTrue("DataFrame should have 3 row", res5.rows() == 3);
        assertTrue("DataFrame should have 3 row", res6.rows() == 3);
        truth = toBeSorted.clone();
        truth.clear();
        truth.addRow(toBeSorted.getRow(2));
        truth.addRow(toBeSorted.getRow(1));
        truth.addRow(toBeSorted.getRow(4));
        assertTrue("DataFrames should be equal", res1.equals(truth));
        assertTrue("DataFrames should be equal", res2.equals(truth));
        assertTrue("DataFrames should be equal", res3.equals(truth));
        assertTrue("DataFrames should be equal", res4.equals(truth));
        assertTrue("DataFrames should be equal", res5.equals(truth));
        assertTrue("DataFrames should be equal", res6.equals(truth));
        assertArrayEquals("Column names should be equal",
                res1.getColumnNames(), toBeSorted.getColumnNames());
    }
    
    @Test
    public void testMinimumRankByName(){
        DataFrame res1 = toBeSorted.minimum("byteCol", 1);
        DataFrame res2 = toBeSorted.minimum("shortCol", 1);
        DataFrame res3 = toBeSorted.minimum("intCol", 1);
        DataFrame res4 = toBeSorted.minimum("longCol", 1);
        DataFrame res5 = toBeSorted.minimum("floatCol", 1);
        DataFrame res6 = toBeSorted.minimum("doubleCol", 1);
        assertTrue("DataFrame should have 1 row", res1.rows() == 1);
        assertTrue("DataFrame should have 1 row", res2.rows() == 1);
        assertTrue("DataFrame should have 1 row", res3.rows() == 1);
        assertTrue("DataFrame should have 1 row", res4.rows() == 1);
        assertTrue("DataFrame should have 1 row", res5.rows() == 1);
        assertTrue("DataFrame should have 1 row", res6.rows() == 1);
        DataFrame truth = toBeSorted.clone().getRows(2, 3);
        assertTrue("DataFrames should be equal", res1.equals(truth));
        assertTrue("DataFrames should be equal", res2.equals(truth));
        assertTrue("DataFrames should be equal", res3.equals(truth));
        assertTrue("DataFrames should be equal", res4.equals(truth));
        assertTrue("DataFrames should be equal", res5.equals(truth));
        assertTrue("DataFrames should be equal", res6.equals(truth));
        assertArrayEquals("Column names should be equal",
                res1.getColumnNames(), toBeSorted.getColumnNames());
        
        res1 = toBeSorted.minimum("byteCol", 3);
        res2 = toBeSorted.minimum("shortCol", 3);
        res3 = toBeSorted.minimum("intCol", 3);
        res4 = toBeSorted.minimum("longCol", 3);
        res5 = toBeSorted.minimum("floatCol", 3);
        res6 = toBeSorted.minimum("doubleCol", 3);
        assertTrue("DataFrame should have 3 row", res1.rows() == 3);
        assertTrue("DataFrame should have 3 row", res2.rows() == 3);
        assertTrue("DataFrame should have 3 row", res3.rows() == 3);
        assertTrue("DataFrame should have 3 row", res4.rows() == 3);
        assertTrue("DataFrame should have 3 row", res5.rows() == 3);
        assertTrue("DataFrame should have 3 row", res6.rows() == 3);
        truth = toBeSorted.clone();
        truth.clear();
        truth.addRow(toBeSorted.getRow(2));
        truth.addRow(toBeSorted.getRow(1));
        truth.addRow(toBeSorted.getRow(4));
        assertTrue("DataFrames should be equal", res1.equals(truth));
        assertTrue("DataFrames should be equal", res2.equals(truth));
        assertTrue("DataFrames should be equal", res3.equals(truth));
        assertTrue("DataFrames should be equal", res4.equals(truth));
        assertTrue("DataFrames should be equal", res5.equals(truth));
        assertTrue("DataFrames should be equal", res6.equals(truth));
        assertArrayEquals("Column names should be equal",
                res1.getColumnNames(), toBeSorted.getColumnNames());
    }
    
    @Test
    public void testMinimumRankLarge(){
        DataFrame res1 = toBeSorted.minimum("byteCol", 15);
        DataFrame res2 = toBeSorted.minimum("shortCol", 15);
        DataFrame res3 = toBeSorted.minimum("intCol", 15);
        DataFrame res4 = toBeSorted.minimum("longCol", 15);
        DataFrame res5 = toBeSorted.minimum("floatCol", 15);
        DataFrame res6 = toBeSorted.minimum("doubleCol", 15);
        assertTrue("DataFrame should have 5 row", res1.rows() == 5);
        assertTrue("DataFrame should have 5 row", res2.rows() == 5);
        assertTrue("DataFrame should have 5 row", res3.rows() == 5);
        assertTrue("DataFrame should have 5 row", res4.rows() == 5);
        assertTrue("DataFrame should have 5 row", res5.rows() == 5);
        assertTrue("DataFrame should have 5 row", res6.rows() == 5);
        DataFrame truth = toBeSorted.clone();
        truth.clear();
        truth.addRow(toBeSorted.getRow(2));
        truth.addRow(toBeSorted.getRow(1));
        truth.addRow(toBeSorted.getRow(4));
        truth.addRow(toBeSorted.getRow(0));
        truth.addRow(toBeSorted.getRow(3));
        assertTrue("DataFrames should be equal", res1.equals(truth));
        assertTrue("DataFrames should be equal", res2.equals(truth));
        assertTrue("DataFrames should be equal", res3.equals(truth));
        assertTrue("DataFrames should be equal", res4.equals(truth));
        assertTrue("DataFrames should be equal", res5.equals(truth));
        assertTrue("DataFrames should be equal", res6.equals(truth));
        assertArrayEquals("Column names should be equal",
                res1.getColumnNames(), toBeSorted.getColumnNames());
    }
    
    @Test
    public void testMaximumRank(){
        DataFrame res1 = toBeSorted.maximum(0, 1);
        DataFrame res2 = toBeSorted.maximum(1, 1);
        DataFrame res3 = toBeSorted.maximum(2, 1);
        DataFrame res4 = toBeSorted.maximum(3, 1);
        DataFrame res5 = toBeSorted.maximum(6, 1);
        DataFrame res6 = toBeSorted.maximum(7, 1);
        assertTrue("DataFrame should have 1 row", res1.rows() == 1);
        assertTrue("DataFrame should have 1 row", res2.rows() == 1);
        assertTrue("DataFrame should have 1 row", res3.rows() == 1);
        assertTrue("DataFrame should have 1 row", res4.rows() == 1);
        assertTrue("DataFrame should have 1 row", res5.rows() == 1);
        assertTrue("DataFrame should have 1 row", res6.rows() == 1);
        DataFrame truth = toBeSorted.clone().getRows(3, 4);
        assertTrue("DataFrames should be equal", res1.equals(truth));
        assertTrue("DataFrames should be equal", res2.equals(truth));
        assertTrue("DataFrames should be equal", res3.equals(truth));
        assertTrue("DataFrames should be equal", res4.equals(truth));
        assertTrue("DataFrames should be equal", res5.equals(truth));
        assertTrue("DataFrames should be equal", res6.equals(truth));
        assertArrayEquals("Column names should be equal",
                res1.getColumnNames(), toBeSorted.getColumnNames());
        
        res1 = toBeSorted.maximum(0, 3);
        res2 = toBeSorted.maximum(1, 3);
        res3 = toBeSorted.maximum(2, 3);
        res4 = toBeSorted.maximum(3, 3);
        res5 = toBeSorted.maximum(6, 3);
        res6 = toBeSorted.maximum(7, 3);
        assertTrue("DataFrame should have 3 row", res1.rows() == 3);
        assertTrue("DataFrame should have 3 row", res2.rows() == 3);
        assertTrue("DataFrame should have 3 row", res3.rows() == 3);
        assertTrue("DataFrame should have 3 row", res4.rows() == 3);
        assertTrue("DataFrame should have 3 row", res5.rows() == 3);
        assertTrue("DataFrame should have 3 row", res6.rows() == 3);
        truth = toBeSorted.clone();
        truth.clear();
        truth.addRow(toBeSorted.getRow(3));
        truth.addRow(toBeSorted.getRow(0));
        truth.addRow(toBeSorted.getRow(4));
        assertTrue("DataFrames should be equal", res1.equals(truth));
        assertTrue("DataFrames should be equal", res2.equals(truth));
        assertTrue("DataFrames should be equal", res3.equals(truth));
        assertTrue("DataFrames should be equal", res4.equals(truth));
        assertTrue("DataFrames should be equal", res5.equals(truth));
        assertTrue("DataFrames should be equal", res6.equals(truth));
        assertArrayEquals("Column names should be equal",
                res1.getColumnNames(), toBeSorted.getColumnNames());
    }
    
    @Test
    public void testMaximumRankByName(){
        DataFrame res1 = toBeSorted.maximum("byteCol", 1);
        DataFrame res2 = toBeSorted.maximum("shortCol", 1);
        DataFrame res3 = toBeSorted.maximum("intCol", 1);
        DataFrame res4 = toBeSorted.maximum("longCol", 1);
        DataFrame res5 = toBeSorted.maximum("floatCol", 1);
        DataFrame res6 = toBeSorted.maximum("doubleCol", 1);
        assertTrue("DataFrame should have 1 row", res1.rows() == 1);
        assertTrue("DataFrame should have 1 row", res2.rows() == 1);
        assertTrue("DataFrame should have 1 row", res3.rows() == 1);
        assertTrue("DataFrame should have 1 row", res4.rows() == 1);
        assertTrue("DataFrame should have 1 row", res5.rows() == 1);
        assertTrue("DataFrame should have 1 row", res6.rows() == 1);
        DataFrame truth = toBeSorted.clone().getRows(3, 4);
        assertTrue("DataFrames should be equal", res1.equals(truth));
        assertTrue("DataFrames should be equal", res2.equals(truth));
        assertTrue("DataFrames should be equal", res3.equals(truth));
        assertTrue("DataFrames should be equal", res4.equals(truth));
        assertTrue("DataFrames should be equal", res5.equals(truth));
        assertTrue("DataFrames should be equal", res6.equals(truth));
        assertArrayEquals("Column names should be equal",
                res1.getColumnNames(), toBeSorted.getColumnNames());
        
        res1 = toBeSorted.maximum("byteCol", 3);
        res2 = toBeSorted.maximum("shortCol", 3);
        res3 = toBeSorted.maximum("intCol", 3);
        res4 = toBeSorted.maximum("longCol", 3);
        res5 = toBeSorted.maximum("floatCol", 3);
        res6 = toBeSorted.maximum("doubleCol", 3);
        assertTrue("DataFrame should have 3 row", res1.rows() == 3);
        assertTrue("DataFrame should have 3 row", res2.rows() == 3);
        assertTrue("DataFrame should have 3 row", res3.rows() == 3);
        assertTrue("DataFrame should have 3 row", res4.rows() == 3);
        assertTrue("DataFrame should have 3 row", res5.rows() == 3);
        assertTrue("DataFrame should have 3 row", res6.rows() == 3);
        truth = toBeSorted.clone();
        truth.clear();
        truth.addRow(toBeSorted.getRow(3));
        truth.addRow(toBeSorted.getRow(0));
        truth.addRow(toBeSorted.getRow(4));
        assertTrue("DataFrames should be equal", res1.equals(truth));
        assertTrue("DataFrames should be equal", res2.equals(truth));
        assertTrue("DataFrames should be equal", res3.equals(truth));
        assertTrue("DataFrames should be equal", res4.equals(truth));
        assertTrue("DataFrames should be equal", res5.equals(truth));
        assertTrue("DataFrames should be equal", res6.equals(truth));
        assertArrayEquals("Column names should be equal",
                res1.getColumnNames(), toBeSorted.getColumnNames());
    }
    
    @Test
    public void testMaximumRankLarge(){
        DataFrame res1 = toBeSorted.maximum("byteCol", 15);
        DataFrame res2 = toBeSorted.maximum("shortCol", 15);
        DataFrame res3 = toBeSorted.maximum("intCol", 15);
        DataFrame res4 = toBeSorted.maximum("longCol", 15);
        DataFrame res5 = toBeSorted.maximum("floatCol", 15);
        DataFrame res6 = toBeSorted.maximum("doubleCol", 15);
        assertTrue("DataFrame should have 5 row", res1.rows() == 5);
        assertTrue("DataFrame should have 5 row", res2.rows() == 5);
        assertTrue("DataFrame should have 5 row", res3.rows() == 5);
        assertTrue("DataFrame should have 5 row", res4.rows() == 5);
        assertTrue("DataFrame should have 5 row", res5.rows() == 5);
        assertTrue("DataFrame should have 5 row", res6.rows() == 5);
        DataFrame truth = toBeSorted.clone();
        truth.clear();
        truth.addRow(toBeSorted.getRow(3));
        truth.addRow(toBeSorted.getRow(0));
        truth.addRow(toBeSorted.getRow(4));
        truth.addRow(toBeSorted.getRow(1));
        truth.addRow(toBeSorted.getRow(2));
        assertTrue("DataFrames should be equal", res1.equals(truth));
        assertTrue("DataFrames should be equal", res2.equals(truth));
        assertTrue("DataFrames should be equal", res3.equals(truth));
        assertTrue("DataFrames should be equal", res4.equals(truth));
        assertTrue("DataFrames should be equal", res5.equals(truth));
        assertTrue("DataFrames should be equal", res6.equals(truth));
        assertArrayEquals("Column names should be equal",
                res1.getColumnNames(), toBeSorted.getColumnNames());
    }
    
    @Test(expected=DataFrameException.class)
    public void testMinimumException(){
        df.minimum("stringCol");
    }

    @Test(expected=DataFrameException.class)
    public void testMaximumException(){
        df.maximum("stringCol");
    }

    @Test(expected=DataFrameException.class)
    public void testAverageException(){
        df.average("stringCol");
    }
    
    @Test(expected=DataFrameException.class)
    public void testMedianException(){
        df.median("stringCol");
    }
    
    @Test(expected=DataFrameException.class)
    public void testSumException(){
        df.sum("stringCol");
    }
    
    @Test(expected=DataFrameException.class)
    public void testMinimumRankException(){
        df.minimum("stringCol", 3);
    }
    
    @Test(expected=DataFrameException.class)
    public void testMinimumInvalidRankException(){
        df.minimum("intCol", 0);
    }
    
    @Test(expected=DataFrameException.class)
    public void testMaximumRankException(){
        df.maximum("stringCol", 3);
    }
    
    @Test(expected=DataFrameException.class)
    public void testMaximumInvalidRankException(){
        df.maximum("intCol", 0);
    }
    
    @Test
    public void testAbsolute(){
        df.setRow(2, (byte)-42,(short)-42,-42,-42l,"A",'a', -42.12f,-42.12, false);
        df.absolute("byteCol");
        df.absolute("shortCol");
        df.absolute("intCol");
        df.absolute("longCol");
        df.absolute("floatCol");
        df.absolute("doubleCol");      
        assertTrue("Value should be positive", df.getByte("byteCol", 2) == (byte)42);
        assertTrue("Value should be positive", df.getShort("shortCol", 2) == (short)42);
        assertTrue("Value should be positive", df.getInt("intCol", 2) == 42);
        assertTrue("Value should be positive", df.getLong("longCol", 2) == 42l);
        assertTrue("Value should be positive", df.getFloat("floatCol", 2) == 42.12f);
        assertTrue("Value should be positive", df.getDouble("doubleCol", 2) == 42.12);
    }
    
    @Test
    public void testCeil(){
        df.ceil("intCol");
        df.ceil("floatCol");
        df.ceil("doubleCol");
        assertArrayEquals("Column values are not equal", new int[]{12,22,32,42,52},
                ((IntColumn)df.getColumn("intCol")).asArray());
        
        assertArrayEquals("Column values are not equal", new float[]{11,21,31,41,51},
                ((FloatColumn)df.getColumn("floatCol")).asArray(), 0);
        
        assertArrayEquals("Column values are not equal", new double[]{12,22,32,42,52},
                ((DoubleColumn)df.getColumn("doubleCol")).asArray(), 0);
    }
    
    @Test
    public void testFloor(){
        df.floor("intCol");
        df.floor("floatCol");
        df.floor("doubleCol");
        assertArrayEquals("Column values are not equal", new int[]{12,22,32,42,52},
                ((IntColumn)df.getColumn("intCol")).asArray());
        
        assertArrayEquals("Column values are not equal", new float[]{10,20,30,40,50},
                ((FloatColumn)df.getColumn("floatCol")).asArray(), 0);
        
        assertArrayEquals("Column values are not equal", new double[]{11,21,31,41,51},
                ((DoubleColumn)df.getColumn("doubleCol")).asArray(), 0);
    }
    
    @Test
    public void testRound(){
        df.setColumn("floatCol", new FloatColumn(new float[]{10.2354f,20.2547f,30.256f,40.0f,50.515f}));
        df.setColumn("doubleCol", new DoubleColumn(new double[]{10.2354,20.2547,30.256,40.0,50.515}));
        df.round("intCol", 2);
        df.round("floatCol", 2);
        df.round("doubleCol", 2);
        assertArrayEquals("Column values are not equal", new int[]{12,22,32,42,52},
                ((IntColumn)df.getColumn("intCol")).asArray());
        
        assertArrayEquals("Column values are not equal", new float[]{10.24f,20.25f,30.26f,40.0f,50.52f},
                ((FloatColumn)df.getColumn("floatCol")).asArray(), 0.1f);
        
        assertArrayEquals("Column values are not equal", new double[]{10.24,20.25,30.26,40.0,50.52},
                ((DoubleColumn)df.getColumn("doubleCol")).asArray(), 0.1);
        
        df.round("floatCol", 0);
        df.round("doubleCol", 0);
        assertArrayEquals("Column values are not equal", new float[]{10.0f,20.0f,30.0f,40.0f,51.0f},
                ((FloatColumn)df.getColumn("floatCol")).asArray(), 0.1f);
        
        assertArrayEquals("Column values are not equal", new double[]{10.0,20.0,30.0,40.0,51.0},
                ((DoubleColumn)df.getColumn("doubleCol")).asArray(), 0.1);
    }
    
    @Test(expected=DataFrameException.class)
    public void testAbsoluteException(){
        df.absolute("stringCol");
    }
    
    @Test(expected=DataFrameException.class)
    public void testCeilException(){
        df.ceil("stringCol");
    }
    
    @Test(expected=DataFrameException.class)
    public void testFloorException(){
        df.floor("stringCol");
    }
    
    @Test(expected=DataFrameException.class)
    public void testRoundException(){
        df.round("stringCol", 2);
    }
    
    @Test(expected=DataFrameException.class)
    public void testRoundInvalidArgException(){
        df.round("floatCol", -1);
    }
    
    //***************************//
    //           Clip            //
    //***************************//
    
    @Test
    public void testClip(){
        df.removeColumn("stringCol");
        df.removeColumn("charCol");
        df.removeColumn("booleanCol");
        df.clip("byteCol", 20.0, 40.0);
        df.clip("shortCol", 20.0, 40.0);
        df.clip("intCol", 20.0, 40.0);
        df.clip("longCol", 20.0, 40.0);
        df.clip("floatCol", 20.0, 40.0);
        df.clip("doubleCol", 20.0, 40.0);
        DataFrame truth = new DefaultDataFrame(
                Column.create("byteCol", (byte)20,(byte)20,(byte)30,(byte)40,(byte)40),
                Column.create("shortCol", (short)20,(short)21,(short)31,(short)40,(short)40),
                Column.create("intCol", 20,22,32,40,40),
                Column.create("longCol", 20l,23l,33l,40l,40l),
                Column.create("floatCol", 20.0f,20.2f,30.3f,40.0f,40.0f),
                Column.create("doubleCol", 20.0,21.2,31.3,40.0,40.0));
        
        assertTrue("DataFrame does not match expected content", df.equals(truth));
    }
    
    @Test
    public void testClipWithLowUnspecified(){
        df.removeColumn("stringCol");
        df.removeColumn("charCol");
        df.removeColumn("booleanCol");
        df.clip("byteCol", null, 40.0);
        df.clip("shortCol", null, 40.0);
        df.clip("intCol", null, 40.0);
        df.clip("longCol", null, 40.0);
        df.clip("floatCol", null, 40.0);
        df.clip("doubleCol", null, 40.0);
        DataFrame truth = new DefaultDataFrame(
                Column.create("byteCol", (byte)10,(byte)20,(byte)30,(byte)40,(byte)40),
                Column.create("shortCol", (short)11,(short)21,(short)31,(short)40,(short)40),
                Column.create("intCol", 12,22,32,40,40),
                Column.create("longCol", 13l,23l,33l,40l,40l),
                Column.create("floatCol", 10.1f,20.2f,30.3f,40.0f,40.0f),
                Column.create("doubleCol", 11.1,21.2,31.3,40.0,40.0));
        
        assertTrue("DataFrame does not match expected content", df.equals(truth));
    }
    
    @Test
    public void testClipWithHighUnspecified(){
        df.removeColumn("stringCol");
        df.removeColumn("charCol");
        df.removeColumn("booleanCol");
        df.clip("byteCol", 20.0, null);
        df.clip("shortCol", 20.0, null);
        df.clip("intCol", 20.0, null);
        df.clip("longCol", 20.0, null);
        df.clip("floatCol", 20.0, null);
        df.clip("doubleCol", 20.0, null);
        DataFrame truth = new DefaultDataFrame(
                Column.create("byteCol", (byte)20,(byte)20,(byte)30,(byte)40,(byte)50),
                Column.create("shortCol", (short)20,(short)21,(short)31,(short)41,(short)51),
                Column.create("intCol", 20,22,32,42,52),
                Column.create("longCol", 20l,23l,33l,43l,53l),
                Column.create("floatCol", 20.0f,20.2f,30.3f,40.4f,50.5f),
                Column.create("doubleCol", 20.0,21.2,31.3,41.4,51.5));
        
        assertTrue("DataFrame does not match expected content", df.equals(truth));
    }
    
    @Test(expected=DataFrameException.class)
    public void testClipInvalidColumnArgException(){
        df.clip("stringCol", 20.0, 40.0);
    }
    
    @Test(expected=DataFrameException.class)
    public void testClipInvalidRangeArgException(){
        df.clip("intCol", 3, 2);
    }

    //*************************//
    //         Sorting         //
    //*************************//

    @Test
    public void testSortAscendByByte(){
        toBeSorted.sortBy("byteCol");
        testDataFrameIsSortedAscend();
    }

    @Test
    public void testSortAscendByShort(){
        toBeSorted.sortBy("shortCol");
        testDataFrameIsSortedAscend();
    }

    @Test
    public void testSortAscendByInt(){
        toBeSorted.sortBy("intCol");
        testDataFrameIsSortedAscend();
    }

    @Test
    public void testSortAscendByLong(){
        toBeSorted.sortBy("longCol");
        testDataFrameIsSortedAscend();
    }

    @Test
    public void testSortAscendByString(){
        toBeSorted.sortBy("stringCol");
        testDataFrameIsSortedAscend();
    }

    @Test
    public void testSortAscendByChar(){
        toBeSorted.sortBy("charCol");
        testDataFrameIsSortedAscend();
    }

    @Test
    public void testSortAscendByFloat(){
        toBeSorted.sortBy("floatCol");
        testDataFrameIsSortedAscend();
    }

    @Test
    public void testSortAscendByDouble(){
        toBeSorted.sortBy("doubleCol");
        testDataFrameIsSortedAscend();
    }

    @Test
    public void testSortByBooleanAscend(){
        toBeSorted.sortBy("booleanCol");
        assertFalse(
                "Row does not match expected values at row index 0. DataFrame is not sorted correctly",
                toBeSorted.getBoolean("booleanCol", 0));
        assertFalse(
                "Row does not match expected values at row index 1. DataFrame is not sorted correctly",
                toBeSorted.getBoolean("booleanCol", 1));
        assertTrue(
                "Row does not match expected values at row index 2. DataFrame is not sorted correctly",
                toBeSorted.getBoolean("booleanCol", 2));
        assertTrue(
                "Row does not match expected values at row index 3. DataFrame is not sorted correctly",
                toBeSorted.getBoolean("booleanCol", 3));
        assertTrue(
                "Row does not match expected values at row index 4. DataFrame is not sorted correctly",
                toBeSorted.getBoolean("booleanCol", 4));
    }
    
    @Test
    public void testSortDescendByByte(){
        toBeSorted.sortDescendingBy("byteCol");
        testDataFrameIsSortedDescend();
    }

    @Test
    public void testSortDescendByShort(){
        toBeSorted.sortDescendingBy("shortCol");
        testDataFrameIsSortedDescend();
    }

    @Test
    public void testSortDescendByInt(){
        toBeSorted.sortDescendingBy("intCol");
        testDataFrameIsSortedDescend();
    }

    @Test
    public void testSortDescendByLong(){
        toBeSorted.sortDescendingBy("longCol");
        testDataFrameIsSortedDescend();
    }

    @Test
    public void testSortDescendByString(){
        toBeSorted.sortDescendingBy("stringCol");
        testDataFrameIsSortedDescend();
    }

    @Test
    public void testSortDescendByChar(){
        toBeSorted.sortDescendingBy("charCol");
        testDataFrameIsSortedDescend();
    }

    @Test
    public void testSortDescendByFloat(){
        toBeSorted.sortDescendingBy("floatCol");
        testDataFrameIsSortedDescend();
    }

    @Test
    public void testSortDescendByDouble(){
        toBeSorted.sortDescendingBy("doubleCol");
        testDataFrameIsSortedDescend();
    }
    
    @Test
    public void testSortByBooleanDescend(){
        toBeSorted.sortDescendingBy("booleanCol");
        assertTrue(
                "Row does not match expected values at row index 0. DataFrame is not sorted correctly",
                toBeSorted.getBoolean("booleanCol", 0));
        assertTrue(
                "Row does not match expected values at row index 1. DataFrame is not sorted correctly",
                toBeSorted.getBoolean("booleanCol", 1));
        assertTrue(
                "Row does not match expected values at row index 2. DataFrame is not sorted correctly",
                toBeSorted.getBoolean("booleanCol", 2));
        assertFalse(
                "Row does not match expected values at row index 3. DataFrame is not sorted correctly",
                toBeSorted.getBoolean("booleanCol", 3));
        assertFalse(
                "Row does not match expected values at row index 4. DataFrame is not sorted correctly",
                toBeSorted.getBoolean("booleanCol", 4));
    }

    @Test
    public void testSortAscendWithNaNs(){
        DataFrame df = new DefaultDataFrame(
                Column.create("A", 4, 2, 1, 5, 3),
                Column.create("B", "4", "2", "1", "5", "3"),
                Column.create("C", 4.0f, Float.NaN, 1.0f, Float.NaN, 3.0f),
                Column.create("D", Double.NaN, 2.0, 1.0, 5.0, Double.NaN));

        df.sortBy("C");
        float[] valsF = ((FloatColumn)df.getColumn("C")).asArray();
        int i = 0;
        for(float truth : new float[]{1.0f, 3.0f, 4.0f, Float.NaN, Float.NaN}){
            if(Float.isNaN(truth)){
                assertTrue("DataFrame is not sorted correctly", Float.isNaN(valsF[i]));
            }else{
                assertTrue("DataFrame is not sorted correctly", truth == valsF[i]);
            }
            ++i;
        }
        df.sortBy("D");
        double[] valsD = ((DoubleColumn)df.getColumn("D")).asArray();
        i = 0;
        for(double truth : new double[]{1.0, 2.0, 5.0, Double.NaN, Double.NaN}){
            if(Double.isNaN(truth)){
                assertTrue("DataFrame is not sorted correctly", Double.isNaN(valsD[i]));
            }else{
                assertTrue("DataFrame is not sorted correctly", truth == valsD[i]);
            }
            ++i;
        }
    }

    @Test
    public void testSortAscendOnlyNaNs(){
        Float nanF = Float.NaN;
        Double nanD = Double.NaN;
        DataFrame df = new DefaultDataFrame(
                Column.create("A", 4, 2, 1, 5, 3),
                Column.create("B", "4", "2", "1", "5", "3"),
                Column.create("C", nanF, nanF, nanF, nanF, nanF),
                Column.create("D", nanD, nanD, nanD, nanD, nanD));

        df.sortBy("C");
        float[] valsF = ((FloatColumn)df.getColumn("C")).asArray();
        for(float val : valsF){
            assertTrue("DataFrame is not sorted correctly", Float.isNaN(val));
        }
        df.sortBy("D");
        double[] valsD = ((DoubleColumn)df.getColumn("D")).asArray();
        for(double val : valsD){
            assertTrue("DataFrame is not sorted correctly", Double.isNaN(val));
        }
    }

    @Test
    public void testSortDescendWithNaNs(){
        DataFrame df = new DefaultDataFrame(
                Column.create("A", 4, 2, 1, 5, 3),
                Column.create("B", "4", "2", "1", "5", "3"),
                Column.create("C", 4.0f, Float.NaN, 1.0f, Float.NaN, 3.0f),
                Column.create("D", Double.NaN, 2.0, 1.0, 5.0, Double.NaN));

        df.sortDescendingBy("C");
        float[] valsF = ((FloatColumn)df.getColumn("C")).asArray();
        int i = 0;
        for(float truth : new float[]{4.0f, 3.0f, 1.0f, Float.NaN, Float.NaN}){
            if(Float.isNaN(truth)){
                assertTrue("DataFrame is not sorted correctly", Float.isNaN(valsF[i]));
            }else{
                assertTrue("DataFrame is not sorted correctly", truth == valsF[i]);
            }
            ++i;
        }
        df.sortDescendingBy("D");
        double[] valsD = ((DoubleColumn)df.getColumn("D")).asArray();
        i = 0;
        for(double truth : new double[]{5.0, 2.0, 1.0, Double.NaN, Double.NaN}){
            if(Double.isNaN(truth)){
                assertTrue("DataFrame is not sorted correctly", Double.isNaN(valsD[i]));
            }else{
                assertTrue("DataFrame is not sorted correctly", truth == valsD[i]);
            }
            ++i;
        }
    }

    @Test
    public void testSortDescendOnlyNaNs(){
        Float nanF = Float.NaN;
        Double nanD = Double.NaN;
        DataFrame df = new DefaultDataFrame(
                Column.create("A", 4, 2, 1, 5, 3),
                Column.create("B", "4", "2", "1", "5", "3"),
                Column.create("C", nanF, nanF, nanF, nanF, nanF),
                Column.create("D", nanD, nanD, nanD, nanD, nanD));

        df.sortDescendingBy("C");
        float[] valsF = ((FloatColumn)df.getColumn("C")).asArray();
        for(float val : valsF){
            assertTrue("DataFrame is not sorted correctly", Float.isNaN(val));
        }
        df.sortDescendingBy("D");
        double[] valsD = ((DoubleColumn)df.getColumn("D")).asArray();
        for(double val : valsD){
            assertTrue("DataFrame is not sorted correctly", Double.isNaN(val));
        }
    }

    public void testDataFrameIsSortedAscend(){
        assertArrayEquals(
                "Row does not match expected values at row index 0. DataFrame is not sorted correctly", 
                new Object[]{(byte)1,(short)1,1,1l,"1",'a',1.0f,1.0d,true}, 
                toBeSorted.getRow(0));
        
        assertArrayEquals(
                "Row does not match expected values at row index 1. DataFrame is not sorted correctly", 
                new Object[]{(byte)2,(short)2,2,2l,"2",'b',2.0f,2.0d,false}, 
                toBeSorted.getRow(1));
        
        assertArrayEquals(
                "Row does not match expected values at row index 2. DataFrame is not sorted correctly", 
                new Object[]{(byte)3,(short)3,3,3l,"3",'c',3.0f,3.0d,true}, 
                toBeSorted.getRow(2));
        
        assertArrayEquals(
                "Row does not match expected values at row index 3. DataFrame is not sorted correctly", 
                new Object[]{(byte)4,(short)4,4,4l,"4",'d',4.0f,4.0d,true}, 
                toBeSorted.getRow(3));

        assertArrayEquals(
                "Row does not match expected values at row index 4. DataFrame is not sorted correctly", 
                new Object[]{(byte)5,(short)5,5,5l,"5",'e',5.0f,5.0d,false}, 
                toBeSorted.getRow(4));
    }

    public void testDataFrameIsSortedDescend(){
        assertArrayEquals(
                "Row does not match expected values at row index 0. DataFrame is not sorted correctly", 
                new Object[]{(byte)5,(short)5,5,5l,"5",'e',5.0f,5.0d,false}, 
                toBeSorted.getRow(0));

        assertArrayEquals(
                "Row does not match expected values at row index 1. DataFrame is not sorted correctly", 
                new Object[]{(byte)4,(short)4,4,4l,"4",'d',4.0f,4.0d,true}, 
                toBeSorted.getRow(1));

        assertArrayEquals(
                "Row does not match expected values at row index 2. DataFrame is not sorted correctly", 
                new Object[]{(byte)3,(short)3,3,3l,"3",'c',3.0f,3.0d,true}, 
                toBeSorted.getRow(2));

        assertArrayEquals(
                "Row does not match expected values at row index 3. DataFrame is not sorted correctly", 
                new Object[]{(byte)2,(short)2,2,2l,"2",'b',2.0f,2.0d,false}, 
                toBeSorted.getRow(3));

        assertArrayEquals(
                "Row does not match expected values at row index 4. DataFrame is not sorted correctly", 
                new Object[]{(byte)1,(short)1,1,1l,"1",'a',1.0f,1.0d,true}, 
                toBeSorted.getRow(4));
    }

    //***************************************//
    //         Resizing and Flushing         //
    //***************************************//

    @Test
    public void testSpaceAlteration(){
        //initial row count is 5
        for(int i=0; i<5; ++i){//add 5 rows
            df.addRow(new Object[]{(byte)42,(short)42,42,42l,"42",'A',42.2f,42.2d,true});
        }
        assertTrue("Row count should be 10", df.rows() == 10);
        assertTrue("Capacity should be 10", df.capacity() == 10);
        //add another row to trigger resizing
        df.addRow(new Object[]{(byte)42,(short)42,42,42l,"42",'A',42.2f,42.2d,true});
        //one additional row but capacity should have doubled
        assertTrue("Row count should be 11", df.rows() == 11);
        assertTrue("Capacity should be 20", df.capacity() == 20);

        //add more rows
        for(int i=0; i<10; ++i){
            df.addRow(new Object[]{(byte)42,(short)42,42,42l,"42",'A',42.2f,42.2d,true});
        }
        assertTrue("Row count should be 21", df.rows() == 21);
        assertTrue("Capacity should be 40", df.capacity() == 40);
        //flush back to 21
        df.flush();
        assertTrue("Row count should be 21", df.rows() == 21);
        assertTrue("Capacity should be 21", df.capacity() == 21);
        df.addRow(new Object[]{(byte)42,(short)42,42,42l,"42",'A',42.2f,42.2d,true});
        assertTrue("Row count should be 22", df.rows() == 22);
        assertTrue("Capacity should be 42", df.capacity() == 42);

        //remove 19 rows which should cause an automatic flush operation
        //with an applied buffer of 4
        df.removeRows(0, 19);
        assertTrue("Row count should be 3", df.rows() == 3);
        assertTrue("Capacity should be 7", df.capacity() == 7);

        //add again
        for(int i=0; i<5; ++i){
            df.addRow(new Object[]{(byte)42,(short)42,42,42l,"42",'A',42.2f,42.2d,true});
        }
        assertTrue("Row count should be 8", df.rows() == 8);
        assertTrue("Capacity should be 14", df.capacity() == 14);
    }

    //***************************************//
    //          Equals and HashCode          //
    //***************************************//

    @Test
    public void testEqualsHashCodeContract(){
        DefaultDataFrame test1 = new DefaultDataFrame(
                new String[]{"BYTE","SHORT","INT","LONG","STRING"
                        ,"CHAR","FLOAT","DOUBLE","BOOLEAN"},
                new ByteColumn(new byte[]{1,2,3}),
                new ShortColumn(new short[]{1,2,3}),
                new IntColumn(new int[]{1,2,3}),
                new LongColumn(new long[]{1,2,3}),
                new StringColumn(new String[]{"1","2","3"}),
                new CharColumn(new char[]{'1','2','3'}),
                new FloatColumn(new float[]{1f,2f,3f}),
                new DoubleColumn(new double[]{1,2,3}),
                new BooleanColumn(new boolean[]{true,false,true}));

        DefaultDataFrame test2 = new DefaultDataFrame(
                new String[]{"BYTE","SHORT","INT","LONG","STRING"
                        ,"CHAR","FLOAT","DOUBLE","BOOLEAN"},
                new ByteColumn(new byte[]{1,2,3}),
                new ShortColumn(new short[]{1,2,3}),
                new IntColumn(new int[]{1,2,3}),
                new LongColumn(new long[]{1,2,3}),
                new StringColumn(new String[]{"1","2","3"}),
                new CharColumn(new char[]{'1','2','3'}),
                new FloatColumn(new float[]{1f,2f,3f}),
                new DoubleColumn(new double[]{1,2,3}),
                new BooleanColumn(new boolean[]{true,false,true}));

        assertTrue("Equals method should return true", test1.equals(test2));
        assertTrue("HashCode method should return the same hash code", test1.hashCode() == test2.hashCode());

        //change to make unequal
        test1.setByte("BYTE", 2, (byte)4);
        assertFalse("Equals method should return false", test1.equals(test2));
    }

}
