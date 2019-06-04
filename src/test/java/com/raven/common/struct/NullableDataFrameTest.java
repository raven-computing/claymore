/* 
 * Copyright (C) 2019 Raven Computing
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

import com.raven.common.struct.Column;
import com.raven.common.struct.DataFrame;
import com.raven.common.struct.DataFrameException;
import com.raven.common.struct.DefaultDataFrame;
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

/**
 * Tests for NullableDataFrame implementation.
 * 
 * @author Phil Gaiser
 *
 */
public class NullableDataFrameTest {
	
	String[] columnNames;
	NullableDataFrame df;
	//DataFrame for sorting tests
	NullableDataFrame toBeSorted;

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
		
		df = new NullableDataFrame(
				columnNames, 
				new NullableByteColumn(new Byte[]{
						10,null,30,null,50
						}),
				new NullableShortColumn(new Short[]{
						11,null,31,null,51
						}),
				new NullableIntColumn(new Integer[]{
						12,null,32,null,52
						}),
				new NullableLongColumn(new Long[]{
						13l,null,33l,null,53l
						}),
				new NullableStringColumn(new String[]{
						"10",null,"30",null,"50"
						}),
				new NullableCharColumn(new Character[]{
						'a',null,'c',null,'e'
						}),
				new NullableFloatColumn(new Float[]{
						10.1f,null,30.3f,null,50.5f
						}),
				new NullableDoubleColumn(new Double[]{
						11.1,null,31.3,null,51.5
						}),
				new NullableBooleanColumn(new Boolean[]{
						true,null,true,null,true
						}));
		
		toBeSorted = new NullableDataFrame(
				columnNames, 
				new NullableByteColumn(new Byte[]{
						null,2,1,null,3
						}),
				new NullableShortColumn(new Short[]{
						null,2,1,null,3
						}),
				new NullableIntColumn(new Integer[]{
						null,2,1,null,3
						}),
				new NullableLongColumn(new Long[]{
						null,2l,1l,null,3l
						}),
				new NullableStringColumn(new String[]{
						null,"2","1",null,"3"
						}),
				new NullableCharColumn(new Character[]{
						null,'b','a',null,'c'
						}),
				new NullableFloatColumn(new Float[]{
						null,2.0f,1.0f,null,3.0f
						}),
				new NullableDoubleColumn(new Double[]{
						null,2.0,1.0,null,3.0
						}),
				new NullableBooleanColumn(new Boolean[]{
						null,false,true,null,true
						}));
	}

	@After
	public void tearDown(){ }
	
	//**************************//
	//        Constructors      //
	//**************************//

	@Test
	public void testConstructorNoArgs(){
		NullableDataFrame test = new NullableDataFrame();
		assertTrue("NullableDataFrame should be empty", test.isEmpty());
		assertTrue("NullableDataFrame row count should be 0", test.rows() == 0);
		assertTrue("NullableDataFrame column count should be 0", test.columns() == 0);
		assertFalse("NullableDataFrame should not have column names set", test.hasColumnNames());
	}

	@Test
	public void testConstructorWithColumns(){
		NullableDataFrame test = new NullableDataFrame(
				new NullableIntColumn(new int[]{1,2,3}),
				new NullableStringColumn(new String[]{"1","2","3"}),
				new NullableByteColumn(new byte[]{1,2,3}));
		
		assertFalse("NullableDataFrame should not be empty", test.isEmpty());
		assertTrue("NullableDataFrame row count should be 3", test.rows() == 3);
		assertTrue("NullableDataFrame column count should be 3", test.columns() == 3);
		assertFalse("NullableDataFrame should not have column names set", test.hasColumnNames());
	}
	
	@Test
	public void testConstructorWithLabeledColumns(){
		String[] names = new String[]{"myInt","myString","myByte"};
		NullableDataFrame test = new NullableDataFrame(
				new NullableIntColumn(names[0], new int[]{1,2,3}),
				new NullableStringColumn(names[1], new String[]{"1","2","3"}),
				new NullableByteColumn(names[2], new byte[]{1,2,3}));
		
		assertFalse("NullableDataFrame should not be empty", test.isEmpty());
		assertTrue("NullableDataFrame row count should be 3", test.rows() == 3);
		assertTrue("NullableDataFrame column count should be 3", test.columns() == 3);
		assertTrue("NullableDataFrame should have column names set", test.hasColumnNames());
		assertArrayEquals("NullableDataFrame column names do not match", names, test.getColumnNames());
	}

	@Test
	public void testConstructorWithColumnsAndNames(){
		NullableDataFrame test = new NullableDataFrame(
				new String[]{"col1","col2","col3"},
				new NullableIntColumn(new Integer[]{1,null,3}),
				new NullableStringColumn(new String[]{"1","2","3"}),
				new NullableByteColumn(new Byte[]{1,null,3}));
		
		assertFalse("NullableDataFrame should not be empty", test.isEmpty());
		assertTrue("NullableDataFrame row count should be 3", test.rows() == 3);
		assertTrue("NullableDataFrame column count should be 3", test.columns() == 3);
		assertTrue("NullableDataFrame should have column names set", test.hasColumnNames());
	}
	
	@Test
	public void testConstructorWithLabeledColumnsOverridden(){
		String[] names = new String[]{"col1","col2","col3"};
		NullableDataFrame test = new NullableDataFrame(
				names,
				new NullableIntColumn("myInt", new int[]{1,2,3}),
				new NullableStringColumn("myString", new String[]{"1","2","3"}),
				new NullableByteColumn("myByte", new byte[]{1,2,3}));
		
		assertFalse("NullableDataFrame should not be empty", test.isEmpty());
		assertTrue("NullableDataFrame row count should be 3", test.rows() == 3);
		assertTrue("NullableDataFrame column count should be 3", test.columns() == 3);
		assertTrue("NullableDataFrame should have column names set", test.hasColumnNames());
		assertArrayEquals("NullableDataFrame column names do not match", names, test.getColumnNames());
	}
	
	@Test
	public void testConstructorWithAnnotatedRow(){
		//the names of row items in the RowDummyNullable class
		String[] nTruth = new String[]{"BYTE","SHORT","INT","LONG","STRING","CHAR","FLOAT","DOUBLE","BOOLEAN"};
		NullableDataFrame test = new NullableDataFrame(RowDummyNullable.class);
		
		assertTrue("NullableDataFrame should be empty", test.isEmpty());
		assertTrue("NullableDataFrame row count should be 0", test.rows() == 0);
		assertTrue("NullableDataFrame column count should be 9", test.columns() == 9);
		assertTrue("NullableDataFrame should have column names set", test.hasColumnNames());
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
		assertTrue("Column should be of type NullableByteColumn", c instanceof NullableByteColumn);
		c = test.getColumn("SHORT");
		assertTrue("Column should be of type NullableShortColumn", c instanceof NullableShortColumn);
		c = test.getColumn("INT");
		assertTrue("Column should be of type NullableIntColumn", c instanceof NullableIntColumn);
		c = test.getColumn("LONG");
		assertTrue("Column should be of type NullableLongColumn", c instanceof NullableLongColumn);
		c = test.getColumn("STRING");
		assertTrue("Column should be of type NullableStringColumn", c instanceof NullableStringColumn);
		c = test.getColumn("CHAR");
		assertTrue("Column should be of type NullableCharColumn", c instanceof NullableCharColumn);
		c = test.getColumn("FLOAT");
		assertTrue("Column should be of type NullableFloatColumn", c instanceof NullableFloatColumn);
		c = test.getColumn("DOUBLE");
		assertTrue("Column should be of type NullableDoubleColumn", c instanceof NullableDoubleColumn);
		c = test.getColumn("BOOLEAN");
		assertTrue("Column should be of type NullableBooleanColumn", c instanceof NullableBooleanColumn);
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
		assertTrue("Short at index 3 should be null", df.getShort(1, 3) == null);
	}

	@Test
	public void testGetShortByName(){
		assertTrue("Short at index 3 should be null", df.getShort("shortCol", 3) == null);
	}
	
	@Test
	public void testGetIntByIndex(){
		assertTrue("Int at index 1 should be null", df.getInt(2, 1) == null);
	}

	@Test
	public void testGetIntByName(){
		assertTrue("Int at index 1 should be null", df.getInt("intCol", 1) == null);
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
		assertTrue("Float at index 1 should be null", df.getFloat(6, 1) == null);
	}

	@Test
	public void testGetFloatByName(){
		assertTrue("Float at index 1 should be null", df.getFloat("floatCol", 1) == null);
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
		assertTrue("Boolean at index 1 should be null", df.getBoolean(8, 1) == null);
	}

	@Test
	public void testGetBooleanByName(){
		assertTrue("Boolean at index 1 should be null", df.getBoolean("booleanCol", 1) == null);
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
				df.getColumnAt(3).getName());
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
				"NEW_NAME", df.getColumnAt(3).getName());
		
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
	public void testGetRowAt(){
		Object[] row = df.getRowAt(1);
		assertArrayEquals("Row does not match set values. Should contain only null", 
				new Object[]{null,null,null,null,null,null,null,null,null}, 
				row);
	}
	
	@Test
	public void testSetRowAt(){
		Object[] row = new Object[]{(byte)42,(short)42,null,42l,"42",'A',42.2f,null,true};
		df.setRowAt(1, row);
		assertArrayEquals("Row does not match set values", 
				new Object[]{(byte)42,(short)42,null,42l,"42",'A',42.2f,null,true}, 
				row);
	}
	
	@Test
	public void testAddRow(){
		df.addRow(new Object[]{(byte)42,(short)42,null,42l,"42",'A',42.2f,null,true});
		assertTrue("Row count should be 6", df.rows() == 6);
		Object[] row = df.getRowAt(5);
		assertArrayEquals("Row does not match added values", 
				new Object[]{(byte)42,(short)42,null,42l,"42",'A',42.2f,null,true}, 
				row);
	}
	
	@Test
	public void testInsertRowAt(){
		df.insertRowAt(2, new Object[]{(byte)42,(short)42,null,42l,"42",'A',42.2f,null,true});
		assertTrue("Row count should be 6", df.rows() == 6);
		Object[] row = df.getRowAt(2);
		assertArrayEquals("Row does not match inserted values", 
				new Object[]{(byte)42,(short)42,null,42l,"42",'A',42.2f,null,true}, 
				row);
	}
	
	@Test
	public void testInsertRowAtZero(){
		df.insertRowAt(0, new Object[]{(byte)42,(short)42,null,42l,"42",'A',42.2f,null,true});
		assertTrue("Row count should be 6", df.rows() == 6);
		Object[] row = df.getRowAt(0);
		assertArrayEquals("Row does not match inserted values", 
				new Object[]{(byte)42,(short)42,null,42l,"42",'A',42.2f,null,true}, 
				row);
	}
	
	@Test
	public void testInsertRowAtEnd(){
		df.insertRowAt(5, new Object[]{(byte)42,(short)42,42,42l,"42",'A',null,42.2d,true});
		assertTrue("Row count should be 6", df.rows() == 6);
		Object[] row = df.getRowAt(5);
		assertArrayEquals("Row does not match inserted values", 
				new Object[]{(byte)42,(short)42,42,42l,"42",'A',null,42.2d,true}, 
				row);
	}
	
	@Test
	public void testRemoveRow(){
		df.removeRow(1);
		assertTrue("Row count should be 4", df.rows() == 4);
		Object[] row = df.getRowAt(1);
		assertArrayEquals("Row does not match expected values", 
				new Object[]{(byte)30,(short)31,32,33l,"30",'c',30.3f,31.3d,true}, 
				row);
	}
	
	@Test
	public void testRemoveRows(){
		df.removeRows(1, 3);
		assertTrue("Row count should be 3", df.rows() == 3);
		Object[] row = df.getRowAt(1);
		assertArrayEquals("Row does not match expected values after removal point", 
				new Object[]{null,null,null,null,null,null,null,null,null}, 
				row);
		
		row = df.getRowAt(0);
		assertArrayEquals("Row does not match expected values before removal point", 
				new Object[]{(byte)10,(short)11,12,13l,"10",'a',10.1f,11.1d,true}, 
				row);
		
	}
	
	@Test
	public void testGetRowAtAnnotated(){
		NullableDataFrame test = new NullableDataFrame(
				new String[]{"BYTE","SHORT","INT","LONG","STRING"
						,"CHAR","FLOAT","DOUBLE","BOOLEAN"},
				new NullableByteColumn(new byte[]{1,2,3}),
				new NullableShortColumn(new short[]{1,2,3}),
				new NullableIntColumn(new int[]{1,2,3}),
				new NullableLongColumn(new long[]{1,2,3}),
				new NullableStringColumn(new String[]{"1","2","3"}),
				new NullableCharColumn(new char[]{'1','2','3'}),
				new NullableFloatColumn(new float[]{1f,2f,3f}),
				new NullableDoubleColumn(new double[]{1,2,3}),
				new NullableBooleanColumn(new boolean[]{true,false,true}));
		
		RowDummyNullable dummy = test.getRowAt(0, RowDummyNullable.class);
		assertTrue("Row does not match expected data", dummy.getmByte() == 1);
		assertTrue("Row does not match expected data", dummy.getmShort() == 1);
		assertTrue("Row does not match expected data", dummy.getmInt() == 1);
		assertTrue("Row does not match expected data", dummy.getmLong() == 1);
		assertEquals("Row does not match expected data", dummy.getmString(), "1");
		assertTrue("Row does not match expected data", dummy.getmFloat() == 1.0f);
		assertTrue("Row does not match expected data", dummy.getmDouble() == 1.0d);
		assertTrue("Row does not match expected data", dummy.getmChar() == '1');
		assertTrue("Row does not match expected data", dummy.getmBoolean());
		dummy = test.getRowAt(1, RowDummyNullable.class);
		assertTrue("Row does not match expected data", dummy.getmByte() == 2);
		assertTrue("Row does not match expected data", dummy.getmShort() == 2);
		assertTrue("Row does not match expected data", dummy.getmInt() == 2);
		assertTrue("Row does not match expected data", dummy.getmLong() == 2);
		assertEquals("Row does not match expected data", dummy.getmString(), "2");
		assertTrue("Row does not match expected data", dummy.getmFloat() == 2.0f);
		assertTrue("Row does not match expected data", dummy.getmDouble() == 2.0d);
		assertTrue("Row does not match expected data", dummy.getmChar() == '2');
		assertFalse("Row does not match expected data", dummy.getmBoolean());
		dummy = test.getRowAt(2, RowDummyNullable.class);
		assertTrue("Row does not match expected data", dummy.getmByte() == 3);
		assertTrue("Row does not match expected data", dummy.getmShort() == 3);
		assertTrue("Row does not match expected data", dummy.getmInt() == 3);
		assertTrue("Row does not match expected data", dummy.getmLong() == 3);
		assertEquals("Row does not match expected data", dummy.getmString(), "3");
		assertTrue("Row does not match expected data", dummy.getmFloat() == 3.0f);
		assertTrue("Row does not match expected data", dummy.getmDouble() == 3.0d);
		assertTrue("Row does not match expected data", dummy.getmChar() == '3');
		assertTrue("Row does not match expected data", dummy.getmBoolean());
	}
	
	@Test
	public void testSetRowAtAnnotated(){
		NullableDataFrame test = new NullableDataFrame(
				new String[]{"BYTE","SHORT","INT","LONG","STRING"
						,"CHAR","FLOAT","DOUBLE","BOOLEAN"},
				new NullableByteColumn(new byte[]{1,2,3}),
				new NullableShortColumn(new short[]{1,2,3}),
				new NullableIntColumn(new int[]{1,2,3}),
				new NullableLongColumn(new long[]{1,2,3}),
				new NullableStringColumn(new String[]{"1","2","3"}),
				new NullableCharColumn(new char[]{'1','2','3'}),
				new NullableFloatColumn(new float[]{1f,2f,3f}),
				new NullableDoubleColumn(new double[]{1,2,3}),
				new NullableBooleanColumn(new boolean[]{true,false,true}));
		
		RowDummyNullable dummy = new RowDummyNullable((byte)9, (short)9, null,
				9l, "TEST", 'T', 9.0f, null, true);
		
		test.setRowAt(1, dummy);
		assertTrue("Row does not match expected data", test.getByte(0, 1) == 9);
		assertTrue("Row does not match expected data", test.getShort(1, 1) == 9);
		assertNull("Row does not match expected data", test.getInt(2, 1));
		assertTrue("Row does not match expected data", test.getLong(3, 1) == 9l);
		assertEquals("Row does not match expected data", test.getString(4, 1), "TEST");
		assertTrue("Row does not match expected data", test.getChar(5, 1) == 'T');
		assertTrue("Row does not match expected data", test.getFloat(6, 1) == 9.0f);
		assertNull("Row does not match expected data", test.getDouble(7, 1));
		assertTrue("Row does not match expected data", test.getBoolean(8, 1));
	}
	
	@Test
	public void testAddRowAnnotated(){
		NullableDataFrame test = new NullableDataFrame(
				new String[]{"BYTE","SHORT","INT","LONG","STRING"
						,"CHAR","FLOAT","DOUBLE","BOOLEAN"},
				new NullableByteColumn(new byte[]{1,2,3}),
				new NullableShortColumn(new short[]{1,2,3}),
				new NullableIntColumn(new int[]{1,2,3}),
				new NullableLongColumn(new long[]{1,2,3}),
				new NullableStringColumn(new String[]{"1","2","3"}),
				new NullableCharColumn(new char[]{'1','2','3'}),
				new NullableFloatColumn(new float[]{1f,2f,3f}),
				new NullableDoubleColumn(new double[]{1,2,3}),
				new NullableBooleanColumn(new boolean[]{true,false,true}));
		
		RowDummyNullable dummy = new RowDummyNullable((byte)9, (short)9, null,
				9l, "TEST", 'T', 9.0f, null, true);
		
		test.addRow(dummy);
		assertFalse("NullableDataFrame should not be empty", test.isEmpty());
		assertTrue("NullableDataFrame row count should be 4", test.rows() == 4);
		assertTrue("NullableDataFrame column count should be 9", test.columns() == 9);
		
		assertTrue("Row does not match expected data", test.getByte(0, 3) == 9);
		assertTrue("Row does not match expected data", test.getShort(1, 3) == 9);
		assertNull("Row does not match expected data", test.getInt(2, 3));
		assertTrue("Row does not match expected data", test.getLong(3, 3) == 9l);
		assertEquals("Row does not match expected data", test.getString(4, 3), "TEST");
		assertTrue("Row does not match expected data", test.getChar(5, 3) == 'T');
		assertTrue("Row does not match expected data", test.getFloat(6, 3) == 9.0f);
		assertNull("Row does not match expected data", test.getDouble(7, 3));
		assertTrue("Row does not match expected data", test.getBoolean(8, 3));
		
	}
	
	@Test
	public void testInsertRowAtAnnotated(){
		NullableDataFrame test = new NullableDataFrame(
				new String[]{"BYTE","SHORT","INT","LONG","STRING"
						,"CHAR","FLOAT","DOUBLE","BOOLEAN"},
				new NullableByteColumn(new byte[]{1,2,3}),
				new NullableShortColumn(new short[]{1,2,3}),
				new NullableIntColumn(new int[]{1,2,3}),
				new NullableLongColumn(new long[]{1,2,3}),
				new NullableStringColumn(new String[]{"1","2","3"}),
				new NullableCharColumn(new char[]{'1','2','3'}),
				new NullableFloatColumn(new float[]{1f,2f,3f}),
				new NullableDoubleColumn(new double[]{1,2,3}),
				new NullableBooleanColumn(new boolean[]{true,false,true}));
		
		RowDummyNullable dummy = new RowDummyNullable((byte)9, (short)9, null,
				9l, "TEST", 'T', 9.0f, null, true);
		
		test.insertRowAt(1, dummy);
		assertFalse("NullableDataFrame should not be empty", test.isEmpty());
		assertTrue("NullableDataFrame row count should be 4", test.rows() == 4);
		assertTrue("NullableDataFrame column count should be 9", test.columns() == 9);
		
		//test inserted row
		assertTrue("Row does not match expected data", test.getByte(0, 1) == 9);
		assertTrue("Row does not match expected data", test.getShort(1, 1) == 9);
		assertNull("Row does not match expected data", test.getInt(2, 1));
		assertTrue("Row does not match expected data", test.getLong(3, 1) == 9l);
		assertEquals("Row does not match expected data", test.getString(4, 1), "TEST");
		assertTrue("Row does not match expected data", test.getChar(5, 1) == 'T');
		assertTrue("Row does not match expected data", test.getFloat(6, 1) == 9.0f);
		assertNull("Row does not match expected data", test.getDouble(7, 1));
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
	
	//***************************//
	//           Columns         //
	//***************************//
	
	@Test
	public void testAddColumn(){
		Column col = new NullableIntColumn(new int[]{0,1,2,3,4}); 
		df.addColumn(col);
		assertTrue("Column count should be 10", df.columns() == 10);
		assertSame("Column reference should be the same", col, df.getColumnAt(9));
	}
	
	@Test
	public void testAddColumnWithName(){
		Column col = new NullableIntColumn(new int[]{0,1,2,3,4}); 
		df.addColumn("INT", col);
		assertTrue("Column count should be 10", df.columns() == 10);
		assertSame("Column reference should be the same", col, df.getColumnAt(9));
		assertSame("Column reference should be the same", col, df.getColumn("INT"));
	}
	
	@Test
	public void testRemoveColumnByIndex(){
		df.removeColumn(3);
		assertTrue("Column count should be 8", df.columns() == 8);
		assertTrue("Column after removal point should be of type NullableStringColumn", 
				df.getColumnAt(3) instanceof NullableStringColumn);
		
		assertTrue("Column before removal point should be of type NullableIntColumn", 
				df.getColumnAt(2) instanceof NullableIntColumn);
	}
	
	@Test
	public void testRemoveColumnByName(){
		df.removeColumn("longCol");
		assertTrue("Column count should be 8", df.columns() == 8);
		assertTrue("Column after removal point should be of type NullableStringColumn", 
				df.getColumnAt(3) instanceof NullableStringColumn);
		
		assertTrue("Column before removal point should be of type NullableIntColumn", 
				df.getColumnAt(2) instanceof NullableIntColumn);
	}
	
	@Test
	public void testInsertColumnAt(){
		Column col = new NullableIntColumn(new int[]{0,1,2,3,4});
		df.insertColumnAt(2, col);
		assertTrue("Column count should be 10", df.columns() == 10);
		assertSame("Column reference should be the same", col, df.getColumnAt(2));
		
		assertTrue("Column after insertion point should be of type NullableIntColumn", 
				df.getColumnAt(3) instanceof NullableIntColumn);
		
		assertTrue("Column before insertion point should be of type NullableShortColumn", 
				df.getColumnAt(1) instanceof NullableShortColumn);
		
	}
	
	@Test
	public void testInsertColumnAtWithName(){
		Column col = new NullableIntColumn(new int[]{0,1,2,3,4});
		df.insertColumnAt(2, "INT", col);
		assertTrue("Column count should be 10", df.columns() == 10);
		assertSame("Column reference should be the same", col, df.getColumnAt(2));
		assertSame("Column reference should be the same", col, df.getColumn("INT"));
		
		assertTrue("Column after insertion point should be of type NullableIntColumn", 
				df.getColumnAt(3) instanceof NullableIntColumn);
		
		assertTrue("Column before insertion point should be of type NullableShortColumn", 
				df.getColumnAt(1) instanceof NullableShortColumn);
	}
	
	@Test
	public void testGetColumnAt(){
		Column col = df.getColumnAt(2);
		assertTrue("Column at index 2 should be of type NullableIntColumn", 
				col instanceof NullableIntColumn);
	}
	
	@Test
	public void testGetColumn(){
		Column col = df.getColumn("stringCol");
		assertTrue("Column \"stringCol\" should be of type NullableStringColumn", 
				col instanceof NullableStringColumn);
	}
	
	@Test
	public void testSetColumnAt(){
		Column col = new NullableIntColumn(new int[]{0,1,2,3,4}); 
		df.setColumnAt(3, col);
		Column col2 = df.getColumnAt(3);
		assertSame("References to columns should match", col, col2);
		assertTrue("Column count should be 9", df.columns() == 9);
	}
	
	//************************************************//
	//           Search and Filter operations         //
	//************************************************//
	
	@Test
	public void testIndexOf(){
		int i = df.indexOf(2, "52");
		assertTrue("Found index should be 4", i == 4);
		i = df.indexOf(2, "nothing");
		assertTrue("Returned index should be -1", i == -1);
	}
	
	@Test
	public void testIndexOfByName(){
		int i = df.indexOf("intCol", "52");
		assertTrue("Found index should be 4", i == 4);
		i = df.indexOf("intCol", "nothing");
		assertTrue("Returned index should be -1", i == -1);
	}
	
	@Test
	public void testIndexOfWithStartPoint(){
		int i = df.indexOf(2, 2, "52");
		assertTrue("Found index should be 4", i == 4);
		i = df.indexOf(2, 2, "nothing");
		assertTrue("Returned index should be -1", i == -1);
		i = df.indexOf(2, 1, "12");
		assertTrue("Returned index should be -1", i == -1);
	}
	
	@Test
	public void testIndexOfByNameWithStartPoint(){
		int i = df.indexOf("intCol", 2, "52");
		assertTrue("Found index should be 4", i == 4);
		i = df.indexOf("intCol", 2, "nothing");
		assertTrue("Returned index should be -1", i == -1);
		i = df.indexOf("intCol", 1, "12");
		assertTrue("Returned index should be -1", i == -1);
	}
	
	@Test
	public void testIndexOfAll(){
		int[] i = df.indexOfAll(2, "[1-4]2");
		assertTrue("Returned array should have length 2", i.length == 2);
		int[] truth = new int[]{0,2};
		assertArrayEquals("Content of the returned array does not match expected values", 
				truth,
				i);
		
		i = df.indexOfAll(2, "nothing");
		assertNull("Returned array should be null", i);
	}
	
	@Test
	public void testIndexOfAllByName(){
		int[] i = df.indexOfAll("intCol", "[1-4]2");
		assertTrue("Returned array should have length 2", i.length == 2);
		int[] truth = new int[]{0,2};
		assertArrayEquals("Content of the returned array does not match expected values", 
				truth,
				i);
		
		i = df.indexOfAll("intCol", "nothing");
		assertNull("Returned array should be null", i);
	}
	
	@Test
	public void testFilter(){
		DataFrame filtered = df.filter(2, "[1-4]2");
		assertNotNull("API violation: Returned DataFrame should not be null", filtered);
		assertFalse("Returned DataFrame should not be empty", filtered.isEmpty());
		assertTrue("Returned DataFrame should be of type NullableDefaultDataFrame", 
				filtered instanceof NullableDataFrame);
		
		assertTrue("Returned DataFrame should have 2 rows", filtered.rows() == 2);
		assertTrue("Returned DataFrame should have 9 columns", filtered.columns() == 9);
		
		assertTrue("Value at index 1 should be 32", filtered.getInt("intCol", 1) == 32);
		assertArrayEquals("Row does not match expected values", 
				new Object[]{(byte)10,(short)11,12,13l,"10",'a',10.1f,11.1d,true}, 
				filtered.getRowAt(0));
	}
	
	@Test
	public void testFilterByName(){
		DataFrame filtered = df.filter("intCol", "[1-4]2");
		assertNotNull("API violation: Returned DataFrame should not be null", filtered);
		assertFalse("Returned DataFrame should not be empty", filtered.isEmpty());
		assertTrue("Returned DataFrame should be of type NullableDataFrame", 
				filtered instanceof NullableDataFrame);
		
		assertTrue("Returned DataFrame should have 2 rows", filtered.rows() == 2);
		assertTrue("Returned DataFrame should have 9 columns", filtered.columns() == 9);
		
		assertTrue("Value at index 0 should be 12", filtered.getInt("intCol", 0) == 12);
		assertArrayEquals("Row does not match expected values", 
				new Object[]{(byte)10,(short)11,12,13l,"10",'a',10.1f,11.1d,true}, 
				filtered.getRowAt(0));
	}
	
	@Test
	public void testFilterNoMatch(){
		DataFrame filtered = df.filter(2, "[1-4]2Digit");
		assertNotNull("API violation: Returned DataFrame should not be null", filtered);
		assertTrue("Returned DataFrame should be empty", filtered.isEmpty());
		assertTrue("Returned DataFrame should be of type NullableDataFrame", 
				filtered instanceof NullableDataFrame);
		
		assertTrue("Returned DataFrame should have 0 rows", filtered.rows() == 0);
		assertTrue("Returned DataFrame should have 9 columns", filtered.columns() == 9);
	}
	
	//************************************************//
	//           Minimum, Maximum, Average            //
	//************************************************//
	
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
	
	//*************************//
	//         Sorting         //
	//*************************//
	
	@Test
	public void testSortByByte(){
		toBeSorted.sortBy("byteCol");
		testDataFrameIsSorted();
	}
	
	@Test
	public void testSortByShort(){
		toBeSorted.sortBy("shortCol");
		testDataFrameIsSorted();
	}
	
	@Test
	public void testSortByInt(){
		toBeSorted.sortBy("intCol");
		testDataFrameIsSorted();
	}
	
	@Test
	public void testSortByLong(){
		toBeSorted.sortBy("longCol");
		testDataFrameIsSorted();
	}
	
	@Test
	public void testSortByString(){
		toBeSorted.sortBy("stringCol");
		testDataFrameIsSorted();
	}
	
	@Test
	public void testSortByChar(){
		toBeSorted.sortBy("charCol");
		testDataFrameIsSorted();
	}
	
	@Test
	public void testSortByFloat(){
		toBeSorted.sortBy("floatCol");
		testDataFrameIsSorted();
	}
	
	@Test
	public void testSortByDouble(){
		toBeSorted.sortBy("doubleCol");
		testDataFrameIsSorted();
	}
	
	@Test
	public void testSortByBoolean(){
		toBeSorted.sortBy("booleanCol");
		assertFalse(
				"Row does not match expected values at row index 0. DataFrame is not sorted correctly",
				toBeSorted.getBoolean("booleanCol", 0));
		assertTrue(
				"Row does not match expected values at row index 1. DataFrame is not sorted correctly",
				toBeSorted.getBoolean("booleanCol", 1));
		assertTrue(
				"Row does not match expected values at row index 2. DataFrame is not sorted correctly",
				toBeSorted.getBoolean("booleanCol", 2));
		assertNull(
				"Row does not match expected values at row index 3 (Should be null)."
				+ " DataFrame is not sorted correctly",
				toBeSorted.getBoolean("booleanCol", 3));
		assertNull(
				"Row does not match expected values at row index 4 (Should be null)."
				+ " DataFrame is not sorted correctly",
				toBeSorted.getBoolean("booleanCol", 4));
	}
	
	public void testDataFrameIsSorted(){
		assertArrayEquals(
				"Row does not match expected values at row index 0. DataFrame is not sorted correctly", 
				new Object[]{(byte)1,(short)1,1,1l,"1",'a',1.0f,1.0d,true}, 
				toBeSorted.getRowAt(0));
		
		assertArrayEquals(
				"Row does not match expected values at row index 1. DataFrame is not sorted correctly", 
				new Object[]{(byte)2,(short)2,2,2l,"2",'b',2.0f,2.0d,false}, 
				toBeSorted.getRowAt(1));
		
		assertArrayEquals(
				"Row does not match expected values at row index 2. DataFrame is not sorted correctly", 
				new Object[]{(byte)3,(short)3,3,3l,"3",'c',3.0f,3.0d,true}, 
				toBeSorted.getRowAt(2));
		
		assertArrayEquals(
				"Row does not match expected values at row index 3 "
				+ "(Should contain only null). DataFrame is not sorted correctly", 
				new Object[]{null,null,null,null,null,null,null,null,null}, 
				toBeSorted.getRowAt(3));
		
		assertArrayEquals(
				"Row does not match expected values at row index 4 "
				+ "(Should contain only null). DataFrame is not sorted correctly", 
				new Object[]{null,null,null,null,null,null,null,null,null}, 
				toBeSorted.getRowAt(4));
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
		NullableDataFrame test1 = new NullableDataFrame(
				new String[]{"BYTE","SHORT","INT","LONG","STRING"
						,"CHAR","FLOAT","DOUBLE","BOOLEAN"},
				new NullableByteColumn(new Byte[]{1,null,3}),
				new NullableShortColumn(new short[]{1,2,3}),
				new NullableIntColumn(new int[]{1,2,3}),
				new NullableLongColumn(new long[]{1,2,3}),
				new NullableStringColumn(new String[]{"1","2","3"}),
				new NullableCharColumn(new char[]{'1','2','3'}),
				new NullableFloatColumn(new Float[]{null,2f,3f}),
				new NullableDoubleColumn(new double[]{1,2,3}),
				new NullableBooleanColumn(new boolean[]{true,false,true}));
		
		NullableDataFrame test2 = new NullableDataFrame(
				new String[]{"BYTE","SHORT","INT","LONG","STRING"
						,"CHAR","FLOAT","DOUBLE","BOOLEAN"},
				new NullableByteColumn(new Byte[]{1,null,3}),
				new NullableShortColumn(new short[]{1,2,3}),
				new NullableIntColumn(new int[]{1,2,3}),
				new NullableLongColumn(new long[]{1,2,3}),
				new NullableStringColumn(new String[]{"1","2","3"}),
				new NullableCharColumn(new char[]{'1','2','3'}),
				new NullableFloatColumn(new Float[]{null,2f,3f}),
				new NullableDoubleColumn(new double[]{1,2,3}),
				new NullableBooleanColumn(new boolean[]{true,false,true}));
		
		assertTrue("Equals method should return true", test1.equals(test2));
		assertTrue("HashCode method should return the same hash code", test1.hashCode() == test2.hashCode());
		
		//change to make unequal
		test1.setByte("BYTE", 2, (byte)4);
		assertFalse("Equals method should return false", test1.equals(test2));
	}

}
