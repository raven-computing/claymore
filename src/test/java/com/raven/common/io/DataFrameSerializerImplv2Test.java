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

package com.raven.common.io;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.raven.common.io.DataFrameSerializer;
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
 * Tests for DataFrameSerializer implementation.<br>
 * This class tests the binary version 2 format.
 * 
 * @author Phil Gaiser
 *
 */
public class DataFrameSerializerImplv2Test {
	
	static String[] columnNames;
	static DataFrame dfDefault;
	static byte[] truthImplv2;
	static byte[] truthImplv2Compressed;
	
	static DataFrame dfNullable;
	static byte[] truthNullableImplv2;
	static byte[] truthNullableImplv2Compressed;
	
	static String truthBase64Implv2;
	static String truthNullableBase64Implv2;

	@BeforeClass
	public static void setUpBeforeClass(){
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
		
		dfDefault = new DefaultDataFrame(
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
		
		//expected uncompressed
		truthImplv2 = new byte[]{123,118,58,50,59,100,0,0,0,5,0,0,0,9,98,121,116,101,67,
				111,108,0,115,104,111,114,116,67,111,108,0,105,110,116,67,111,108,0,
				108,111,110,103,67,111,108,0,115,116,114,105,110,103,67,111,108,0,99,
				104,97,114,67,111,108,0,102,108,111,97,116,67,111,108,0,100,111,117,98,
				108,101,67,111,108,0,98,111,111,108,101,97,110,67,111,108,0,1,2,3,4,5,8,
				6,7,9,125,10,20,30,40,50,0,11,0,21,0,31,0,41,0,51,0,0,0,12,0,0,0,22,0,0,
				0,32,0,0,0,42,0,0,0,52,0,0,0,0,0,0,0,13,0,0,0,0,0,0,0,23,0,0,0,0,0,0,0,
				33,0,0,0,0,0,0,0,43,0,0,0,0,0,0,0,53,49,48,0,50,48,0,51,48,0,52,48,0,53,
				48,0,0,97,0,98,0,99,0,100,0,101,65,33,-103,-102,65,-95,-103,-102,65,-14,
				102,102,66,33,-103,-102,66,74,0,0,64,38,51,51,51,51,51,51,64,53,51,51,51,
				51,51,51,64,63,76,-52,-52,-52,-52,-51,64,68,-77,51,51,51,51,51,64,73,-64,
				0,0,0,0,0,-88};
		
		//expected compressed
		truthImplv2Compressed = new byte[]{100,102,-85,46,-77,50,-78,78,97,96,96,96,5,98,-50,-92,
				-54,-110,84,-25,-4,28,-122,-30,-116,-4,-94,18,16,35,51,15,76,-27,-28,-25,
				-91,-125,-59,75,-118,50,33,-84,-28,-116,-60,34,16,-99,-106,-109,-97,8,
				86,-110,-110,95,-102,-108,3,-42,-100,-108,-97,-97,-109,-102,-104,7,98,50,
				50,49,-77,-80,114,-80,-79,115,-42,114,-119,-56,105,24,49,112,51,-120,
				50,-56,51,104,50,24,3,45,-29,1,98,49,32,86,0,98,45,32,54,97,-128,0,94,40,
				45,14,-91,21,-95,-76,54,-108,54,53,52,96,48,50,96,48,54,96,48,49,96,48,
				53,96,96,72,100,72,98,72,102,72,97,72,117,84,-100,57,-53,113,33,16,127,
				74,75,115,2,-78,-99,-68,24,24,28,-44,-116,-63,-64,-63,20,74,-37,-5,-100,
				1,-126,-77,14,46,-101,33,124,-49,3,96,83,87,0,0,36,-20,57,-107};
		
		truthBase64Implv2 = "ZGarLrMysk5hYGBgBWLOpMqSVOf8HIbijPyiEhAjMw9M5eTnpYPFS4oyIa"
				+ "zkjMQiEJ2Wk58IVpKSX5qUA9aclJ+fk5qYB2IyMjGzsHKwsXPWconIaRgxcDOIMsgzaD"
				+ "IYAy3jAWIxIFYAYi0gNmGAAF4oLQ6lFaG0NpQ2NTRgMDJgMDZgMDFgMDVgYEhkSGJIZ"
				+ "khhSHVUnDnLcSEQf0pLcwKynbwYGBzUjMHAwRRK2/ucAYKzDi6bIXzPA2BTVwAAJOw5lQ==";
		
		
		//*************************************************//
		//                                                 //
		//           Data for NullableDataFrame            //
		//                                                 //
		//*************************************************//
		
		dfNullable = new NullableDataFrame(
				columnNames, 
				new NullableByteColumn(new Byte[]{
						10,null,null,0,50
						}),
				new NullableShortColumn(new Short[]{
						11,21,null,0,null
						}),
				new NullableIntColumn(new Integer[]{
						12,null,32,0,null
						}),
				new NullableLongColumn(new Long[]{
						null,null,33l,0l,53l
						}),
				new NullableStringColumn(new String[]{
						"ABCD","2!\"0,.",null,"","#5{=0>}"
						}),
				new NullableCharColumn(new Character[]{
						',','b',null,'d','\u0000'
						}),
				new NullableFloatColumn(new Float[]{
						10.1f,null,0.0f,null,50.5f
						}),
				new NullableDoubleColumn(new Double[]{
						null,0.0,0.0,null,51.5
						}),
				new NullableBooleanColumn(new Boolean[]{
						true,null,false,null,true
						}));
		
		truthNullableImplv2 = new byte[]{123,118,58,50,59,110,0,0,0,5,0,0,0,9,98,121,
				116,101,67,111,108,0,115,104,111,114,116,67,111,108,0,105,110,116,67,
				111,108,0,108,111,110,103,67,111,108,0,115,116,114,105,110,103,67,111,
				108,0,99,104,97,114,67,111,108,0,102,108,111,97,116,67,111,108,0,100,
				111,117,98,108,101,67,111,108,0,98,111,111,108,101,97,110,67,111,108,
				0,10,11,12,13,14,17,15,16,18,0,0,0,4,-42,-22,-77,64,125,10,0,0,0,50,0,
				11,0,21,0,0,0,0,0,0,0,0,0,12,0,0,0,0,0,0,0,32,0,0,0,0,0,0,0,0,0,0,0,0,
				0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,33,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
				0,53,65,66,67,68,0,50,33,34,48,44,46,0,0,0,35,53,123,61,48,62,125,0,0,
				44,0,98,0,0,0,100,0,0,65,33,-103,-102,0,0,0,0,0,0,0,0,0,0,0,0,66,74,0,
				0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,64,
				73,-64,0,0,0,0,0,-120,};
		
		truthNullableImplv2Compressed = new byte[]{100,102,-85,46,-77,50,-78,-50,99,96,
				96,96,5,98,-50,-92,-54,-110,84,-25,-4,28,-122,-30,-116,-4,-94,18,16,
				35,51,15,76,-27,-28,-25,-91,-125,-59,75,-118,50,33,-84,-28,-116,-60,
				34,16,-99,-106,-109,-97,8,86,-110,-110,95,-102,-108,3,-42,-100,-108,
				-97,-97,-109,-102,-104,7,98,114,113,-13,-16,-14,9,-14,11,8,1,13,103,
				-71,-10,106,-77,67,45,23,-112,101,-60,-64,-51,32,-54,0,3,60,80,90,
				-127,1,63,80,68,-29,-101,58,58,57,-69,48,24,41,42,25,-24,-24,1,-71,
				-54,-90,-43,-74,6,118,-75,12,12,58,12,73,64,110,10,3,-125,-93,-30,
				-52,89,-56,26,-100,-68,8,-40,0,4,14,-98,7,-64,116,7,0,-107,-117,44,32,};
		
		truthNullableBase64Implv2 = "ZGarLrMyss5jYGBgBWLOpMqSVOf8HIbijPyiEhAjMw9M5eTnpY"
				+ "PFS4oyIazkjMQiEJ2Wk58IVpKSX5qUA9aclJ+fk5qYB2JycfPw8gnyCwgBDWe59mqzQy"
				+ "0XkGXEwM0gygADPFBagQE/UETjmzo6ObswGCkqGejoAbnKptW2Bna1DAw6DElAbgoDg6P"
				+ "izFnIGpy8CNgABA6eB8B0BwCViywg";
		
	}

	@AfterClass
	public static void tearDownAfterClass(){ 
		columnNames = null;
		dfDefault = null;
		truthImplv2 = null;
		dfNullable = null;
		truthNullableImplv2 = null;
	}

	@Before
	public void setUp(){ }

	@After
	public void tearDown(){ }

	@Test
	public void testSerializationDefault() throws Exception{
		byte[] bytes = DataFrameSerializer.serialize(dfDefault);
		assertArrayEquals("Serialized Dataframe does not match expected bytes", truthImplv2, bytes);
	}
	
	@Test
	public void testSerializationDefaultCompress() throws Exception{
		byte[] bytes = DataFrameSerializer.serialize(dfDefault, true);
		assertArrayEquals("Serialized Dataframe does not match expected bytes", truthImplv2Compressed, bytes);
	}

	@Test
	public void testSerializationNullable() throws Exception{
		byte[] bytes = DataFrameSerializer.serialize(dfNullable);
		assertArrayEquals("Serialized Dataframe does not match expected bytes", truthNullableImplv2, bytes);
	}
	
	@Test
	public void testSerializationNullableCompressed() throws Exception{
		byte[] bytes = DataFrameSerializer.serialize(dfNullable, true);
		assertArrayEquals("Serialized Dataframe does not match expected bytes", truthNullableImplv2Compressed, bytes);
	}
	
	@Test
	public void testDeserializationDefault() throws Exception{
		DataFrame res = DataFrameSerializer.deserialize(truthImplv2);
		assertFalse("DataFrame should not be empty", res.isEmpty());
		assertTrue("DataFrame row count should be 5", res.rows() == 5);
		assertTrue("DataFrame column count should be 9", res.columns() == 9);
		assertTrue("DataFrame should have column names set", res.hasColumnNames());
		assertTrue("DataFrame should be of type DefaultDataFrame", res instanceof DefaultDataFrame);
		assertTrue("DataFrame differs in content", res.equals(dfDefault));
	}
	
	@Test
	public void testDeserializationDefaultCompressed() throws Exception{
		DataFrame res = DataFrameSerializer.deserialize(truthImplv2Compressed);
		assertFalse("DataFrame should not be empty", res.isEmpty());
		assertTrue("DataFrame row count should be 5", res.rows() == 5);
		assertTrue("DataFrame column count should be 9", res.columns() == 9);
		assertTrue("DataFrame should have column names set", res.hasColumnNames());
		assertTrue("DataFrame should be of type DefaultDataFrame", res instanceof DefaultDataFrame);
		assertTrue("DataFrame differs in content", res.equals(dfDefault));
	}
	
	@Test
	public void testDeserializationNullable() throws Exception{
		DataFrame res = DataFrameSerializer.deserialize(truthNullableImplv2);
		assertFalse("DataFrame should not be empty", res.isEmpty());
		assertTrue("DataFrame row count should be 5", res.rows() == 5);
		assertTrue("DataFrame column count should be 9", res.columns() == 9);
		assertTrue("DataFrame should have column names set", res.hasColumnNames());
		assertTrue("DataFrame should be of type NullableDataFrame", res instanceof NullableDataFrame);
		assertTrue("DataFrame differs in content", res.equals(dfNullable));
	}
	
	@Test
	public void testDeserializationNullableCompressed() throws Exception{
		DataFrame res = DataFrameSerializer.deserialize(truthNullableImplv2Compressed);
		assertFalse("DataFrame should not be empty", res.isEmpty());
		assertTrue("DataFrame row count should be 5", res.rows() == 5);
		assertTrue("DataFrame column count should be 9", res.columns() == 9);
		assertTrue("DataFrame should have column names set", res.hasColumnNames());
		assertTrue("DataFrame should be of type NullableDataFrame", res instanceof NullableDataFrame);
		assertTrue("DataFrame differs in content", res.equals(dfNullable));
	}
	
	@Test
	public void testToBase64StringDefault() throws Exception{
		String s = DataFrameSerializer.toBase64(dfDefault);
		assertEquals("Serialized Dataframe does not match expected Base64 string", truthBase64Implv2, s);
	}
	
	@Test
	public void testToBase64StringNullable() throws Exception{
		String s = DataFrameSerializer.toBase64(dfNullable);
		assertEquals("Serialized Dataframe does not match expected Base64 string", truthNullableBase64Implv2, s);
	}
	
	@Test
	public void testFromBase64StringDefault() throws Exception{
		DataFrame res = DataFrameSerializer.fromBase64(truthBase64Implv2);
		assertFalse("DataFrame should not be empty", res.isEmpty());
		assertTrue("DataFrame row count should be 5", res.rows() == 5);
		assertTrue("DataFrame column count should be 9", res.columns() == 9);
		assertTrue("DataFrame should have column names set", res.hasColumnNames());
		assertTrue("DataFrame should be of type DefaultDataFrame", res instanceof DefaultDataFrame);
		assertTrue("DataFrame differs in content", res.equals(dfDefault));
	}
	
	@Test
	public void testFromBase64StringNullable() throws Exception{
		DataFrame res = DataFrameSerializer.fromBase64(truthNullableBase64Implv2);
		assertFalse("DataFrame should not be empty", res.isEmpty());
		assertTrue("DataFrame row count should be 5", res.rows() == 5);
		assertTrue("DataFrame column count should be 9", res.columns() == 9);
		assertTrue("DataFrame should have column names set", res.hasColumnNames());
		assertTrue("DataFrame should be of type NullableDataFrame", res instanceof NullableDataFrame);
		assertTrue("DataFrame differs in content", res.equals(dfNullable));
	}

}
