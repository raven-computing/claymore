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

package com.raven.common.io;

import static org.junit.Assert.*;

import static com.raven.common.io.DataFrameSerializer.MODE_COMPRESSED;
import static com.raven.common.io.DataFrameSerializer.MODE_UNCOMPRESSED;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.raven.common.struct.BinaryColumn;
import com.raven.common.struct.BitVector;
import com.raven.common.struct.BooleanColumn;
import com.raven.common.struct.ByteColumn;
import com.raven.common.struct.CharColumn;
import com.raven.common.struct.Column;
import com.raven.common.struct.DataFrame;
import com.raven.common.struct.DefaultDataFrame;
import com.raven.common.struct.DoubleColumn;
import com.raven.common.struct.FloatColumn;
import com.raven.common.struct.IntColumn;
import com.raven.common.struct.LongColumn;
import com.raven.common.struct.NullableBinaryColumn;
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
                }),
                new BinaryColumn(new byte[][]{
                    new byte[]{1,2,3,4,5},
                    new byte[]{5,4,3,2,1},
                    new byte[]{5,2,1,2,3},
                    new byte[]{2,1,4,5,3},
                    new byte[]{3,1,2,5,4}
                }));

        // expected uncompressed
        truthImplv2 = BitVector.fromHexString(
                      "7b763a323b64000000050000000a62797465436f6c0073686f7274436f6c00696e74436f6"
                    + "c006c6f6e67436f6c00737472696e67436f6c0063686172436f6c00666c6f6174436f6c00"
                    + "646f75626c65436f6c00626f6f6c65616e436f6c0062696e617279436f6c0001020304050"
                    + "8060709137d0a141e2832000b0015001f002900330000000c00000016000000200000002a"
                    + "00000034000000000000000d00000000000000170000000000000021000000000000002b0"
                    + "00000000000003531300032300033300034300035300061626364654121999a41a1999a41"
                    + "f266664221999a424a000040263333333333334035333333333333403f4ccccccccccd404"
                    + "4b333333333334049c00000000000a8000000050102030405000000050504030201000000"
                    + "050502010203000000050201040503000000050301020504")
                      .asArray();

        // expected compressed
        truthImplv2Compressed = BitVector.fromHexString(
                      "6466ab2eb332b24e616060600562aea4ca9254e7fc1c86e28"
                    + "cfca2121023330f4ce5e4e7a583c54b8a3221ace48cc42210"
                    + "9d96939f085692925f9a9403d69c949f9f939a98076666e62"
                    + "5165582588c4ccc2cac1c6cec9cc2b55c22721a460cdc0ca2"
                    + "0cf20c9a0cc6407b7980580c881580580b884d182080174a8"
                    + "b43694528ad0da54d0d0d188c0c188c0d184c0c184c0d1812"
                    + "939253521d1567ce725c08c49fd2d29c806c272f060607356"
                    + "330703085d2f63e6780e0ac83cb6608dff300d8c015a08000"
                    + "3b15c460656166620433988062200613230b2b98c1ccc8c4c"
                    + "a020011103dae")
                      .asArray();
        

        truthBase64Implv2 = "ZGarLrMysk5hYGBgBWKupMqSVOf8HIbijPyiEhAjMw9M5eTnpYPFS4oyIaz"
                          + "kjMQiEJ2Wk58IVpKSX5qUA9aclJ+fk5qYB2Zm5iUWVYJYjEzMLKwcbOycwr"
                          + "VcInIaRgzcDKIM8gyaDMZAe3mAWAyIFYBYC4hNGCCAF0qLQ2lFKK0NpU0ND"
                          + "RiMDBiMDRhMDBhMDRgSk5JTUh0VZ85yXAjEn9LSnIBsJy8GBgc1YzBwMIXS"
                          + "9j5ngOCsg8tmCN/zANjAFaCAADsVxGBlYWZiBDOYgGIgBhMjCyuYwczIxMo"
                          + "CABEQPa4=";



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
                        ',','b',null,'d','?'
                }),
                new NullableFloatColumn(new Float[]{
                        10.1f,null,0.0f,null,50.5f
                }),
                new NullableDoubleColumn(new Double[]{
                        null,0.0,0.0,null,51.5
                }),
                new NullableBooleanColumn(new Boolean[]{
                        true,null,false,null,true
                }),
                new NullableBinaryColumn(new byte[][]{
                    new byte[]{0},
                    new byte[]{5,4,3,2,1},
                    null,
                    new byte[]{2,1,4,5,74,5,3},
                    null
                }));

        truthNullableImplv2 = BitVector.fromHexString(
                              "7b763a323b6e000000050000000a62797465436f6c0073686"
                            + "f7274436f6c00696e74436f6c006c6f6e67436f6c00737472"
                            + "696e67436f6c0063686172436f6c00666c6f6174436f6c006"
                            + "46f75626c65436f6c00626f6f6c65616e436f6c0062696e61"
                            + "7279436f6c000a0b0c0d0e110f10121400000003d6eacd7d0"
                            + "a00000032000b00150000000000000000000c000000000000"
                            + "0020000000000000000000000000000000000000000000000"
                            + "0000000000000000021000000000000000000000000000000"
                            + "354142434400322122302c2e00000023357b3d303e7d002c6"
                            + "200643f4121999a000000000000000000000000424a000000"
                            + "0000000000000000000000000000000000000000000000000"
                            + "00000000000004049c0000000000088000000010000000005"
                            + "05040302010000000000000007020104054a050300000000")
                              .asArray();


        truthNullableImplv2Compressed = BitVector.fromHexString(
                                        "6466ab2eb332b2ce636060600562aea4ca9254e7fc1"
                                      + "c86e28cfca2121023330f4ce5e4e7a583c54b8a3221"
                                      + "ace48cc422109d96939f085692925f9a9403d69c949"
                                      + "f9f939a98076666e625165582585cdc3cbc7c82fc02"
                                      + "4222407b98afbd3a5bcb0564183170338832c0000f9"
                                      + "45660c00f14d1f8a68e4ece2e0c468a4a063a7a40ae"
                                      + "b269b5ad815d2d834e12438abda3e2cc59c86a9dbc0"
                                      + "8180e040e9e07c0740710338218acac2ccc4c8c5059"
                                      + "76264616562f5666101b00df48306a")
                                        .asArray();


        truthNullableBase64Implv2 = "ZGarLrMyss5jYGBgBWKupMqSVOf8HIbijPyiEhAjMw9M5e"
                                  + "TnpYPFS4oyIazkjMQiEJ2Wk58IVpKSX5qUA9aclJ+fk5qY"
                                  + "B2Zm5iUWVYJYXNw8vHyC/AJCIkB7mK+9OlvLBWQYMXAziD"
                                  + "LAAA+UVmDADxTR+KaOTs4uDEaKSgY6ekCusmm1rYFdLYNO"
                                  + "EkOKvaPizFnIap28CBgOBA6eB8B0BxAzghisrCzMTIxQWX"
                                  + "YmRhZWL1ZmEBsA30gwag==";

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
        assertArrayEquals(
                "Serialized Dataframe does not match expected bytes",
                truthImplv2, bytes);
    }

    @Test
    public void testSerializationDefaultCompress() throws Exception{
        byte[] bytes = DataFrameSerializer.serialize(dfDefault, MODE_COMPRESSED);
        assertArrayEquals(
                "Serialized Dataframe does not match expected bytes",
                truthImplv2Compressed, bytes);
    }

    @Test
    public void testSerializationNullable() throws Exception{
        byte[] bytes = DataFrameSerializer.serialize(dfNullable, MODE_UNCOMPRESSED);
        assertArrayEquals(
                "Serialized Dataframe does not match expected bytes",
                truthNullableImplv2, bytes);
    }

    @Test
    public void testSerializationNullableCompressed() throws Exception{
        byte[] bytes = DataFrameSerializer.serialize(dfNullable, MODE_COMPRESSED);
        assertArrayEquals(
                "Serialized Dataframe does not match expected bytes",
                truthNullableImplv2Compressed, bytes);
    }

    @Test
    public void testDeserializationDefault() throws Exception{
        DataFrame res = DataFrameSerializer.deserialize(truthImplv2);
        assertFalse("DataFrame should not be empty", res.isEmpty());
        assertTrue("DataFrame row count should be 5", res.rows() == 5);
        assertTrue("DataFrame column count should be 10", res.columns() == 10);
        assertTrue("DataFrame should have column names set", res.hasColumnNames());
        assertTrue("DataFrame should be of type DefaultDataFrame",
                res instanceof DefaultDataFrame);

        assertTrue("DataFrame differs in content", res.equals(dfDefault));
    }

    @Test
    public void testDeserializationDefaultCompressed() throws Exception{
        DataFrame res = DataFrameSerializer.deserialize(truthImplv2Compressed);
        assertFalse("DataFrame should not be empty", res.isEmpty());
        assertTrue("DataFrame row count should be 5", res.rows() == 5);
        assertTrue("DataFrame column count should be 10", res.columns() == 10);
        assertTrue("DataFrame should have column names set", res.hasColumnNames());
        assertTrue("DataFrame should be of type DefaultDataFrame",
                res instanceof DefaultDataFrame);
        assertTrue("DataFrame differs in content", res.equals(dfDefault));
    }

    @Test
    public void testDeserializationNullable() throws Exception{
        DataFrame res = DataFrameSerializer.deserialize(truthNullableImplv2);
        assertFalse("DataFrame should not be empty", res.isEmpty());
        assertTrue("DataFrame row count should be 5", res.rows() == 5);
        assertTrue("DataFrame column count should be 10", res.columns() == 10);
        assertTrue("DataFrame should have column names set", res.hasColumnNames());
        assertTrue("DataFrame should be of type NullableDataFrame",
                res instanceof NullableDataFrame);

        assertTrue("DataFrame differs in content", res.equals(dfNullable));
    }

    @Test
    public void testDeserializationNullableCompressed() throws Exception{
        DataFrame res = DataFrameSerializer.deserialize(truthNullableImplv2Compressed);
        assertFalse("DataFrame should not be empty", res.isEmpty());
        assertTrue("DataFrame row count should be 5", res.rows() == 5);
        assertTrue("DataFrame column count should be 10", res.columns() == 10);
        assertTrue("DataFrame should have column names set", res.hasColumnNames());
        assertTrue("DataFrame should be of type NullableDataFrame",
                res instanceof NullableDataFrame);

        assertTrue("DataFrame differs in content", res.equals(dfNullable));
    }
    
    @Test
    public void testToBase64Default() throws Exception{
        String s = DataFrameSerializer.toBase64(dfDefault);
        DataFrame df = DataFrameSerializer.fromBase64(s);
        assertEquals("Dataframe does not match original", df, dfDefault);
    }
    
    @Test
    public void testToBase64Nullable() throws Exception{
        String s = DataFrameSerializer.toBase64(dfNullable);
        DataFrame df = DataFrameSerializer.fromBase64(s);
        assertEquals("Dataframe does not match original", df, dfNullable);
    }

    @Test
    public void testToBase64StringDefault() throws Exception{
        String s = DataFrameSerializer.toBase64(dfDefault);
        assertEquals(
                "Serialized Dataframe does not match expected Base64 string",
                truthBase64Implv2, s);
    }

    @Test
    public void testToBase64StringNullable() throws Exception{
        String s = DataFrameSerializer.toBase64(dfNullable);
        assertEquals(
                "Serialized Dataframe does not match expected Base64 string",
                truthNullableBase64Implv2, s);
    }

    @Test
    public void testFromBase64StringDefault() throws Exception{
        DataFrame res = DataFrameSerializer.fromBase64(truthBase64Implv2);
        assertFalse("DataFrame should not be empty", res.isEmpty());
        assertTrue("DataFrame row count should be 5", res.rows() == 5);
        assertTrue("DataFrame column count should be 10", res.columns() == 10);
        assertTrue("DataFrame should have column names set", res.hasColumnNames());
        assertTrue("DataFrame should be of type DefaultDataFrame",
                res instanceof DefaultDataFrame);

        assertTrue("DataFrame differs in content", res.equals(dfDefault));
    }

    @Test
    public void testFromBase64StringNullable() throws Exception{
        DataFrame res = DataFrameSerializer.fromBase64(truthNullableBase64Implv2);
        assertFalse("DataFrame should not be empty", res.isEmpty());
        assertTrue("DataFrame row count should be 5", res.rows() == 5);
        assertTrue("DataFrame column count should be 10", res.columns() == 10);
        assertTrue("DataFrame should have column names set", res.hasColumnNames());
        assertTrue("DataFrame should be of type NullableDataFrame",
                res instanceof NullableDataFrame);

        assertTrue("DataFrame differs in content", res.equals(dfNullable));
    }

    @Test
    public void testSerialDeserialDefault() throws Exception{
        byte[] bytes = DataFrameSerializer.serialize(dfDefault, MODE_UNCOMPRESSED);
        DataFrame res = DataFrameSerializer.deserialize(bytes);
        assertTrue("DataFrames are not equal", res.equals(dfDefault));
    }

    @Test
    public void testSerialDeserialDefaultCompressed() throws Exception{
        byte[] bytes = DataFrameSerializer.serialize(dfDefault, MODE_COMPRESSED);
        DataFrame res = DataFrameSerializer.deserialize(bytes);
        assertTrue("DataFrames are not equal", res.equals(dfDefault));
    }

    @Test
    public void testSerialDeserialNullable() throws Exception{
        byte[] bytes = DataFrameSerializer.serialize(dfNullable, MODE_UNCOMPRESSED);
        DataFrame res = DataFrameSerializer.deserialize(bytes);
        assertTrue("DataFrames are not equal", res.equals(dfNullable));
    }

    @Test
    public void testSerialDeserialNullableCompressed() throws Exception{
        byte[] bytes = DataFrameSerializer.serialize(dfNullable, MODE_COMPRESSED);
        DataFrame res = DataFrameSerializer.deserialize(bytes);
        assertTrue("DataFrames are not equal", res.equals(dfNullable));
    }
    
    @Test
    public void stressTestDefault() throws Exception{
        DataFrame df = DataFrame.copy(dfDefault);
        for(int i=0; i<df.columns(); ++i){
            Column col = df.getColumn(0);
            df.removeColumn(0);
            df.addColumn(col);
            byte[] bytes = DataFrameSerializer.serialize(df);
            df = DataFrameSerializer.deserialize(bytes);
        }
        assertTrue("DataFrame does not match original", df.equals(dfDefault));

        DataFrame df2 = DataFrame.copy(dfDefault);
        for(int i=0; i<df2.rows(); ++i){
            df2.removeRow(0);
            df.removeRow(0);
            byte[] bytes = DataFrameSerializer.serialize(df);
            df = DataFrameSerializer.deserialize(bytes);
            assertTrue("DataFrame does not match changed object", df.equals(df2));
        }
    }
    
    @Test
    public void stressTestNullable() throws Exception{
        DataFrame df = DataFrame.copy(dfNullable);
        for(int i=0; i<df.columns(); ++i){
            Column col = df.getColumn(0);
            df.removeColumn(0);
            df.addColumn(col);
            byte[] bytes = DataFrameSerializer.serialize(df);
            df = DataFrameSerializer.deserialize(bytes);
        }
        assertTrue("DataFrame does not match original", df.equals(dfNullable));

        DataFrame df2 = DataFrame.copy(dfNullable);
        for(int i=0; i<df2.rows(); ++i){
            df2.removeRow(0);
            df.removeRow(0);
            byte[] bytes = DataFrameSerializer.serialize(df);
            df = DataFrameSerializer.deserialize(bytes);
            assertTrue("DataFrame does not match changed object", df.equals(df2));
        }
    }

}
