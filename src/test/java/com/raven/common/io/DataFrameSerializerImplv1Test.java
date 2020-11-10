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

package com.raven.common.io;

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
 * This class tests the legacy text-based version 1 format.<br>
 * The legacy format version 1 is not supported anymore.
 * Deserialization should fail exceptionally. 
 * 
 */
public class DataFrameSerializerImplv1Test {

    static String[] columnNames;
    static DataFrame df;
    static byte[] truthImplv1;

    static String[] columnNamesEscaped;
    static DataFrame dfEscaped;
    static byte[] truthEscapedImplv1;

    static String[] columnNamesEscapedNullable;
    static DataFrame dfEscapedNullable;
    static byte[] truthEscapedNullableImplv1;
    static String truthBase64Implv1;

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

        //expected
        truthImplv1 = new byte[]{123,118,58,49,59,105,58,100,101,102,97,117,108,116,
                59,114,58,53,59,99,58,57,59,110,58,98,121,116,101,67,111,108,44,115,
                104,111,114,116,67,111,108,44,105,110,116,67,111,108,44,108,111,110,
                103,67,111,108,44,115,116,114,105,110,103,67,111,108,44,99,104,97,114,
                67,111,108,44,102,108,111,97,116,67,111,108,44,100,111,117,98,108,101,
                67,111,108,44,98,111,111,108,101,97,110,67,111,108,44,59,116,58,66,121,
                116,101,67,111,108,117,109,110,44,83,104,111,114,116,67,111,108,117,109,
                110,44,73,110,116,67,111,108,117,109,110,44,76,111,110,103,67,111,108,117,
                109,110,44,83,116,114,105,110,103,67,111,108,117,109,110,44,67,104,97,114,
                67,111,108,117,109,110,44,70,108,111,97,116,67,111,108,117,109,110,44,68,
                111,117,98,108,101,67,111,108,117,109,110,44,66,111,111,108,101,97,110,67,
                111,108,117,109,110,44,59,125,49,48,44,50,48,44,51,48,44,52,48,44,53,48,44,
                49,49,44,50,49,44,51,49,44,52,49,44,53,49,44,49,50,44,50,50,44,51,50,44,52,
                50,44,53,50,44,49,51,44,50,51,44,51,51,44,52,51,44,53,51,44,49,48,44,50,48,
                44,51,48,44,52,48,44,53,48,44,97,44,98,44,99,44,100,44,101,44,49,48,46,49,44,
                50,48,46,50,44,51,48,46,51,44,52,48,46,52,44,53,48,46,53,44,49,49,46,49,44,
                50,49,46,50,44,51,49,46,51,44,52,49,46,52,44,53,49,46,53,44,116,114,117,101,
                44,102,97,108,115,101,44,116,114,117,101,44,102,97,108,115,101,44,116,
                114,117,101,44};



        //*************************************************//
        //                                                 //
        //        Test with escaped characters             //
        //                                                 //
        //*************************************************//

        columnNamesEscaped = new String[]{
                "byte,Col",
                "sh,or,tCol",
                "intC,ol",
                "lon,gCol",
                "str,i,ngCol",
                "cha,r,Col",
                "floa<>t,<Col",
                "dou>,bl>eCol",
                "bo?o_le.anCol<>>"
        };

        dfEscaped = new DefaultDataFrame(
                columnNamesEscaped, 
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
                        "1,,0<","2!\"0,.","3<>0","<40>","#5{=0>}"
                }),
                new CharColumn(new char[]{
                        ',','b',',','d','e'
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

        truthEscapedImplv1 = new byte[]{123,118,58,49,59,105,58,100,101,102,97,117,108,
                116,59,114,58,53,59,99,58,57,59,110,58,98,121,116,101,60,44,62,
                67,111,108,44,115,104,60,44,62,111,114,60,44,62,116,67,111,108,
                44,105,110,116,67,60,44,62,111,108,44,108,111,110,60,44,62,103,
                67,111,108,44,115,116,114,60,44,62,105,60,44,62,110,103,67,111,
                108,44,99,104,97,60,44,62,114,60,44,62,67,111,108,44,102,108,
                111,97,60,60,62,62,116,60,44,62,60,60,62,67,111,108,44,100,111,
                117,62,60,44,62,98,108,62,101,67,111,108,44,98,111,63,111,95,
                108,101,46,97,110,67,111,108,60,60,62,62,62,44,59,116,58,66,121,
                116,101,67,111,108,117,109,110,44,83,104,111,114,116,67,111,
                108,117,109,110,44,73,110,116,67,111,108,117,109,110,44,76,111,
                110,103,67,111,108,117,109,110,44,83,116,114,105,110,103,67,111,
                108,117,109,110,44,67,104,97,114,67,111,108,117,109,110,44,
                70,108,111,97,116,67,111,108,117,109,110,44,68,111,117,98,108,
                101,67,111,108,117,109,110,44,66,111,111,108,101,97,110,67,111,
                108,117,109,110,44,59,125,49,48,44,50,48,44,51,48,44,52,48,44,
                53,48,44,49,49,44,50,49,44,51,49,44,52,49,44,53,49,44,49,50,44,
                50,50,44,51,50,44,52,50,44,53,50,44,49,51,44,50,51,44,51,51,44,
                52,51,44,53,51,44,49,60,44,62,60,44,62,48,60,60,62,44,50,33,
                34,48,60,44,62,46,44,51,60,60,62,62,48,44,60,60,62,52,48,62,44,
                35,53,123,61,48,62,125,44,60,44,62,44,98,44,60,44,62,44,100,
                44,101,44,49,48,46,49,44,50,48,46,50,44,51,48,46,51,44,52,48,
                46,52,44,53,48,46,53,44,49,49,46,49,44,50,49,46,50,44,51,49,
                46,51,44,52,49,46,52,44,53,49,46,53,44,116,114,117,101,44,102,
                97,108,115,101,44,116,114,117,101,44,102,97,108,115,101,44,
                116,114,117,101,44};



        //**************************************************************//
        //                                                              //
        //        Test NullableDataFRame with escaped characters        //
        //                                                              //
        //**************************************************************//

        columnNamesEscapedNullable = new String[]{
                "byte,Col",
                "sh,or,tCol",
                "intC,ol",
                "lon,gCol",
                "str,i,ngCol",
                "cha,r,Col",
                "floa<>t,<Col",
                "dou>,bl>eCol",
                "bo?o_le.anCol<>>"
        };

        dfEscapedNullable = new NullableDataFrame(
                columnNamesEscapedNullable, 
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
                        "1,,0<","2!\"0,.","3<>0"
                }),
                new NullableCharColumn(new Character[]{
                        ',',null,','
                }),
                new NullableFloatColumn(new Float[]{
                        1.0f,null,3.0f
                }),
                new NullableDoubleColumn(new Double[]{
                        1.0,null,3.0
                }),
                new NullableBooleanColumn(new Boolean[]{
                        true,false,null
                }));

        truthEscapedNullableImplv1 = new byte[]{123,118,58,49,59,105,58,110,117,108,
                108,97,98,108,101,59,114,58,51,59,99,58,57,59,110,58,98,121,
                116,101,60,44,62,67,111,108,44,115,104,60,44,62,111,114,60,44,
                62,116,67,111,108,44,105,110,116,67,60,44,62,111,108,44,108,
                111,110,60,44,62,103,67,111,108,44,115,116,114,60,44,62,105,
                60,44,62,110,103,67,111,108,44,99,104,97,60,44,62,114,60,44,
                62,67,111,108,44,102,108,111,97,60,60,62,62,116,60,44,62,60,
                60,62,67,111,108,44,100,111,117,62,60,44,62,98,108,62,101,67,
                111,108,44,98,111,63,111,95,108,101,46,97,110,67,111,108,60,
                60,62,62,62,44,59,116,58,78,117,108,108,97,98,108,101,66,121,
                116,101,67,111,108,117,109,110,44,78,117,108,108,97,98,108,
                101,83,104,111,114,116,67,111,108,117,109,110,44,78,117,108,
                108,97,98,108,101,73,110,116,67,111,108,117,109,110,44,78,
                117,108,108,97,98,108,101,76,111,110,103,67,111,108,117,109,
                110,44,78,117,108,108,97,98,108,101,83,116,114,105,110,103,
                67,111,108,117,109,110,44,78,117,108,108,97,98,108,101,67,
                104,97,114,67,111,108,117,109,110,44,78,117,108,108,97,98,
                108,101,70,108,111,97,116,67,111,108,117,109,110,44,78,117,
                108,108,97,98,108,101,68,111,117,98,108,101,67,111,108,117,
                109,110,44,78,117,108,108,97,98,108,101,66,111,111,108,101,
                97,110,67,111,108,117,109,110,44,59,125,49,44,110,117,108,
                108,44,51,44,49,44,110,117,108,108,44,51,44,49,44,110,117,
                108,108,44,51,44,49,44,110,117,108,108,44,51,44,49,60,44,
                62,60,44,62,48,60,60,62,44,50,33,34,48,60,44,62,46,44,51,
                60,60,62,62,48,44,60,44,62,44,110,117,108,108,44,60,44,
                62,44,49,46,48,44,110,117,108,108,44,51,46,48,44,49,46,
                48,44,110,117,108,108,44,51,46,48,44,116,114,117,101,44,
                102,97,108,115,101,44,110,117,108,108,44};

        truthBase64Implv1 = "ZGZ9kMFqwzAMhp9lPYuQLKfawYOmFApllz1AcTp3CagWuHJh"
                + "jL37pDQ9JIUdbP//Z9n80s/NVHYwMSP6DoNNprYns7bRdN8cGnAtI"
                + "Vx7EZRkY7VD5FYBAlIU8TXWsN4PsuLoT70XnaYfzki+aZxj8XIq+qTs"
                + "xHXogtqO3uiIofBRnJY6sGzep1wbCSM8XyI80EdPiRdsH5fkQGOc2UNO"
                + "wxNse58WaCeZl99tKWvxHG6IMIy5ldrfCnScUMN/QgcBrpRO4fVlVYouo"
                + "Na+SxB9L1NRFeX0RsTMcMoBzh6v4c7+AN+Cmao=";

    }

    @AfterClass
    public static void tearDownAfterClass(){ 
        columnNames = null;
        df = null;
        truthImplv1 = null;
        columnNamesEscaped = null;
        dfEscaped = null;
        truthEscapedImplv1 = null;
    }

    @Before
    public void setUp(){ }

    @After
    public void tearDown(){ }

    @Test(expected = SerializationException.class)
    public void testDeserialization(){
        DataFrameSerializer.deserialize(truthImplv1);
    }

    @Test(expected = SerializationException.class)
    public void testDeserializationEscaped(){
        DataFrameSerializer.deserialize(truthEscapedImplv1);
    }

    @Test(expected = SerializationException.class)
    public void testDeserializationNullableEscaped(){
        DataFrameSerializer.deserialize(truthEscapedNullableImplv1);
    }

    @Test(expected = SerializationException.class)
    public void testFromBase64String(){
        DataFrameSerializer.fromBase64(truthBase64Implv1);
    }
}
