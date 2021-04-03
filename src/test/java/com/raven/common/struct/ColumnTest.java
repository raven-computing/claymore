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

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for Column construction, conversions and static functions.
 *
 */
public class ColumnTest {

    Column[] allColumnClasses;

    @BeforeClass
    public static void setUpBeforeClass(){ }

    @AfterClass
    public static void tearDownAfterClass(){ }

    @Before
    public void setUp(){
        allColumnClasses = new Column[]{
                new ByteColumn(),
                new ShortColumn(),
                new IntColumn(),
                new LongColumn(),
                new FloatColumn(),
                new DoubleColumn(),
                new StringColumn(),
                new CharColumn(),
                new BooleanColumn(),
                new BinaryColumn(),
                new NullableByteColumn(),
                new NullableShortColumn(),
                new NullableIntColumn(),
                new NullableLongColumn(),
                new NullableFloatColumn(),
                new NullableDoubleColumn(),
                new NullableStringColumn(),
                new NullableCharColumn(),
                new NullableBooleanColumn(),
                new NullableBinaryColumn()};
    }

    @After
    public void tearDown(){ }



    //***************************************//
    //              Constructors             //
    //***************************************//



    @Test
    public void testConstructByteColumn(){
        Column col = new ByteColumn(new byte[]{11, 22, 33, 44, 55});
        assertTrue(col.typeCode() == ByteColumn.TYPE_CODE);
        assertTrue(col.typeName().equals("byte"));
        assertFalse(col.isNullable());
        assertTrue(col.isNumeric());
        assertNull(col.getName());
        assertTrue(col.capacity() == 5);
    }

    @Test
    public void testConstructNamedByteColumn(){
        Column col = new ByteColumn("colname", new byte[]{11, 22, 33, 44, 55});
        assertTrue(col.typeCode() == ByteColumn.TYPE_CODE);
        assertTrue(col.typeName().equals("byte"));
        assertFalse(col.isNullable());
        assertTrue(col.isNumeric());
        assertTrue(col.getName().equals("colname"));
        assertTrue(col.capacity() == 5);
    }

    @Test
    public void testStaticConstructNamedByteColumn(){
        ByteColumn col = Column.create("colname", (byte)11, (byte)22, (byte)33, (byte)44, (byte)55);
        assertTrue(col.typeCode() == ByteColumn.TYPE_CODE);
        assertTrue(col.typeName().equals("byte"));
        assertFalse(col.isNullable());
        assertTrue(col.isNumeric());
        assertTrue(col.getName().equals("colname"));
        assertTrue(col.capacity() == 5);
    }

    @Test
    public void testConstructShortColumn(){
        Column col = new ShortColumn(new short[]{11, 22, 33, 44, 55});
        assertTrue(col.typeCode() == ShortColumn.TYPE_CODE);
        assertTrue(col.typeName().equals("short"));
        assertFalse(col.isNullable());
        assertTrue(col.isNumeric());
        assertNull(col.getName());
        assertTrue(col.capacity() == 5);
    }

    @Test
    public void testConstructNamedShortColumn(){
        Column col = new ShortColumn("colname", new short[]{11, 22, 33, 44, 55});
        assertTrue(col.typeCode() == ShortColumn.TYPE_CODE);
        assertTrue(col.typeName().equals("short"));
        assertFalse(col.isNullable());
        assertTrue(col.isNumeric());
        assertTrue(col.getName().equals("colname"));
        assertTrue(col.capacity() == 5);
    }

    @Test
    public void testStaticConstructNamedShortColumn(){
        ShortColumn col = Column.create("colname",
                (short)11, (short)22, (short)33, (short)44, (short)55);

        assertTrue(col.typeCode() == ShortColumn.TYPE_CODE);
        assertTrue(col.typeName().equals("short"));
        assertFalse(col.isNullable());
        assertTrue(col.isNumeric());
        assertTrue(col.getName().equals("colname"));
        assertTrue(col.capacity() == 5);
    }

    @Test
    public void testConstructIntColumn(){
        Column col = new IntColumn(new int[]{11, 22, 33, 44, 55});
        assertTrue(col.typeCode() == IntColumn.TYPE_CODE);
        assertTrue(col.typeName().equals("int"));
        assertFalse(col.isNullable());
        assertTrue(col.isNumeric());
        assertNull(col.getName());
        assertTrue(col.capacity() == 5);
    }

    @Test
    public void testConstructNamedIntColumn(){
        Column col = new IntColumn("colname", new int[]{11, 22, 33, 44, 55});
        assertTrue(col.typeCode() == IntColumn.TYPE_CODE);
        assertTrue(col.typeName().equals("int"));
        assertFalse(col.isNullable());
        assertTrue(col.isNumeric());
        assertTrue(col.getName().equals("colname"));
        assertTrue(col.capacity() == 5);
    }

    @Test
    public void testStaticConstructNamedIntColumn(){
        IntColumn col = Column.create("colname", 11, 22, 33, 44, 55);
        assertTrue(col.typeCode() == IntColumn.TYPE_CODE);
        assertTrue(col.typeName().equals("int"));
        assertFalse(col.isNullable());
        assertTrue(col.isNumeric());
        assertTrue(col.getName().equals("colname"));
        assertTrue(col.capacity() == 5);
    }

    @Test
    public void testConstructLongColumn(){
        Column col = new LongColumn(new long[]{11L, 22L, 33L, 44L, 55L});
        assertTrue(col.typeCode() == LongColumn.TYPE_CODE);
        assertTrue(col.typeName().equals("long"));
        assertFalse(col.isNullable());
        assertTrue(col.isNumeric());
        assertNull(col.getName());
        assertTrue(col.capacity() == 5);
    }

    @Test
    public void testConstructNamedLongColumn(){
        Column col = new LongColumn("colname", new long[]{11L, 22L, 33L, 44L, 55L});
        assertTrue(col.typeCode() == LongColumn.TYPE_CODE);
        assertTrue(col.typeName().equals("long"));
        assertFalse(col.isNullable());
        assertTrue(col.isNumeric());
        assertTrue(col.getName().equals("colname"));
        assertTrue(col.capacity() == 5);
    }

    @Test
    public void testStaticConstructNamedLongColumn(){
        LongColumn col = Column.create("colname", 11L, 22L, 33L, 44L, 55L);
        assertTrue(col.typeCode() == LongColumn.TYPE_CODE);
        assertTrue(col.typeName().equals("long"));
        assertFalse(col.isNullable());
        assertTrue(col.isNumeric());
        assertTrue(col.getName().equals("colname"));
        assertTrue(col.capacity() == 5);
    }

    @Test
    public void testConstructFloatColumn(){
        Column col = new FloatColumn(new float[]{11.1f, 22.2f, 33.3f, 44.4f, 55.5f});
        assertTrue(col.typeCode() == FloatColumn.TYPE_CODE);
        assertTrue(col.typeName().equals("float"));
        assertFalse(col.isNullable());
        assertTrue(col.isNumeric());
        assertNull(col.getName());
        assertTrue(col.capacity() == 5);
    }

    @Test
    public void testConstructNamedFloatColumn(){
        Column col = new FloatColumn("colname", new float[]{11.1f, 22.2f, 33.3f, 44.4f, 55.5f});
        assertTrue(col.typeCode() == FloatColumn.TYPE_CODE);
        assertTrue(col.typeName().equals("float"));
        assertFalse(col.isNullable());
        assertTrue(col.isNumeric());
        assertTrue(col.getName().equals("colname"));
        assertTrue(col.capacity() == 5);
    }

    @Test
    public void testStaticConstructNamedFloatColumn(){
        FloatColumn col = Column.create("colname", 11.1f, 22.2f, 33.3f, 44.4f, 55.5f);
        assertTrue(col.typeCode() == FloatColumn.TYPE_CODE);
        assertTrue(col.typeName().equals("float"));
        assertFalse(col.isNullable());
        assertTrue(col.isNumeric());
        assertTrue(col.getName().equals("colname"));
        assertTrue(col.capacity() == 5);
    }

    @Test
    public void testConstructDoubleColumn(){
        Column col = new DoubleColumn(new double[]{11.1, 22.2, 33.3, 44.4, 55.5});
        assertTrue(col.typeCode() == DoubleColumn.TYPE_CODE);
        assertTrue(col.typeName().equals("double"));
        assertFalse(col.isNullable());
        assertTrue(col.isNumeric());
        assertNull(col.getName());
        assertTrue(col.capacity() == 5);
    }

    @Test
    public void testConstructNamedDoubleColumn(){
        Column col = new DoubleColumn("colname", new double[]{11.1, 22.2, 33.3, 44.4, 55.5});
        assertTrue(col.typeCode() == DoubleColumn.TYPE_CODE);
        assertTrue(col.typeName().equals("double"));
        assertFalse(col.isNullable());
        assertTrue(col.isNumeric());
        assertTrue(col.getName().equals("colname"));
        assertTrue(col.capacity() == 5);
    }

    @Test
    public void testStaticConstructNamedDoubleColumn(){
        DoubleColumn col = Column.create("colname", 11.1, 22.2, 33.3, 44.4, 55.5);
        assertTrue(col.typeCode() == DoubleColumn.TYPE_CODE);
        assertTrue(col.typeName().equals("double"));
        assertFalse(col.isNullable());
        assertTrue(col.isNumeric());
        assertTrue(col.getName().equals("colname"));
        assertTrue(col.capacity() == 5);
    }

    @Test
    public void testConstructStringColumn(){
        Column col = new StringColumn(new String[]{"AAA", "AAB", "AAC", "AAD", "AAE"});
        assertTrue(col.typeCode() == StringColumn.TYPE_CODE);
        assertTrue(col.typeName().equals("string"));
        assertFalse(col.isNullable());
        assertFalse(col.isNumeric());
        assertNull(col.getName());
        assertTrue(col.capacity() == 5);
    }

    @Test
    public void testConstructNamedStringColumn(){
        Column col = new StringColumn("colname",
                new String[]{"AAA", "AAB", "AAC", "AAD", "AAE"});

        assertTrue(col.typeCode() == StringColumn.TYPE_CODE);
        assertTrue(col.typeName().equals("string"));
        assertFalse(col.isNullable());
        assertFalse(col.isNumeric());
        assertTrue(col.getName().equals("colname"));
        assertTrue(col.capacity() == 5);
    }

    @Test
    public void testStaticConstructNamedStringColumn(){
        StringColumn col = Column.create("colname", "AAA", "AAB", "AAC", "AAD", "AAE");
        assertTrue(col.typeCode() == StringColumn.TYPE_CODE);
        assertTrue(col.typeName().equals("string"));
        assertFalse(col.isNullable());
        assertFalse(col.isNumeric());
        assertTrue(col.getName().equals("colname"));
        assertTrue(col.capacity() == 5);
    }

    @Test
    public void testConstructCharColumn(){
        Column col = new CharColumn(new char[]{'A', 'B', 'C', 'D', 'E'});
        assertTrue(col.typeCode() == CharColumn.TYPE_CODE);
        assertTrue(col.typeName().equals("char"));
        assertFalse(col.isNullable());
        assertFalse(col.isNumeric());
        assertNull(col.getName());
        assertTrue(col.capacity() == 5);
    }

    @Test
    public void testConstructNamedCharColumn(){
        Column col = new CharColumn("colname", new char[]{'A', 'B', 'C', 'D', 'E'});
        assertTrue(col.typeCode() == CharColumn.TYPE_CODE);
        assertTrue(col.typeName().equals("char"));
        assertFalse(col.isNullable());
        assertFalse(col.isNumeric());
        assertTrue(col.getName().equals("colname"));
        assertTrue(col.capacity() == 5);
    }

    @Test
    public void testStaticConstructNamedCharColumn(){
        CharColumn col = Column.create("colname", 'A', 'B', 'C', 'D', 'E');
        assertTrue(col.typeCode() == CharColumn.TYPE_CODE);
        assertTrue(col.typeName().equals("char"));
        assertFalse(col.isNullable());
        assertFalse(col.isNumeric());
        assertTrue(col.getName().equals("colname"));
        assertTrue(col.capacity() == 5);
    }

    @Test
    public void testConstructBooleanColumn(){
        Column col = new BooleanColumn(new boolean[]{true, false, true, false, true});
        assertTrue(col.typeCode() == BooleanColumn.TYPE_CODE);
        assertTrue(col.typeName().equals("boolean"));
        assertFalse(col.isNullable());
        assertFalse(col.isNumeric());
        assertNull(col.getName());
        assertTrue(col.capacity() == 5);
    }

    @Test
    public void testConstructNamedBooleanColumn(){
        Column col = new BooleanColumn("colname",
                new boolean[]{true, false, true, false, true});

        assertTrue(col.typeCode() == BooleanColumn.TYPE_CODE);
        assertTrue(col.typeName().equals("boolean"));
        assertFalse(col.isNullable());
        assertFalse(col.isNumeric());
        assertTrue(col.getName().equals("colname"));
        assertTrue(col.capacity() == 5);
    }

    @Test
    public void testStaticConstructNamedBooleanColumn(){
        BooleanColumn col = Column.create("colname", true, false, true, false, true);
        assertTrue(col.typeCode() == BooleanColumn.TYPE_CODE);
        assertTrue(col.typeName().equals("boolean"));
        assertFalse(col.isNullable());
        assertFalse(col.isNumeric());
        assertTrue(col.getName().equals("colname"));
        assertTrue(col.capacity() == 5);
    }

    @Test
    public void testConstructBinaryColumn(){
        Column col = new BinaryColumn(new byte[][]{
            new byte[]{0x01, 0x01},
            new byte[]{0x02, 0x02},
            new byte[]{0x03, 0x03},
            new byte[]{0x04, 0x04},
            new byte[]{0x05, 0x05}
        });
        assertTrue(col.typeCode() == BinaryColumn.TYPE_CODE);
        assertTrue(col.typeName().equals("binary"));
        assertFalse(col.isNullable());
        assertFalse(col.isNumeric());
        assertNull(col.getName());
        assertTrue(col.capacity() == 5);
    }

    @Test
    public void testConstructNamedBinaryColumn(){
        Column col = new BinaryColumn("colname", new byte[][]{
                new byte[]{0x01, 0x01},
                new byte[]{0x02, 0x02},
                new byte[]{0x03, 0x03},
                new byte[]{0x04, 0x04},
                new byte[]{0x05, 0x05}
        });
        assertTrue(col.typeCode() == BinaryColumn.TYPE_CODE);
        assertTrue(col.typeName().equals("binary"));
        assertFalse(col.isNullable());
        assertFalse(col.isNumeric());
        assertTrue(col.getName().equals("colname"));
        assertTrue(col.capacity() == 5);
    }

    @Test
    public void testStaticConstructNamedBinaryColumn(){
        BinaryColumn col = Column.create("colname",
                new byte[]{0x01, 0x01},
                new byte[]{0x02, 0x02},
                new byte[]{0x03, 0x03},
                new byte[]{0x04, 0x04},
                new byte[]{0x05, 0x05});

        assertTrue(col.typeCode() == BinaryColumn.TYPE_CODE);
        assertTrue(col.typeName().equals("binary"));
        assertFalse(col.isNullable());
        assertFalse(col.isNumeric());
        assertTrue(col.getName().equals("colname"));
        assertTrue(col.capacity() == 5);
    }

    @Test
    public void testConstructNullableByteColumn(){
        NullableColumn col = new NullableByteColumn(new Byte[]{11, null, 33, 44, null});
        assertTrue(col.typeCode() == NullableByteColumn.TYPE_CODE);
        assertTrue(col.typeName().equals("byte"));
        assertTrue(col.isNullable());
        assertTrue(col.isNumeric());
        assertNull(col.getName());
        assertTrue(col.capacity() == 5);
    }

    @Test
    public void testConstructNamedNullableByteColumn(){
        NullableColumn col = new NullableByteColumn("colname",
                new Byte[]{null, 22, 33, 44, null});

        assertTrue(col.typeCode() == NullableByteColumn.TYPE_CODE);
        assertTrue(col.typeName().equals("byte"));
        assertTrue(col.isNullable());
        assertTrue(col.isNumeric());
        assertTrue(col.getName().equals("colname"));
        assertTrue(col.capacity() == 5);
    }

    @Test
    public void testStaticConstructNamedNullableByteColumn(){
        NullableByteColumn col = Column.nullable("colname",
                (byte)11, null, (byte)33, null, (byte)55);

        assertTrue(col.typeCode() == NullableByteColumn.TYPE_CODE);
        assertTrue(col.typeName().equals("byte"));
        assertTrue(col.isNullable());
        assertTrue(col.isNumeric());
        assertTrue(col.getName().equals("colname"));
        assertTrue(col.capacity() == 5);
    }

    @Test
    public void testConstructNullableShortColumn(){
        NullableColumn col = new NullableShortColumn(new Short[]{11, null, null, 44, 55});
        assertTrue(col.typeCode() == NullableShortColumn.TYPE_CODE);
        assertTrue(col.typeName().equals("short"));
        assertTrue(col.isNullable());
        assertTrue(col.isNumeric());
        assertNull(col.getName());
        assertTrue(col.capacity() == 5);
    }

    @Test
    public void testConstructNamedNullableShortColumn(){
        NullableColumn col = new NullableShortColumn("colname",
                new Short[]{11, 22, 33, null, null});

        assertTrue(col.typeCode() == NullableShortColumn.TYPE_CODE);
        assertTrue(col.typeName().equals("short"));
        assertTrue(col.isNullable());
        assertTrue(col.isNumeric());
        assertTrue(col.getName().equals("colname"));
        assertTrue(col.capacity() == 5);
    }

    @Test
    public void testStaticConstructNamedNullableShortColumn(){
        NullableShortColumn col = Column.nullable("colname",
                null, null, (short)33, null, (short)55);

        assertTrue(col.typeCode() == NullableShortColumn.TYPE_CODE);
        assertTrue(col.typeName().equals("short"));
        assertTrue(col.isNullable());
        assertTrue(col.isNumeric());
        assertTrue(col.getName().equals("colname"));
        assertTrue(col.capacity() == 5);
    }

    @Test
    public void testConstructNullableIntColumn(){
        NullableColumn col = new NullableIntColumn(new Integer[]{11, null, null, 44, 55});
        assertTrue(col.typeCode() == NullableIntColumn.TYPE_CODE);
        assertTrue(col.typeName().equals("int"));
        assertTrue(col.isNullable());
        assertTrue(col.isNumeric());
        assertNull(col.getName());
        assertTrue(col.capacity() == 5);
    }

    @Test
    public void testConstructNamedNullableIntColumn(){
        NullableColumn col = new NullableIntColumn("colname",
                new Integer[]{null, 22, 33, 44, null});

        assertTrue(col.typeCode() == NullableIntColumn.TYPE_CODE);
        assertTrue(col.typeName().equals("int"));
        assertTrue(col.isNullable());
        assertTrue(col.isNumeric());
        assertTrue(col.getName().equals("colname"));
        assertTrue(col.capacity() == 5);
    }

    @Test
    public void testStaticConstructNamedNullableIntColumn(){
        NullableIntColumn col = NullableColumn.nullable("colname", 11, 22, null, 44, null);
        assertTrue(col.typeCode() == NullableIntColumn.TYPE_CODE);
        assertTrue(col.typeName().equals("int"));
        assertTrue(col.isNullable());
        assertTrue(col.isNumeric());
        assertTrue(col.getName().equals("colname"));
        assertTrue(col.capacity() == 5);
    }

    @Test
    public void testConstructNullableLongColumn(){
        NullableColumn col = new NullableLongColumn(
                new Long[]{null, null, 33L, 44L, 55L});

        assertTrue(col.typeCode() == NullableLongColumn.TYPE_CODE);
        assertTrue(col.typeName().equals("long"));
        assertTrue(col.isNullable());
        assertTrue(col.isNumeric());
        assertNull(col.getName());
        assertTrue(col.capacity() == 5);
    }

    @Test
    public void testConstructNamedNullableLongColumn(){
        NullableColumn col = new NullableLongColumn("colname",
                new Long[]{11L, 22L, null, null, null});

        assertTrue(col.typeCode() == NullableLongColumn.TYPE_CODE);
        assertTrue(col.typeName().equals("long"));
        assertTrue(col.isNullable());
        assertTrue(col.isNumeric());
        assertTrue(col.getName().equals("colname"));
        assertTrue(col.capacity() == 5);
    }

    @Test
    public void testStaticConstructNamedNullableLongColumn(){
        NullableLongColumn col = Column.nullable("colname",
                11L, null, null, 44L, 55L);

        assertTrue(col.typeCode() == NullableLongColumn.TYPE_CODE);
        assertTrue(col.typeName().equals("long"));
        assertTrue(col.isNullable());
        assertTrue(col.isNumeric());
        assertTrue(col.getName().equals("colname"));
        assertTrue(col.capacity() == 5);
    }

    @Test
    public void testConstructNullableFloatColumn(){
        NullableColumn col = new NullableFloatColumn(
                new Float[]{11.1f, 22.2f, null, 44.4f, null});

        assertTrue(col.typeCode() == NullableFloatColumn.TYPE_CODE);
        assertTrue(col.typeName().equals("float"));
        assertTrue(col.isNullable());
        assertTrue(col.isNumeric());
        assertNull(col.getName());
        assertTrue(col.capacity() == 5);
    }

    @Test
    public void testConstructNamedNullableFloatColumn(){
        NullableColumn col = new NullableFloatColumn("colname",
                new Float[]{11.1f, null, 33.3f, null, 55.5f});

        assertTrue(col.typeCode() == NullableFloatColumn.TYPE_CODE);
        assertTrue(col.typeName().equals("float"));
        assertTrue(col.isNullable());
        assertTrue(col.isNumeric());
        assertTrue(col.getName().equals("colname"));
        assertTrue(col.capacity() == 5);
    }

    @Test
    public void testStaticConstructNamedNullableFloatColumn(){
        NullableFloatColumn col = Column.nullable("colname",
                11.1f, 22.2f, 33.3f, 44.4f, 55.5f);

        assertTrue(col.typeCode() == NullableFloatColumn.TYPE_CODE);
        assertTrue(col.typeName().equals("float"));
        assertTrue(col.isNullable());
        assertTrue(col.isNumeric());
        assertTrue(col.getName().equals("colname"));
        assertTrue(col.capacity() == 5);
    }

    @Test
    public void testConstructNullableDoubleColumn(){
        NullableColumn col = new NullableDoubleColumn(
                new Double[]{null, null, null, null, null});

        assertTrue(col.typeCode() == NullableDoubleColumn.TYPE_CODE);
        assertTrue(col.typeName().equals("double"));
        assertTrue(col.isNullable());
        assertTrue(col.isNumeric());
        assertNull(col.getName());
        assertTrue(col.capacity() == 5);
    }

    @Test
    public void testConstructNamedNullableDoubleColumn(){
        NullableColumn col = new NullableDoubleColumn("colname",
                new Double[]{null, null, null, 44.4, 55.5});

        assertTrue(col.typeCode() == NullableDoubleColumn.TYPE_CODE);
        assertTrue(col.typeName().equals("double"));
        assertTrue(col.isNullable());
        assertTrue(col.isNumeric());
        assertTrue(col.getName().equals("colname"));
        assertTrue(col.capacity() == 5);
    }

    @Test
    public void testStaticConstructNamedNullableDoubleColumn(){
        NullableDoubleColumn col = Column.nullable("colname",
                11.1, 22.2, null, 44.4, null);

        assertTrue(col.typeCode() == NullableDoubleColumn.TYPE_CODE);
        assertTrue(col.typeName().equals("double"));
        assertTrue(col.isNullable());
        assertTrue(col.isNumeric());
        assertTrue(col.getName().equals("colname"));
        assertTrue(col.capacity() == 5);
    }

    @Test
    public void testConstructNullableStringColumn(){
        NullableColumn col = new NullableStringColumn(new String[]{"AAA", "", null, "", "AAE"});
        assertTrue(col.typeCode() == NullableStringColumn.TYPE_CODE);
        assertTrue(col.typeName().equals("string"));
        assertTrue(col.isNullable());
        assertFalse(col.isNumeric());
        assertNull(col.getName());
        assertTrue(col.capacity() == 5);
    }

    @Test
    public void testConstructNamedNullableStringColumn(){
        NullableColumn col = new NullableStringColumn("colname",
                new String[]{"", "AAB", null, "AAD", "AAE"});

        assertTrue(col.typeCode() == NullableStringColumn.TYPE_CODE);
        assertTrue(col.typeName().equals("string"));
        assertTrue(col.isNullable());
        assertFalse(col.isNumeric());
        assertTrue(col.getName().equals("colname"));
        assertTrue(col.capacity() == 5);
    }

    @Test
    public void testStaticConstructNamedNullableStringColumn(){
        NullableStringColumn col = Column.nullable("colname", "AAA", null, "", "", null);
        assertTrue(col.typeCode() == NullableStringColumn.TYPE_CODE);
        assertTrue(col.typeName().equals("string"));
        assertTrue(col.isNullable());
        assertFalse(col.isNumeric());
        assertTrue(col.getName().equals("colname"));
        assertTrue(col.capacity() == 5);
    }

    @Test
    public void testConstructNullableCharColumn(){
        NullableColumn col = new NullableCharColumn(new Character[]{'A', null, 'C', '?', null});
        assertTrue(col.typeCode() == NullableCharColumn.TYPE_CODE);
        assertTrue(col.typeName().equals("char"));
        assertTrue(col.isNullable());
        assertFalse(col.isNumeric());
        assertNull(col.getName());
        assertTrue(col.capacity() == 5);
    }

    @Test
    public void testConstructNamedNullableCharColumn(){
        NullableColumn col = new NullableCharColumn("colname",
                new Character[]{null, null, 'C', 'D', 'E'});

        assertTrue(col.typeCode() == NullableCharColumn.TYPE_CODE);
        assertTrue(col.typeName().equals("char"));
        assertTrue(col.isNullable());
        assertFalse(col.isNumeric());
        assertTrue(col.getName().equals("colname"));
        assertTrue(col.capacity() == 5);
    }

    @Test
    public void testStaticConstructNamedNullableCharColumn(){
        NullableCharColumn col = Column.nullable("colname", 'A', 'B', 'C', 'D', 'E');
        assertTrue(col.typeCode() == NullableCharColumn.TYPE_CODE);
        assertTrue(col.typeName().equals("char"));
        assertTrue(col.isNullable());
        assertFalse(col.isNumeric());
        assertTrue(col.getName().equals("colname"));
        assertTrue(col.capacity() == 5);
    }

    @Test
    public void testConstructNullableBooleanColumn(){
        NullableColumn col = new NullableBooleanColumn(
                new Boolean[]{true, null, null, false, true});

        assertTrue(col.typeCode() == NullableBooleanColumn.TYPE_CODE);
        assertTrue(col.typeName().equals("boolean"));
        assertTrue(col.isNullable());
        assertFalse(col.isNumeric());
        assertNull(col.getName());
        assertTrue(col.capacity() == 5);
    }

    @Test
    public void testConstructNamedNullableBooleanColumn(){
        NullableColumn col = new NullableBooleanColumn("colname",
                new Boolean[]{true, false, null, false, null});

        assertTrue(col.typeCode() == NullableBooleanColumn.TYPE_CODE);
        assertTrue(col.typeName().equals("boolean"));
        assertTrue(col.isNullable());
        assertFalse(col.isNumeric());
        assertTrue(col.getName().equals("colname"));
        assertTrue(col.capacity() == 5);
    }

    @Test
    public void testStaticConstructNamedNullableBooleanColumn(){
        NullableBooleanColumn col = Column.nullable("colname",
                null, null, true, false, null);

        assertTrue(col.typeCode() == NullableBooleanColumn.TYPE_CODE);
        assertTrue(col.typeName().equals("boolean"));
        assertTrue(col.isNullable());
        assertFalse(col.isNumeric());
        assertTrue(col.getName().equals("colname"));
        assertTrue(col.capacity() == 5);
    }

    @Test
    public void testConstructNullableBinaryColumn(){
        NullableColumn col = new NullableBinaryColumn(new byte[][]{
            new byte[]{0x01, 0x01},
            new byte[]{0x02, 0x02},
            null,
            null,
            new byte[]{0x05, 0x05}
        });
        assertTrue(col.typeCode() == NullableBinaryColumn.TYPE_CODE);
        assertTrue(col.typeName().equals("binary"));
        assertTrue(col.isNullable());
        assertFalse(col.isNumeric());
        assertNull(col.getName());
        assertTrue(col.capacity() == 5);
    }

    @Test
    public void testConstructNamedNullableBinaryColumn(){
        NullableColumn col = new NullableBinaryColumn("colname", new byte[][]{
                null,
                new byte[]{0x02, 0x02},
                null,
                new byte[]{0x04, 0x04},
                null
        });
        assertTrue(col.typeCode() == NullableBinaryColumn.TYPE_CODE);
        assertTrue(col.typeName().equals("binary"));
        assertTrue(col.isNullable());
        assertFalse(col.isNumeric());
        assertTrue(col.getName().equals("colname"));
        assertTrue(col.capacity() == 5);
    }

    @Test
    public void testStaticConstructNamedNullableBinaryColumn(){
        NullableBinaryColumn col = Column.nullable("colname",
                new byte[]{0x01, 0x01},
                null,
                new byte[]{0x03, 0x03},
                null,
                new byte[]{0x05, 0x05});

        assertTrue(col.typeCode() == NullableBinaryColumn.TYPE_CODE);
        assertTrue(col.typeName().equals("binary"));
        assertTrue(col.isNullable());
        assertFalse(col.isNumeric());
        assertTrue(col.getName().equals("colname"));
        assertTrue(col.capacity() == 5);
    }



    //********************************************//
    //              Column Conversion             //
    //********************************************//



    @Test
    public void testConvertByteColumn(){
        Column col = new ByteColumn("col", new byte[]{11, 22, 33, 44, 55});
        for(Column colClass : allColumnClasses){
            Column converted = col.convertTo(colClass.typeCode());
            assertTrue("Invalid Column type",
                    converted.getClass().getName().equals(colClass.getClass().getName()));
        }
    }

    @Test
    public void testConvertShortColumn(){
        Column col = new ShortColumn("col", new short[]{11, 22, 33, 44, 55});
        for(Column colClass : allColumnClasses){
            Column converted = col.convertTo(colClass.typeCode());
            assertTrue("Invalid Column type",
                    converted.getClass().getName().equals(colClass.getClass().getName()));
        }
    }

    @Test
    public void testConvertIntColumn(){
        Column col = new IntColumn("col", new int[]{11, 22, 33, 44, 55});
        for(Column colClass : allColumnClasses){
            Column converted = col.convertTo(colClass.typeCode());
            assertTrue("Invalid Column type",
                    converted.getClass().getName().equals(colClass.getClass().getName()));
        }
    }

    @Test
    public void testConvertLongColumn(){
        Column col = new LongColumn("col", new long[]{11L, 22L, 33L, 44L, 55L});
        for(Column colClass : allColumnClasses){
            Column converted = col.convertTo(colClass.typeCode());
            assertTrue("Invalid Column type",
                    converted.getClass().getName().equals(colClass.getClass().getName()));
        }
    }

    @Test
    public void testConvertFloatColumn(){
        Column col = new FloatColumn("col", new float[]{11.0f, 22.0f, 33.0f, 44.0f, 55.0f});
        for(Column colClass : allColumnClasses){
            Column converted = col.convertTo(colClass.typeCode());
            assertTrue("Invalid Column type",
                    converted.getClass().getName().equals(colClass.getClass().getName()));
        }
    }

    @Test
    public void testConvertDoubleColumn(){
        Column col = new DoubleColumn("col", new double[]{11.0, 22.0, 33.0, 44.0, 55.0});
        for(Column colClass : allColumnClasses){
            Column converted = col.convertTo(colClass.typeCode());
            assertTrue("Invalid Column type",
                    converted.getClass().getName().equals(colClass.getClass().getName()));
        }
    }

    @Test
    public void testConvertStringColumn(){
        Column col = new StringColumn("col",
                new String[]{"1", "0", "0", "1", "1"});

        Column colHex = new StringColumn("col",
                new String[]{"11aa", "22bb", "33cc", "ff", "5566ef"});

        for(Column colClass : allColumnClasses){
            Column converted;
            if((colClass.typeCode() == BinaryColumn.TYPE_CODE)
                    || (colClass.typeCode() == NullableBinaryColumn.TYPE_CODE)){

                converted = colHex.convertTo(colClass.typeCode());
            }else{
                converted = col.convertTo(colClass.typeCode());

            }
            assertTrue("Invalid Column type",
                    converted.getClass().getName().equals(colClass.getClass().getName()));
        }
    }

    @Test
    public void testConvertCharColumn(){
        Column col = new CharColumn("col", new char[]{'1', '0', '1', '0', '1'});
        for(Column colClass : allColumnClasses){
            Column converted = col.convertTo(colClass.typeCode());
            assertTrue("Invalid Column type",
                    converted.getClass().getName().equals(colClass.getClass().getName()));
        }
    }

    @Test
    public void testConvertBooleanColumn(){
        Column col = new BooleanColumn("col",
                new boolean[]{true, false, true, false, true});

        for(Column colClass : allColumnClasses){
            Column converted = col.convertTo(colClass.typeCode());
            assertTrue("Invalid Column type",
                    converted.getClass().getName().equals(colClass.getClass().getName()));
        }
    }

    @Test
    public void testConvertBinaryColumn(){
        Column colInt = new BinaryColumn("col", new byte[][]{
            BitVector.fromHexString("0001").asArray(),
            BitVector.fromHexString("0002").asArray(),
            BitVector.fromHexString("03").asArray(),
            BitVector.fromHexString("0004").asArray(),
            BitVector.fromHexString("05").asArray()});

        Column colChar = new BinaryColumn("col", new byte[][]{
            BitVector.fromHexString("41").asArray(),
            BitVector.fromHexString("42").asArray(),
            BitVector.fromHexString("43").asArray(),
            BitVector.fromHexString("44").asArray(),
            BitVector.fromHexString("45").asArray()});

        for(Column colClass : allColumnClasses){
            Column converted;
            if((colClass.typeCode() == CharColumn.TYPE_CODE)
                    || (colClass.typeCode() == NullableCharColumn.TYPE_CODE)){

                converted = colChar.convertTo(colClass.typeCode());
            }else{
                converted = colInt.convertTo(colClass.typeCode());
            }

            assertTrue("Invalid Column type",
                    converted.getClass().getName().equals(colClass.getClass().getName()));
        }
    }

    @Test
    public void testConvertNullableByteColumn(){
        Column col = new NullableByteColumn("col", new Byte[]{11, null, 33, null, 55});
        for(Column colClass : allColumnClasses){
            Column converted = col.convertTo(colClass.typeCode());
            assertTrue("Invalid Column type",
                    converted.getClass().getName().equals(colClass.getClass().getName()));
        }
    }

    @Test
    public void testConvertNullableShortColumn(){
        Column col = new NullableShortColumn("col", new Short[]{11, null, 33, null, 55});
        for(Column colClass : allColumnClasses){
            Column converted = col.convertTo(colClass.typeCode());
            assertTrue("Invalid Column type",
                    converted.getClass().getName().equals(colClass.getClass().getName()));
        }
    }

    @Test
    public void testConvertNullableIntColumn(){
        Column col = new NullableIntColumn("col", new Integer[]{11, null, 33, null, 55});
        for(Column colClass : allColumnClasses){
            Column converted = col.convertTo(colClass.typeCode());
            assertTrue("Invalid Column type",
                    converted.getClass().getName().equals(colClass.getClass().getName()));
        }
    }

    @Test
    public void testConvertNullableLongColumn(){
        Column col = new NullableLongColumn("col",
                new Long[]{11L, null, 33L, null, 55L});

        for(Column colClass : allColumnClasses){
            Column converted = col.convertTo(colClass.typeCode());
            assertTrue("Invalid Column type",
                    converted.getClass().getName().equals(colClass.getClass().getName()));
        }
    }

    @Test
    public void testConvertNullableFloatColumn(){
        Column col = new NullableFloatColumn("col",
                new Float[]{11.0f, null, 33.0f, null, 55.0f});

        for(Column colClass : allColumnClasses){
            Column converted = col.convertTo(colClass.typeCode());
            assertTrue("Invalid Column type",
                    converted.getClass().getName().equals(colClass.getClass().getName()));
        }
    }

    @Test
    public void testConvertNullableDoubleColumn(){
        Column col = new NullableDoubleColumn("col",
                new Double[]{11.0, null, 33.0, null, 55.0});

        for(Column colClass : allColumnClasses){
            Column converted = col.convertTo(colClass.typeCode());
            assertTrue("Invalid Column type",
                    converted.getClass().getName().equals(colClass.getClass().getName()));
        }
    }

    @Test
    public void testConvertNullableStringColumn(){
        Column col = new NullableStringColumn("col",
                new String[]{"1", null, "0", null, "1"});

        Column colHex = new NullableStringColumn("col",
                new String[]{"11aa", null, "33cc", null, "ef"});

        for(Column colClass : allColumnClasses){
            Column converted;
            if((colClass.typeCode() == BinaryColumn.TYPE_CODE)
                    || (colClass.typeCode() == NullableBinaryColumn.TYPE_CODE)){

                converted = colHex.convertTo(colClass.typeCode());
            }else{
                converted = col.convertTo(colClass.typeCode());
            }

            assertTrue("Invalid Column type",
                    converted.getClass().getName().equals(colClass.getClass().getName()));
        }
    }

    @Test
    public void testConvertNullableCharColumn(){
        Column col = new NullableCharColumn("col",
                new Character[]{'1', null, '1', null, '1'});

        for(Column colClass : allColumnClasses){
            Column converted = col.convertTo(colClass.typeCode());
            assertTrue("Invalid Column type",
                    converted.getClass().getName().equals(colClass.getClass().getName()));
        }
    }

    @Test
    public void testConvertNullableBooleanColumn(){
        Column col = new NullableBooleanColumn("col",
                new Boolean[]{true, null, false, null, true});

        for(Column colClass : allColumnClasses){
            Column converted = col.convertTo(colClass.typeCode());
            assertTrue("Invalid Column type",
                    converted.getClass().getName().equals(colClass.getClass().getName()));
        }
    }

    @Test
    public void testConvertNullableBinaryColumn(){
        Column colInt = new NullableBinaryColumn("col", new byte[][]{
            BitVector.fromHexString("0001").asArray(),
            null,
            BitVector.fromHexString("03").asArray(),
            null,
            BitVector.fromHexString("05").asArray()});

        Column colChar = new NullableBinaryColumn("col", new byte[][]{
            BitVector.fromHexString("41").asArray(),
            null,
            BitVector.fromHexString("43").asArray(),
            null,
            BitVector.fromHexString("45").asArray()});

        for(Column colClass : allColumnClasses){
            Column converted;
            if((colClass.typeCode() == CharColumn.TYPE_CODE)
                    || (colClass.typeCode() == NullableCharColumn.TYPE_CODE)){

                converted = colChar.convertTo(colClass.typeCode());
            }else{
                converted = colInt.convertTo(colClass.typeCode());
            }

            assertTrue("Invalid Column type",
                    converted.getClass().getName().equals(colClass.getClass().getName()));
        }
    }



    //********************************************//
    //              Utility Functions             //
    //********************************************//


    @Test
    public void testStaticLike(){
        Column col1 = new IntColumn("myCol", new int[]{11, 22, 33, 44, 55});
        Column col2 = Column.like(col1, 15);
        assertTrue(col2 instanceof IntColumn);
        assertTrue(col2.getName() == col1.getName());
        assertTrue(col2.capacity() == 15);
    }

    @Test
    public void testStaticLikeZeroLength(){
        Column col1 = new IntColumn("myCol", new int[]{11, 22, 33, 44, 55});
        Column col2 = Column.like(col1);
        assertTrue(col2 instanceof IntColumn);
        assertTrue(col2.getName() == col1.getName());
        assertTrue(col2.capacity() == 0);
    }

    @Test
    public void testStaticLikeNullArg(){
        Column col1 = Column.like(null);
        assertNull(col1);
    }

    @Test
    public void testStaticOfType(){
        Column col2 = Column.ofType(IntColumn.TYPE_CODE, 15);
        assertTrue(col2 instanceof IntColumn);
        assertTrue(col2.capacity() == 15);
    }

    @Test
    public void testStaticOfTypeZeroLength(){
        Column col2 = Column.ofType(IntColumn.TYPE_CODE);
        assertTrue(col2 instanceof IntColumn);
        assertTrue(col2.capacity() == 0);
    }

    @Test
    public void testStaticOfTypeInvalidType(){
        Column col2 = Column.ofType((byte)-9);
        assertNull(col2);
    }

}
