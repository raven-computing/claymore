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

/**
 * Tests for the WritableItem implementation.
 *
 */
public class WritableItemTest {
    
    WritableItem<String> i1;
    WritableItem<Double> i2;
    
    @BeforeClass
    public static void setUpBeforeClass(){ }

    @AfterClass
    public static void tearDownAfterClass(){ }

    @Before
    public void setUp(){
        i1 = new WritableItem<>("key1", "value1");
        i2 = new WritableItem<>("key2", 123.456);
    }

    @After
    public void tearDown(){ }

    @Test
    public void testGetKey(){
        assertTrue(i1.getKey().equals("key1"));
        assertTrue(i2.getKey().equals("key2"));
    }

    @Test
    public void testSetKey(){
        i1.setKey("ABCD");
        assertTrue(i1.getKey().equals("ABCD"));
        i2.setKey(null);
        assertTrue(i2.getKey() == null);
    }

    @Test
    public void testHasKey(){
        assertTrue(i1.hasKey());
        assertTrue(i2.hasKey());
        i1.setKey(null);
        assertFalse(i1.hasKey());
    }

    @Test
    public void testGetValue(){
        assertTrue(i1.getValue().equals("value1"));
        assertTrue(i2.getValue().equals(123.456));
    }

    @Test
    public void testSetValue(){
        i1.setValue("AAAABBBB");
        assertTrue(i1.getValue().equals("AAAABBBB"));
        i2.setValue(2.0);
        assertTrue(i2.getValue().equals(2.0));
        i1.setValue(null);
        assertTrue(i1.getValue() == null);
    }

    @Test
    public void testHasValue(){
        assertTrue(i1.hasValue());
        assertTrue(i2.hasValue());
        i1.setValue(null);
        i2.setValue(null);
        assertFalse(i1.hasValue());
        assertFalse(i2.hasValue());
    }

    @Test
    public void testIsEmpty(){
        assertFalse(i1.isEmpty());
        i1.setKey(null);
        i1.setValue(null);
        assertTrue(i1.isEmpty());
    }

    @Test
    public void testEqualsObject(){
        WritableItem<String> item = new WritableItem<>("key", "value1");
        assertFalse(item.equals(i1));
        item.setKey("key1");
        assertTrue(item.equals(i1));
    }
    
}
