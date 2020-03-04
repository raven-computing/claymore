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
 * Tests for the FinalItem implementation.
 *
 */
public class FinalItemTest {

    FinalItem<String> i1;
    FinalItem<Double> i2;

    @BeforeClass
    public static void setUpBeforeClass(){ }

    @AfterClass
    public static void tearDownAfterClass(){ }

    @Before
    public void setUp(){
        i1 = new FinalItem<>("key1", "value1");
        i2 = new FinalItem<>("key2", 123.456);
    }

    @After
    public void tearDown(){ }

    @Test
    public void testGetKey(){
        assertTrue(i1.getKey().equals("key1"));
        assertTrue(i2.getKey().equals("key2"));
    }

    @Test
    public void testHasKey(){
        assertTrue(i1.hasKey());
        assertTrue(i2.hasKey());
        FinalItem<String> fi = new FinalItem<>();
        assertFalse(fi.hasKey());
    }

    @Test
    public void testGetValue(){
        assertTrue(i1.getValue().equals("value1"));
        assertTrue(i2.getValue().equals(123.456));
    }

    @Test
    public void testHasValue(){
        assertTrue(i1.hasValue());
        assertTrue(i2.hasValue());
        FinalItem<String> fi = new FinalItem<>();
        assertFalse(fi.hasValue());
        fi = new FinalItem<>("finalkey");
        assertFalse(fi.hasValue());
    }

    @Test
    public void testIsEmpty(){
        assertFalse(i1.isEmpty());
        FinalItem<String> fi = new FinalItem<>();
        assertTrue(fi.isEmpty());
    }

    @Test
    public void testEqualsObject(){
        FinalItem<String> item = new FinalItem<>("key", "value1");
        assertFalse(item.equals(i1));
        item = new FinalItem<>("key1", "value1");
        assertTrue(item.equals(i1));
    }

}
