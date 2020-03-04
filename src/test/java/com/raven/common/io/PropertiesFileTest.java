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

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for the PropertiesFile implementation.
 *
 */
public class PropertiesFileTest {

    @BeforeClass
    public static void setUpBeforeClass(){ }

    @AfterClass
    public static void tearDownAfterClass(){ }

    @Before
    public void setUp(){ }

    @After
    public void tearDown(){ }

    @Test
    public void testSet(){
        PropertiesFile prop = new PropertiesFile();
        prop.setProperty("key1", "val1");
        prop.setProperty("key2", "val2");
        prop.setProperty("key3", "val3");

        assertEquals("val1", prop.getProperty("key1"));
        assertEquals("val2", prop.getProperty("key2"));
        assertEquals("val3", prop.getProperty("key3"));

        prop.setProperty("key4", "val4");
        prop.setProperty("key5", "val5");
        prop.setProperty("key6", "val6");

        assertEquals("val1", prop.getProperty("key1"));
        assertEquals("val2", prop.getProperty("key2"));
        assertEquals("val3", prop.getProperty("key3"));
        assertEquals("val4", prop.getProperty("key4"));
        assertEquals("val5", prop.getProperty("key5"));
        assertEquals("val6", prop.getProperty("key6"));
    }

    @Test
    public void testSetOverride(){
        PropertiesFile prop = new PropertiesFile();
        prop.setProperty("key1", "val1");
        prop.setProperty("key2", "val2");
        prop.setProperty("key3", "val3");

        assertEquals("val1", prop.getProperty("key1"));
        assertEquals("val2", prop.getProperty("key2"));
        assertEquals("val3", prop.getProperty("key3"));

        prop.setProperty("key1", "TEST1");
        prop.setProperty("key2", "TEST2");
        prop.setProperty("key3", "TEST3");

        assertEquals("TEST1", prop.getProperty("key1"));
        assertEquals("TEST2", prop.getProperty("key2"));
        assertEquals("TEST3", prop.getProperty("key3"));
    }

}
