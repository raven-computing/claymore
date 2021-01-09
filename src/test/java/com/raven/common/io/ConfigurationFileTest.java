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

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.raven.common.io.ConfigurationFile.Section;
import com.raven.common.struct.Item;

/**
 * Tests for the ConfigurationFile implementation.
 *
 */
public class ConfigurationFileTest {

    @BeforeClass
    public static void setUpBeforeClass(){ }

    @AfterClass
    public static void tearDownAfterClass(){ }

    @Before
    public void setUp(){ }

    @After
    public void tearDown(){ }

    @Test
    public void testAdd(){
        ConfigurationFile config = new ConfigurationFile();
        config.addSection(new Section("Sec1")
                .set("1key1", "1val1")
                .set("1key2", "1val2")
                .set("1key3", "1val3"));

        config.addSection(new Section("Sec2")
                .set("2key1", "2val1")
                .set("2key2", "2val2")
                .set("2key3", "2val3"));

        config.addSection(new Section("Sec3")
                .set("3key1", "3val1")
                .set("3key2", "3val2")
                .set("3key3", "3val3"));

        int sections = 0;
        for(Section section : config){
            int entries = 0;
            for(Item<String> item : section){
                String postfix = section.getName().substring(section.getName().length()-1);
                assertTrue(item.getKey().startsWith(postfix + "key"));
                assertTrue(item.getKey().startsWith(postfix));
                assertTrue(item.getValue().endsWith("1")
                        || item.getValue().endsWith("2")
                        || item.getValue().endsWith("3"));

                ++entries;
            }
            assertTrue(entries == 3);
            ++sections;
        }
        assertTrue(sections == 3);
    }

    @Test
    public void testAddDuplicate(){
        ConfigurationFile config = new ConfigurationFile();
        config.addSection(new Section("Sec1")
                .set("key1", "val1")
                .set("key2", "val2")
                .set("key3", "val3"));

        config.addSection(new Section("Sec2")
                .set("key1", "val1")
                .set("key2", "val2")
                .set("key3", "val3"));

        config.addSection(new Section("Sec3")
                .set("key1", "val1")
                .set("key2", "val2")
                .set("key3", "val3"));

        int sections = 0;
        for(Section section : config){
            int entries = 0;
            for(Item<String> item : section){
                assertTrue(item.getKey().startsWith("key"));
                assertTrue(item.getValue().startsWith("val"));
                assertTrue(item.getValue().endsWith("1")
                        || item.getValue().endsWith("2")
                        || item.getValue().endsWith("3"));

                ++entries;
            }
            assertTrue(entries == 3);
            ++sections;
        }
        assertTrue(sections == 3);
    }

    @Test
    public void testSet(){
        ConfigurationFile config = new ConfigurationFile();
        config.addSection(new Section("Sec1")
                .set("key1", "val1")
                .set("key2", "val2")
                .set("key3", "val3"));

        assertEquals("val1", config.getSection("Sec1").valueOf("key1"));
        assertEquals("val2", config.getSection("Sec1").valueOf("key2"));
        assertEquals("val3", config.getSection("Sec1").valueOf("key3"));

        config.getSection("Sec1").set("key4", "val4");
        config.getSection("Sec1").set("key5", "val5");
        config.getSection("Sec1").set("key6", "val6");

        assertEquals("val1", config.getSection("Sec1").valueOf("key1"));
        assertEquals("val2", config.getSection("Sec1").valueOf("key2"));
        assertEquals("val3", config.getSection("Sec1").valueOf("key3"));
        assertEquals("val4", config.getSection("Sec1").valueOf("key4"));
        assertEquals("val5", config.getSection("Sec1").valueOf("key5"));
        assertEquals("val6", config.getSection("Sec1").valueOf("key6"));
    }

    @Test
    public void testSetOverride(){
        ConfigurationFile config = new ConfigurationFile();
        config.addSection(new Section("Sec1")
                .set("key1", "val1")
                .set("key2", "val2")
                .set("key3", "val3"));

        assertEquals("val1", config.getSection("Sec1").valueOf("key1"));
        assertEquals("val2", config.getSection("Sec1").valueOf("key2"));
        assertEquals("val3", config.getSection("Sec1").valueOf("key3"));

        config.getSection("Sec1").set("key1", "TEST1");
        config.getSection("Sec1").set("key2", "TEST2");
        config.getSection("Sec1").set("key3", "TEST3");

        assertEquals("TEST1", config.getSection("Sec1").valueOf("key1"));
        assertEquals("TEST2", config.getSection("Sec1").valueOf("key2"));
        assertEquals("TEST3", config.getSection("Sec1").valueOf("key3"));
    }

    @Test
    public void testSetSectionName(){
        ConfigurationFile config = new ConfigurationFile();
        config.addSection(new Section("Sec1")
                .set("key1", "val1")
                .set("key2", "val2")
                .set("key3", "val3"));

        config.setSectionName("Sec1", "TESTNAME");

        assertTrue(config.getSection("Sec1") == null);
        assertEquals("val1", config.getSection("TESTNAME").valueOf("key1"));
        assertEquals("val2", config.getSection("TESTNAME").valueOf("key2"));
        assertEquals("val3", config.getSection("TESTNAME").valueOf("key3"));
    }
}
