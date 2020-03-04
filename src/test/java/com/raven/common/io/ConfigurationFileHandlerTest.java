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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.raven.common.io.ConfigurationFile.Section;
import com.raven.common.struct.Item;

/**
 * Tests for the ConfigurationFileHandler implementation.
 *
 */
public class ConfigurationFileHandlerTest {

    @BeforeClass
    public static void setUpBeforeClass(){ }

    @AfterClass
    public static void tearDownAfterClass(){ }

    @Before
    public void setUp(){ }

    @After
    public void tearDown(){ }

    @Test
    public void testReadWrite() throws IOException{
        ConfigurationFile config = new ConfigurationFile();
        config.addSection(new Section("Sec1")
                .set("key1", "val1")
                .set("key2", "val2")
                .set("key3", "val3"));

        config.addSection(new Section("Sec2")
                .set("key1", "val1")
                .set("key2", "val2")
                .set("key3", "val3"));

        //Write
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ConfigurationFileHandler.writeTo(baos, config);

        //Read
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ConfigurationFile copy = ConfigurationFileHandler.readFrom(bais);
        for(Section section : config){
            for(Item<String> item : section){
                assertTrue("Configuration keys do not match",
                        (item.getKey() != null)
                        && (copy.getSection(section.getName()).valueOf(item.getKey()) != null));

                assertEquals("Configuration values do not match",
                        item.getValue(),
                        copy.getSection(section.getName()).valueOf(item.getKey()));
            }
        }
    }

}
