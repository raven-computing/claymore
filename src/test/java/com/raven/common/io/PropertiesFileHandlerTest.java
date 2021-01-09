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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map.Entry;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for the PropertiesFileHandler implementation.
 *
 */
public class PropertiesFileHandlerTest {

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
        PropertiesFile prop = new PropertiesFile();

        prop.setProperty("key1", "val1");
        prop.setProperty("key2", "val2");
        prop.setProperty("key3", "val3");

        //Write
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PropertiesFileHandler.writeTo(baos, prop);

        //Read
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        PropertiesFile copy = PropertiesFileHandler.readFrom(bais);
        for(Entry<String, String> e : prop.allProperties()){
            assertTrue("Properties keys do not match",
                    (e.getKey() != null)
                    && (copy.getProperty(e.getKey()) != null));

            assertEquals("Properties values do not match",
                    e.getValue(),
                    copy.getProperty(e.getKey()));

        }
    }
}
