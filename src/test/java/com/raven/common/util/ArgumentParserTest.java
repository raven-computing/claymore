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

package com.raven.common.util;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.raven.common.util.ArgumentParseException;
import com.raven.common.util.ArgumentParser;

/**
 * Tests for the ArgumentParser and ArgumentParser.Builder implementation.
 *
 */
public class ArgumentParserTest {

    @BeforeClass
    public static void setUpBeforeClass(){ }

    @AfterClass
    public static void tearDownAfterClass(){ }

    @Before
    public void setUp(){ }

    @After
    public void tearDown(){ }

    @Test
    public void testBuilder(){
        try{
            new ArgumentParser.Builder()
            .optionalIntegerArg("a1")
            .optionalStringArg("a2")
            .optionalBooleanArg("a3")
            .build();

            new ArgumentParser.Builder()
            .integerArg("a1")
            .stringArg("a2")
            .booleanArg("a3")
            .build();

        }catch(Exception ex){
            fail("ArgumentParser.Builder should not throw an unexpected Exception");
        }
    }

    @Test
    public void testParseSimpleOptional() throws Exception{

        String[] args = new String[]{"-a1=123","-a2=blablub","-a3=true"};

        ArgumentParser ap = new ArgumentParser.Builder()
                .optionalIntegerArg("a1")
                .optionalStringArg("a2")
                .optionalBooleanArg("a3")
                .build();

        //should succeed
        ap.parse(args);
    }

    @Test
    public void testParseSimple() throws Exception{

        String[] args = new String[]{"-a1=123","-a2=blablub","-a3=true"};

        ArgumentParser ap = new ArgumentParser.Builder()
                .integerArg("a1")
                .stringArg("a2")
                .booleanArg("a3")
                .build();

        //should succeed
        ap.parse(args);
    }

    @Test(expected=ArgumentParseException.class)
    public void testParseSimpleMissingException() throws Exception{

        String[] args = new String[]{"-a1=123","-a2=blablub"};

        ArgumentParser ap = new ArgumentParser.Builder()
                .integerArg("a1")
                .stringArg("a2")
                .booleanArg("a3")
                .build();

        //should fail because of missing argument
        ap.parse(args);
    }

    @Test(expected=ArgumentParseException.class)
    public void testParseWrongTypeException() throws Exception{

        String[] args = new String[]{"-a1=123character","-a2=blablub"};

        ArgumentParser ap = new ArgumentParser.Builder()
                .integerArg("a1")
                .stringArg("a2")
                .build();

        //should fail because -a1 has wrong type
        ap.parse(args);
    }

    @Test(expected=ArgumentParseException.class)
    public void testParseEmptyStringException() throws Exception{

        String[] args = new String[]{"-a1=123","-a2="};

        ArgumentParser ap = new ArgumentParser.Builder()
                .integerArg("a1")
                .stringArg("a2")
                .build();

        //should fail because -a2 is empty
        ap.parse(args);
    }

    @Test
    public void testGetArguments() throws Exception{

        String[] args = new String[]{"-a1=123","-a2=blablub","-a3=true"};

        ArgumentParser ap = new ArgumentParser.Builder()
                .optionalIntegerArg("a1")
                .optionalStringArg("a2")
                .optionalBooleanArg("a3")
                .build();

        //should succeed
        ap.parse(args);

        Integer i = ap.getIntegerArg("a1");
        String s = ap.getStringArg("a2");
        Boolean b = ap.getBooleanArg("a3");

        assertTrue("Parsing Error", i == 123);
        assertEquals("Parsing Error", "blablub", s);
        assertTrue("Parsing Error", b);
    }

    @Test
    public void testGetOptionalMissingArguments() throws Exception{

        String[] args = new String[]{"-a1=123"};

        ArgumentParser ap = new ArgumentParser.Builder()
                .optionalIntegerArg("a1")
                .optionalStringArg("a2")
                .optionalBooleanArg("a3")
                .build();

        //should succeed
        ap.parse(args);

        Integer i = ap.getIntegerArg("a1");
        String s = ap.getStringArg("a2");
        Boolean b = ap.getBooleanArg("a3");

        assertTrue("Parsing Error", i == 123);
        assertNull("Parsing Error. (Should be null)", s);
        assertNull("Parsing Error. (Should be null)", b);
    }

    @Test
    public void testHelperArgument1() throws Exception{

        String[] args = new String[]{"-?"};

        ArgumentParser ap = new ArgumentParser.Builder()
                .optionalIntegerArg("a1")
                .optionalStringArg("a2")
                .optionalBooleanArg("a3")
                .build();

        //should succeed
        ap.parse(args);

        assertTrue("Parsing Error. Helper argument should have been triggered",
                ap.helpTriggered());
    }

    @Test
    public void testHelperArgument2() throws Exception{

        String[] args = new String[]{"-HeLp"};//case insensitive

        ArgumentParser ap = new ArgumentParser.Builder()
                .optionalIntegerArg("a1")
                .optionalStringArg("a2")
                .optionalBooleanArg("a3")
                .build();

        //should succeed
        ap.parse(args);

        assertTrue("Parsing Error. Helper argument should have been triggered",
                ap.helpTriggered());
    }

    @Test
    public void testHelperArgument3() throws Exception{

        String[] args = new String[]{"-VerSIon"};//case insensitive

        ArgumentParser ap = new ArgumentParser.Builder()
                .optionalIntegerArg("a1")
                .optionalStringArg("a2")
                .optionalBooleanArg("a3")
                .build();

        //should succeed
        ap.parse(args);

        assertTrue("Parsing Error. Helper argument should have been triggered",
                ap.versionTriggered());
    }

    @Test
    public void testParseRestrictedInt() throws Exception{

        String[] args = new String[]{"-a1=15","-a2=blablub","-a3=true"};

        ArgumentParser ap = new ArgumentParser.Builder()
                .integerArg("a1", 10, 20)
                .stringArg("a2")
                .booleanArg("a3")
                .build();

        //should succeed
        ap.parse(args);
    }

    @Test(expected=ArgumentParseException.class)
    public void testParseRestrictedIntException() throws Exception{

        String[] args = new String[]{"-a1=22","-a2=blablub","-a3=true"};

        ArgumentParser ap = new ArgumentParser.Builder()
                .integerArg("a1", 10, 20)
                .stringArg("a2")
                .booleanArg("a3")
                .build();

        //should fail
        ap.parse(args);
    }

    @Test
    public void testParseRestrictedString() throws Exception{

        String[] args = new String[]{"-a1=123","-a2=blue","-a3=true"};

        Set<String> set = new HashSet<String>(Arrays.asList(new String[]{"red", "green", "blue"}));
        ArgumentParser ap = new ArgumentParser.Builder()
                .integerArg("a1")
                .stringArg("a2", set)
                .booleanArg("a3")
                .build();

        //should succeed
        ap.parse(args);
    }

    @Test(expected=ArgumentParseException.class)
    public void testParseRestrictedStringException() throws Exception{

        String[] args = new String[]{"-a1=22","-a2=LaLaLa","-a3=true"};

        Set<String> set = new HashSet<String>(Arrays.asList(new String[]{"red", "green", "blue"}));
        ArgumentParser ap = new ArgumentParser.Builder()
                .integerArg("a1")
                .stringArg("a2", set)
                .booleanArg("a3")
                .build();

        //should fail
        ap.parse(args);
    }

}
