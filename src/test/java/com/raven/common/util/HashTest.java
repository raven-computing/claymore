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

package com.raven.common.util;

import static org.junit.Assert.*;

import java.io.UnsupportedEncodingException;

import static com.raven.common.struct.BitVector.wrap;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for the Hash class.
 *
 */
public class HashTest {
    
    String input = "This is a test string Bla bla";

    @BeforeClass
    public static void setUpBeforeClass(){ }

    @AfterClass
    public static void tearDownAfterClass(){ }

    @Before
    public void setUp(){ }

    @After
    public void tearDown(){ }

    @Test
    public void testMd5(){
        String hash = str(Hash.md5(input));
        assertEquals("Hashes do not match",
                "fbd69a5010bd842f2aa5bbe83122f6f3",
                hash);
    }
    
    @Test
    public void testSha1(){
        String hash = str(Hash.sha1(input));
        assertEquals("Hashes do not match",
                "f0ec271bb4e265420e94ce08d6bf17ecea0afd00",
                hash);
    }
    
    @Test
    public void testSha224(){
        String hash = str(Hash.sha224(input));
        assertEquals("Hashes do not match",
                "fd7c603388c5eebbea1a2aed2c2fa10a2e1d9724079179a3782392ad",
                hash);
    }
    
    @Test
    public void testSha256(){
        String hash = str(Hash.sha256(input));
        assertEquals("Hashes do not match",
                "8603f2c60b1448d0eea7d7162f34512d729bd99ae08f2ca8701a8b1ea24402ce",
                hash);
    }
    
    @Test
    public void testSha384(){
        String hash = str(Hash.sha384(input));
        assertEquals("Hashes do not match",
                "3a7a927c1b38d078fbb6a4d8307e722c5cc788795284ffaf9f89c110e1e5b47a914529fd69cf4be48d8627a7744d881d",
                hash);
    }
    
    @Test
    public void testSha512(){
        String hash = str(Hash.sha512(input));
        assertEquals("Hashes do not match",
                "2c439097601615774a6518b2fb19e48bb2849e3cc915cc5e36bcd81f48197ea350169dbfc570a2a3388c8eae2a0d42d603513d7d7131abfec3ba88452422082b",
                hash);
    }
    
    @Test
    public void testOfHashFunction() throws UnsupportedEncodingException{
        String hash = str(Hash.of(HashFunction.SHA_256, input.getBytes("UTF-8")));
        assertEquals("Hashes do not match",
                "8603f2c60b1448d0eea7d7162f34512d729bd99ae08f2ca8701a8b1ea24402ce",
                hash);
    }
    
    private String str(byte[] hash){
        return wrap(hash).toHexString();
    }
    
}
