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

import com.raven.common.io.StringSerializer;

/**
 * Tests for the StaticBloomFilter implementation.
 *
 */
public class StaticBloomFilterTest {
    
    @BeforeClass
    public static void setUpBeforeClass(){ }

    @AfterClass
    public static void tearDownAfterClass(){ }

    @Before
    public void setUp(){ }

    @After
    public void tearDown(){ }
    
    @Test
    public void testAddSmall(){
        StaticBloomFilter<String> bf = newStringBf(5000, 0.01);
        int max = 2000;
        for(int i=0; i<max; ++i){
            bf.add("elem" + i);   
        }
        for(int i=0; i<max; ++i){
            String element = ("elem" + i);
            assertTrue("Bloom filter should contain element \""
                        + element + "\"", bf.contains(element));   
        }
    }
    
    @Test
    public void testAddLarge(){
        StaticBloomFilter<String> bf = newStringBf(10000, 0.0001);
        int max = 5000;
        for(int i=0; i<max; ++i){
            bf.add("elem" + i);   
        }
        for(int i=0; i<max; ++i){
            String element = ("elem" + i);
            assertTrue("Bloom filter should contain element \""
                        + element + "\"", bf.contains(element));   
        }
    }
    
    @Test
    public void testAddNoSerializer(){
        StaticBloomFilter<String> bf = new StaticBloomFilter<String>(null, 1000, 0.01);
        int max = 600;
        for(int i=0; i<max; ++i){
            bf.add("elem" + i);   
        }
        for(int i=0; i<max; ++i){
            String element = ("elem" + i);
            assertTrue("Bloom filter should contain element \""
                        + element + "\"", bf.contains(element));   
        }
    }
    
    @Test
    public void testAddJavaSerializable(){
        ScalableBloomFilter<Double> bf = new ScalableBloomFilter<Double>(null, 1000, 0.01);
        int max = 600;
        for(int i=0; i<max; ++i){
            bf.add(Double.valueOf(i + ".123456"));   
        }
        for(int i=0; i<max; ++i){
            Double element = Double.valueOf(i + ".123456");
            assertTrue("Bloom filter should contain element \""
                        + element + "\"", bf.contains(element));   
        }
    }
    
    @Test
    public void testAddSerializationByToString(){
        StaticBloomFilter<Object> bf = new StaticBloomFilter<Object>(null, 1000, 0.01);
        int max = 200;
        Object[]  elements = new Object[max];
        for(int i=0; i<max; ++i){
            elements[i] = new Object(); 
        }
        for(int i=0; i<max; ++i){
            bf.add(elements[i]);
        }
        for(int i=0; i<max; ++i){
            Object element = elements[i];
            assertTrue("Bloom filter should contain element \""
                        + element + "\"", bf.contains(element));   
        }
    }

    @Test
    public void testContains(){
        StaticBloomFilter<String> bf = newStringBf(10000, 0.01);
        int max = 2000;
        for(int i=0; i<max; ++i){
            bf.add("elem" + i);   
        }
        for(int i=0; i<max; ++i){
            String element = ("elem" + i);
            assertTrue("Bloom filter should contain element \""
                        + element + "\"", bf.contains(element));   
        }
    }
    
    @Test
    public void testContainsOverflow(){
        StaticBloomFilter<String> bf = newStringBf(1000, 0.01);
        int max = 10000;
        for(int i=0; i<max; ++i){
            bf.add("elem" + i);   
        }
        for(int i=0; i<max; ++i){
            String element = ("elem" + i);
            assertTrue("Bloom filter should contain element \""
                        + element + "\"", bf.contains(element));   
        }
    }
    
    @Test
    public void testIsEmpty(){
        StaticBloomFilter<String> bf = newStringBf(5000, 0.01);
        assertTrue("Bloom filter should be empty", bf.isEmpty());
        bf.add("elem1");
        assertFalse("Bloom filter should not be empty", bf.isEmpty());
        bf = newStringBf(1000, 0.0001);
        assertTrue("Bloom filter should be empty", bf.isEmpty());
        bf.add("elem1");
        assertFalse("Bloom filter should not be empty", bf.isEmpty());
    }

    @Test
    public void testApproximateSize(){
        StaticBloomFilter<String> bf = newStringBf(5000, 0.01);
        assertTrue("Approximate size should be zero", bf.approximateSize() == 0);
        bf.add("elem1");
        assertTrue("Approximate size should not be negative or zero", bf.approximateSize() > 0);
    }
    
    @Test
    public void testSizeInBytes(){
        StaticBloomFilter<String> bf = newStringBf(5000, 0.01);
        assertTrue("Size in bytes should be positive", bf.sizeInBytes() > 0);
        bf.add("elem1");
        assertTrue("Size in bytes should be positive", bf.sizeInBytes() > 0);
    }

    @Test
    public void testClear(){
        StaticBloomFilter<String> bf = newStringBf(5000, 0.01);
        for(int i=0; i<1000; ++i){
            bf.add("elem" + i);   
        }
        bf.clear();
        assertTrue("Bloom filter should be empty", bf.isEmpty());
        assertTrue("Bloom filter should not contain any elements", bf.approximateSize() == 0);
        bf = newStringBf(10000, 0.0001);
        for(int i=0; i<5000; ++i){
            bf.add("elem" + i);   
        }
        bf.clear();
        assertTrue("Bloom filter should be empty", bf.isEmpty());
        assertTrue("Bloom filter should not contain any elements", bf.approximateSize() == 0);
    }
    
    private StaticBloomFilter<String> newStringBf(int initCap, double maxError){
        return new StaticBloomFilter<String>(new StringSerializer(), initCap, maxError);
    }
}
