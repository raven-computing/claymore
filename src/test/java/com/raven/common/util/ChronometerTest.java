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

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for the Chronometer implementation.
 *
 */
public class ChronometerTest {

    @BeforeClass
    public static void setUpBeforeClass(){ }

    @AfterClass
    public static void tearDownAfterClass(){ }

    @Before
    public void setUp(){ }

    @After
    public void tearDown(){ }

    @Test
    public void testTimeMeasurement(){
        Chronometer chron = new Chronometer();
        chron.start();
        waitForTimeElapse();
        chron.stop();
        assertTrue("Measured time of Chronometer should be greater zero.",
                chron.elapsedMillis() > 0);
        
    }
    
    @Test
    public void testTimeMeasurementDuration(){
        Chronometer chron = new Chronometer();
        chron.start();
        waitForTimeElapse();
        chron.stop();
        assertTrue("Measured time of Chronometer should be greater zero.",
                chron.elapsedDuration().toMillis() > 0);
        
    }
    
    @Test
    public void testTimeMeasurementWhileRunning(){
        Chronometer chron = new Chronometer();
        chron.start();
        waitForTimeElapse();
        long t1 = chron.elapsedMillis();
        assertTrue("Measured time of Chronometer should be greater zero.", t1 > 0);
        waitForTimeElapse();
        long t2 = chron.elapsedMillis();
        assertTrue("Measured time of Chronometer should be greater t1.", t2 > t1);
        waitForTimeElapse();
        chron.stop();
        assertTrue("Measured time of Chronometer should be greater t2.",
                chron.elapsedMillis() > t2);
        
    }
    
    @Test
    public void testToString(){
        Chronometer chron = new Chronometer();
        chron.start();
        waitForTimeElapse();
        chron.stop();
        String s = chron.toString();
        assertTrue("Chronometer should not return null or an empty String.",
                (s != null) && !s.isEmpty());
        
    }
    
    @Test
    public void testToStringNoMeasurement(){
        Chronometer chron = new Chronometer();
        String s = chron.toString();
        assertTrue("Chronometer should not return null or an empty String.",
                (s != null) && !s.isEmpty());
        
        assertEquals("Not started Chronometer should return zero", "0", s);
    }
    
    private void waitForTimeElapse(){
        long time = System.currentTimeMillis();
        int attempts = 0;
        final int maxAttempts = 500;
        while((System.currentTimeMillis() <= time) && (attempts < maxAttempts)){
            doWait();
            ++attempts;
        }
        if(attempts >= maxAttempts){
            fail("Failed to measure time difference. Has your system clock changed?");
        }
    }

    private synchronized void doWait(){
        try{
            this.wait(5);
        }catch(InterruptedException ex){ }
    }

}
