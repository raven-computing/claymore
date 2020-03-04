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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.*;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZonedDateTime;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for the FutureAction implementation.
 *
 */
public class FutureActionTest {

    @BeforeClass
    public static void setUpBeforeClass(){ }

    @AfterClass
    public static void tearDownAfterClass(){ }

    @Before
    public void setUp(){ }

    @After
    public void tearDown(){ }
    
    @Test
    public void testParametersInSingular(){
        FutureAction action = FutureAction.in(6, SECONDS, () -> { }).with("TEST");
        
        assertTrue(action.destiny() == 6000);
        assertTrue(action.getArgument(String.class).equals("TEST"));
        assertTrue(action.getCount() == 1);
        assertTrue(action.getChronometer() == null);
        assertFalse(action.isCancelled());
        assertFalse(action.isRunning());
        assertFalse(action.isCompleted());
        assertFalse(action.isRecurrent());
        
        action.cancel();
        assertTrue(action.isCancelled());
        assertFalse(action.isCompleted());
    }
    
    @Test
    public void testParametersInRecurrent(){
        FutureAction action = FutureAction.in(6, SECONDS, () -> { })
                .after(5, SECONDS)
                .with("TEST")
                .setCount(4);
        
        assertTrue(action.destiny() == 5000);
        assertTrue(action.getArgument(String.class).equals("TEST"));
        assertTrue(action.getCount() == 4);
        assertTrue(action.getChronometer() == null);
        assertFalse(action.isCancelled());
        assertFalse(action.isRunning());
        assertFalse(action.isCompleted());
        assertTrue(action.isRecurrent());
        
        action.cancel();
        assertTrue(action.isCancelled());
        assertFalse(action.isCompleted());
        assertTrue(action.isTerminated());
    }
    
    @Test
    public void testParametersEveryRecurrent(){
        FutureAction action = FutureAction.every(2, SECONDS, () -> { })
                .after(5, SECONDS)
                .with("TEST")
                .setCount(12);
        
        assertTrue(action.destiny() == 5000);
        assertTrue(action.getArgument(String.class).equals("TEST"));
        assertTrue(action.getCount() == 12);
        assertTrue(action.getChronometer() == null);
        assertFalse(action.isCancelled());
        assertFalse(action.isRunning());
        assertFalse(action.isCompleted());
        assertTrue(action.isRecurrent());
        
        action.cancel();
        assertTrue(action.isCancelled());
        assertFalse(action.isCompleted());
        assertTrue(action.isTerminated());
    }
    
    @Test
    public void testParametersAtInstant(){
        FutureAction action = FutureAction.at(Instant.now().plusSeconds(5), () -> { })
                .with("TEST")
                .setCount(12);
        
        assertTrue(action.getArgument(String.class).equals("TEST"));
        assertTrue(action.getCount() == 12);
        assertTrue(action.getChronometer() == null);
        assertFalse(action.isCancelled());
        assertFalse(action.isRunning());
        assertFalse(action.isCompleted());
        assertTrue(action.isRecurrent());
        
        action.cancel();
        assertTrue(action.isCancelled());
        assertFalse(action.isCompleted());
        assertTrue(action.isTerminated());
    }
    
    @Test
    public void testParametersAtZonedDateTime(){
        FutureAction action = FutureAction.at(ZonedDateTime.now().plusSeconds(5), () -> { })
                .with("TEST")
                .setCount(12);
        
        assertTrue(action.getArgument(String.class).equals("TEST"));
        assertTrue(action.getCount() == 12);
        assertTrue(action.getChronometer() == null);
        assertFalse(action.isCancelled());
        assertFalse(action.isRunning());
        assertFalse(action.isCompleted());
        assertTrue(action.isRecurrent());
        
        action.cancel();
        assertTrue(action.isCancelled());
        assertFalse(action.isCompleted());
        assertTrue(action.isTerminated());
    }
    
    @Test
    public void testParametersAtLocalTime(){
        FutureAction action = FutureAction.at(LocalTime.now().plusSeconds(5), () -> { })
                .with("TEST")
                .setCount(12);
        
        assertTrue(action.getArgument(String.class).equals("TEST"));
        assertTrue(action.getCount() == 12);
        assertTrue(action.getChronometer() == null);
        assertFalse(action.isCancelled());
        assertFalse(action.isRunning());
        assertFalse(action.isCompleted());
        assertTrue(action.isRecurrent());
        
        action.cancel();
        assertTrue(action.isCancelled());
        assertFalse(action.isCompleted());
        assertTrue(action.isTerminated());
    }
    
    @Test
    public void testParametersAlwaysAtLocalTime(){
        FutureAction action = FutureAction.alwaysAt(LocalTime.now().plusSeconds(5), () -> { })
                .with("TEST");
        
        assertTrue(action.getArgument(String.class).equals("TEST"));
        assertTrue(action.getCount() == FutureAction.INDEFINITE);
        assertTrue(action.getChronometer() == null);
        assertFalse(action.isCancelled());
        assertFalse(action.isRunning());
        assertFalse(action.isCompleted());
        assertTrue(action.isRecurrent());
        assertTrue(action.interval() == Duration.ofDays(1).toMillis());
        
        action.cancel();
        assertTrue(action.isCancelled());
        assertFalse(action.isCompleted());
        assertTrue(action.isTerminated());
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testConstructorIllegalCount(){
        new FutureAction(2000, -1){
            @Override
            public void run(Action action){ }
        };
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testConstructorIllegalTime(){
        new FutureAction(-500, 1){
            @Override
            public void run(Action action){ }
        };
    }

}
