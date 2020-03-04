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

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.raven.common.struct.ObservableItem.Change;

/**
 * Tests for the ObservableItem implementation.
 *
 */
public class ObservableItemTest {

    ObservableItem<String> i1;
    ObservableItem<Double> i2;

    @BeforeClass
    public static void setUpBeforeClass(){ }

    @AfterClass
    public static void tearDownAfterClass(){ }

    @Before
    public void setUp(){
        i1 = new ObservableItem<>("key1", "value1");
        i2 = new ObservableItem<>("key2", 123.456);
    }

    @After
    public void tearDown(){ }

    @Test
    public void testGetKey(){
        assertTrue(i1.getKey().equals("key1"));
        assertTrue(i2.getKey().equals("key2"));
    }

    @Test
    public void testSetKey(){
        i1.setKey("ABCD");
        assertTrue(i1.getKey().equals("ABCD"));
        i2.setKey(null);
        assertTrue(i2.getKey() == null);
    }

    @Test
    public void testSetKeyObserved(){
        final AtomicBoolean wasTriggered = new AtomicBoolean(false);
        i1.setListener((change) -> {
            wasTriggered.set(true);
            assertTrue(change.getEvent() == Change.Event.KEY);
            assertTrue(change.getNewKey().equals("ABCD"));
            assertTrue(change.getOldKey().equals("key1"));
            assertTrue(change.getNewValue().equals("value1"));
            assertTrue(change.getOldValue().equals("value1"));
        });
        i1.setKey("ABCD");
        assertTrue(i1.getKey().equals("ABCD"));
        assertTrue(wasTriggered.get());

        wasTriggered.set(false);
        i2.setListener((change) -> {
            wasTriggered.set(true);
            assertTrue(change.getEvent() == Change.Event.KEY);
            assertTrue(change.getNewKey() == null);
            assertTrue(change.getOldKey().equals("key2"));
            assertTrue(change.getNewValue().equals(123.456));
            assertTrue(change.getOldValue().equals(123.456));
        });
        i2.setKey(null);
        assertTrue(i2.getKey() == null);
        assertTrue(wasTriggered.get());
    }

    @Test
    public void testHasKey(){
        assertTrue(i1.hasKey());
        assertTrue(i2.hasKey());
        i1.setKey(null);
        assertFalse(i1.hasKey());
    }

    @Test
    public void testGetValue(){
        assertTrue(i1.getValue().equals("value1"));
        assertTrue(i2.getValue().equals(123.456));
    }

    @Test
    public void testSetValue(){
        i1.setValue("AAAABBBB");
        assertTrue(i1.getValue().equals("AAAABBBB"));
        i2.setValue(2.0);
        assertTrue(i2.getValue().equals(2.0));
        i1.setValue(null);
        assertTrue(i1.getValue() == null);
    }

    @Test
    public void testSetValueObserved(){
        final AtomicBoolean wasTriggered = new AtomicBoolean(false);
        i1.setListener((change) -> {
            wasTriggered.set(true);
            assertTrue(change.getEvent() == Change.Event.VALUE);
            assertTrue(change.getNewKey().equals("key1"));
            assertTrue(change.getOldKey().equals("key1"));
            assertTrue(change.getNewValue().equals("AAAABBBB"));
            assertTrue(change.getOldValue().equals("value1"));
        });
        i1.setValue("AAAABBBB");
        assertTrue(i1.getValue().equals("AAAABBBB"));
        assertTrue(wasTriggered.get());

        wasTriggered.set(false);
        i2.setListener((change) -> {
            wasTriggered.set(true);
            assertTrue(change.getEvent() == Change.Event.VALUE);
            assertTrue(change.getNewKey().equals("key2"));
            assertTrue(change.getOldKey().equals("key2"));
            assertTrue(change.getNewValue() == 2.0);
            assertTrue(change.getOldValue() == 123.456);
        });
        i2.setValue(2.0);
        assertTrue(i2.getValue().equals(2.0));
        assertTrue(wasTriggered.get());

        wasTriggered.set(false);
        i1.setListener((change) -> {
            wasTriggered.set(true);
            assertTrue(change.getEvent() == Change.Event.VALUE);
            assertTrue(change.getNewKey().equals("key1"));
            assertTrue(change.getOldKey().equals("key1"));
            assertTrue(change.getNewValue() == null);
            assertTrue(change.getOldValue().equals("AAAABBBB"));
        });
        i1.setValue(null);
        assertTrue(i1.getValue() == null);
        assertTrue(wasTriggered.get());
    }

    @Test
    public void testHasValue(){
        assertTrue(i1.hasValue());
        assertTrue(i2.hasValue());
        i1.setValue(null);
        i2.setValue(null);
        assertFalse(i1.hasValue());
        assertFalse(i2.hasValue());
    }

    @Test
    public void testIsEmpty(){
        assertFalse(i1.isEmpty());
        i1.setKey(null);
        i1.setValue(null);
        assertTrue(i1.isEmpty());
    }

    @Test
    public void testEqualsObject(){
        ObservableItem<String> item = new ObservableItem<>("key", "value1");
        assertFalse(item.equals(i1));
        item.setKey("key1");
        assertTrue(item.equals(i1));
    }

}
