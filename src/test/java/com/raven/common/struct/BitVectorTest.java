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

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Tests for the BitVector implementation.
 *
 */
public class BitVectorTest {
    
    BitVector vecTiny = BitVector.valueOf("10");
    final int vecTinySize = 2;
    
    BitVector vecSmall = BitVector.valueOf("0011");
    final int vecSmallSize = 4;
    
    //                                       0       8       16
    BitVector vecMedium = BitVector.valueOf("00110101110001010110");
    final int vecMediumSize = 20;
    
    //                                      0       8       16      24      32
    BitVector vecLarge = BitVector.valueOf("11010010111100010111000010111100"
                                         + "10010101000110100100011110001000"
                                         + "01111101101110000010001000111111"
                                         + "00101000101101001000100100100100"
                                         + "11000011101010101010101010110101"
                                         + "11010011011011010100001000101101"
                                         + "01111010101110001001000000110100");
    
    final int vecLargeSize = 224;

    @BeforeClass
    public static void setUpBeforeClass(){ }

    @AfterClass
    public static void tearDownAfterClass(){ }

    @Before
    public void setUp() throws Exception{ }

    @After
    public void tearDown() throws Exception{ }

    @Test
    public void testBitVector(){
        BitVector vec = new BitVector();
        assertNotNull("Internal array of bytes must not be null",
                vec.asArray());
    }

    @Test
    public void testBitVectorInitialLength(){
        BitVector vec = new BitVector(260);
        assertNotNull("Internal array of bytes must not be null",
                vec.asArray());
        
        assertTrue("Internal array of bytes length must be 33",
                vec.asArray().length == 33);
        
        assertTrue("BitVector size must be 0",
                vec.size() == 0);
        
        assertTrue("BitVector capacity must be 264",
                vec.capacity() == 264);
    }
    
    @Test
    public void testBitVectorCopyConstr(){
        BitVector copy = new BitVector(vecTiny);
        assertEquals("BitVectors should be equal", vecTiny, copy);
        assertTrue("Hash code of BitVectors should be equal",
                vecTiny.hashCode() == copy.hashCode());
        
        copy = new BitVector(vecSmall);
        assertEquals("BitVectors should be equal", vecSmall, copy);
        assertTrue("Hash code of BitVectors should be equal",
                vecSmall.hashCode() == copy.hashCode());
        
        copy = new BitVector(vecMedium);
        assertEquals("BitVectors should be equal", vecMedium, copy);
        assertTrue("Hash code of BitVectors should be equal",
                vecMedium.hashCode() == copy.hashCode());
        
        copy = new BitVector(vecLarge);
        assertEquals("BitVectors should be equal", vecLarge, copy);
        assertTrue("Hash code of BitVectors should be equal",
                vecLarge.hashCode() == copy.hashCode());
    }

    @Test
    public void testGet(){
        assertTrue(vecTiny.get(0));
        assertFalse(vecTiny.get(1));
        assertFalse(vecSmall.get(1));
        assertTrue(vecSmall.get(2));
        assertTrue(vecMedium.get(13));
        assertFalse(vecMedium.get(16));
        assertTrue(vecLarge.get(3));
        assertFalse(vecLarge.get(34));
    }

    @Test
    public void testGetSubvector(){
        BitVector vec = vecLarge.get(0, 3);
        assertTrue(vec.toString().equals("110"));
        assertTrue(vec.size() == 3);
        assertTrue(vec.capacity() == 8);
        
        vec = vecLarge.get(0, 20);
        assertTrue(vec.toString().equals("11010010111100010111"));
        assertTrue(vec.size() == 20);
        assertTrue(vec.capacity() == 24);
        
        vec = vecLarge.get(112, 126);
        assertTrue(vec.toString().equals("10001001001001"));
        assertTrue(vec.size() == 14);
        assertTrue(vec.capacity() == 16);
    }
    
    @Test
    public void testSet(){
        vecTiny.set(0, Bit._0);
        assertFalse(vecTiny.get(0));
        vecTiny.set(1, Bit._1);
        assertTrue(vecTiny.get(1));
        
        vecLarge.set(8, Bit._0);
        assertFalse(vecLarge.get(8));
        vecLarge.set(13, Bit._0);
        assertFalse(vecLarge.get(13));
        vecLarge.set(13, Bit._1);
        assertTrue(vecLarge.get(13));
        vecLarge.set(37, Bit._0);
        assertFalse(vecLarge.get(37));
    }
    
    @Test
    public void testSetOutOfRange(){
        vecSmall.set(6, Bit._1);
        vecSmall.set(7, Bit._1);
        vecSmall.set(12, Bit._1);
        vecSmall.set(14, Bit._1);
        vecSmall.set(18, Bit._0);
        vecSmall.set(20, Bit._1);
        assertTrue(vecSmall.toString().equals("001100110000101000001"));
    }

    @Test
    public void testSetRange(){
        vecMedium.set(4, 12, Bit._0);
        assertTrue(vecMedium.toString().equals("00110000000001010110"));
        assertTrue(vecMedium.size() == 20);
        vecMedium.set(16, 30, Bit._1);
        assertTrue(vecMedium.toString().equals("001100000000010111111111111111"));
        assertTrue(vecMedium.size() == 30);
    }
    
    @Test
    public void testSetRangeFromVector(){
        vecMedium.set(2, vecSmall);
        assertTrue(vecMedium.toString().equals("00001101110001010110"));
        vecMedium.set(6, vecSmall);
        assertTrue(vecMedium.toString().equals("00001100110001010110"));
        vecMedium.set(15, BitVector.valueOf("111110100"));
        assertTrue(vecMedium.toString().equals("000011001100010111110100"));
        assertTrue(vecMedium.size() == 24);
        vecMedium.set(18, BitVector.valueOf("000010100011"));                             
        assertTrue(vecMedium.toString().equals("000011001100010111000010100011"));
        assertTrue(vecMedium.size() == 30);
        assertTrue(vecMedium.capacity() == 32);
        vecMedium.set(29, vecSmall);
        assertTrue(vecMedium.toString().equals("000011001100010111000010100010011"));
        assertTrue(vecMedium.size() == 33);
        assertTrue(vecMedium.capacity() == 40);
    }
    
    @Test
    public void testAdd(){
        vecTiny.add(Bit._0);
        vecTiny.add(Bit._1);
        vecTiny.add(Bit._0);
        assertTrue(vecTiny.toString().equals("10010"));
        assertTrue(vecTiny.size() == vecTinySize + 3);
        
        vecSmall.add(Bit._1);
        vecSmall.add(Bit._0);
        vecSmall.add(Bit._1);
        assertTrue(vecSmall.toString().equals("0011101"));
        assertTrue(vecSmall.size() == vecSmallSize + 3);
        
        vecMedium.add(Bit._0);
        vecMedium.add(Bit._1);
        vecMedium.add(Bit._1);
        assertTrue(vecMedium.toString().equals("00110101110001010110011"));
        assertTrue(vecMedium.size() == vecMediumSize + 3);
    }
    
    @Test
    public void testAddBitVector(){
        vecTiny.add(vecSmall);
        assertTrue(vecTiny.toString().equals("100011"));
        assertTrue(vecTiny.size() == (vecTinySize + vecSmallSize));
        assertTrue(vecTiny.capacity() == 8);
        
        vecSmall.add(vecMedium);
        assertTrue(vecSmall.toString().equals("001100110101110001010110"));
        assertTrue(vecSmall.size() == (vecSmallSize + vecMediumSize));
        assertTrue(vecSmall.capacity() == 32);
    }

    @Test
    public void testInsertAt(){
        vecTiny.insertAt(0, Bit._1);
        assertTrue(vecTiny.toString().equals("110"));
        assertTrue(vecTiny.size() == vecTinySize + 1);
        assertTrue(vecTiny.capacity() == 8);
        vecTiny.insertAt(0, Bit._0);
        assertTrue(vecTiny.toString().equals("0110"));
        assertTrue(vecTiny.size() == vecTinySize + 2);
        assertTrue(vecTiny.capacity() == 8);
        
        vecMedium.insertAt(4, Bit._1);
        assertTrue(vecMedium.toString().equals("001110101110001010110"));
        assertTrue(vecMedium.size() == vecMediumSize + 1);
        assertTrue(vecMedium.capacity() == 24);
        vecMedium.insertAt(10, Bit._0);
        assertTrue(vecMedium.toString().equals("0011101011010001010110"));
        assertTrue(vecMedium.size() == vecMediumSize + 2);
        assertTrue(vecMedium.capacity() == 24);
        vecMedium.insertAt(12, Bit._1);
        assertTrue(vecMedium.toString().equals("00111010110110001010110"));
        assertTrue(vecMedium.size() == vecMediumSize + 3);
        assertTrue(vecMedium.capacity() == 24);
    }
    
    @Test
    public void testInsertBitVectorAt(){
        vecTiny.insertAt(1, vecSmall);
        assertTrue(vecTiny.toString().equals("100110"));
        assertTrue(vecTiny.size() == (vecTinySize + vecSmallSize));
        assertTrue(vecTiny.capacity() == 8);
        
        vecSmall.insertAt(2, vecMedium);
        assertTrue(vecSmall.toString().equals("000011010111000101011011"));
        assertTrue(vecSmall.size() == (vecSmallSize + vecMediumSize));
        assertTrue(vecSmall.capacity() == 32);
    }
    
    @Test
    public void testRemove(){
        vecTiny.remove(0);
        assertFalse(vecTiny.get(0));
        assertTrue(vecTiny.equals(BitVector.valueOf("0")));
        vecSmall.remove(1);
        assertTrue(vecSmall.get(1));
        assertTrue(vecSmall.size() == 3);
        vecSmall.remove(0);
        assertTrue(vecSmall.get(0));
        assertTrue(vecSmall.size() == 2);
        assertTrue(vecSmall.equals(BitVector.valueOf("11")));
        vecMedium.remove(11);
        assertTrue(vecMedium.size() == 19);
        vecMedium.remove(2);
        vecMedium.remove(8);
        vecMedium.remove(7);
        vecMedium.remove(15);
        vecMedium.remove(4);
        vecMedium.remove(0);
        vecMedium.remove(12);
        assertTrue(vecMedium.size() == 12);
        assertTrue(vecMedium.equals(BitVector.valueOf("010010010101")));
        BitVector vec = BitVector.valueOf("0111001011111011");
        vec.remove(15);
        assertTrue(vec.size() == 15);
        assertTrue(vec.equals(BitVector.valueOf("011100101111101")));
    }

    @Test
    public void testRemoveRange(){
        BitVector copy = vecMedium.clone();
        copy.remove(0, 8);
        assertTrue(copy.size() == 12);
        assertTrue(copy.equals(BitVector.valueOf("110001010110")));
        copy = vecMedium.clone();
        copy.remove(4, 15);
        assertTrue(copy.size() == 9);
        assertTrue(copy.equals(BitVector.valueOf("001110110")));
        copy = vecMedium.clone();
        copy.remove(17, 19);
        assertTrue(copy.size() == 18);
        assertTrue(copy.equals(BitVector.valueOf("001101011100010100")));
        copy = vecMedium.clone();
        copy.remove(15, 20);
        assertTrue(copy.size() == 15);
        assertTrue(copy.equals(BitVector.valueOf("001101011100010")));
        copy = vecLarge.clone();
        copy.remove(9, vecLargeSize);
        assertTrue(copy.size() == 9);
        assertTrue(copy.equals(BitVector.valueOf("110100101")));
        copy = vecLarge.clone();
        copy.remove(0, vecLargeSize - 7);
        assertTrue(copy.size() == 7);
        assertTrue(copy.equals(BitVector.valueOf("0110100")));
    }
    
    @Test
    public void testClear(){
        vecTiny.clear();
        assertTrue(vecTiny.toString().equals("00"));
        assertTrue(vecTiny.size() == vecTinySize);
        assertTrue(vecTiny.capacity() == 8);
        
        vecSmall.clear();
        assertTrue(vecSmall.toString().equals("0000"));
        assertTrue(vecSmall.size() == vecSmallSize);
        assertTrue(vecSmall.capacity() == 8);
        
        vecMedium.clear();
        for(byte b : vecMedium.asArray()){
            if(b != 0){
                fail("Content after clear is not zero");
            }
        }
        assertTrue(vecMedium.size() == vecMediumSize);
        assertTrue(vecMedium.capacity() == 24);
        
        vecLarge.clear();
        for(byte b : vecLarge.asArray()){
            if(b != 0){
                fail("Content after clear is not zero");
            }
        }
        assertTrue(vecLarge.size() == vecLargeSize);
        assertTrue(vecLarge.capacity() == 224);
    }

    @Test
    public void testFlip(){
        vecTiny.flip(0);
        vecTiny.flip(1);
        assertFalse(vecTiny.get(0));
        assertTrue(vecTiny.get(1));
        
        vecMedium.flip(7);
        vecMedium.flip(8);
        vecMedium.flip(9);
        vecMedium.flip(12);
        assertFalse(vecMedium.get(7));
        assertFalse(vecMedium.get(8));
        assertFalse(vecMedium.get(9));
        assertTrue(vecMedium.get(12));
        
        vecLarge.flip(8);
        vecLarge.flip(32);
        vecLarge.flip(65);
        vecLarge.flip(66);
        assertFalse(vecLarge.get(8));
        assertFalse(vecLarge.get(32));
        assertFalse(vecLarge.get(65));
        assertFalse(vecLarge.get(66));
        vecLarge.flip(66);
        assertTrue(vecLarge.get(66));
    }

    @Test
    public void testFlipRange(){
        vecTiny.flip(0, 2);
        assertFalse(vecTiny.get(0));
        assertTrue(vecTiny.get(1));
        
        vecSmall.flip(1, 3);
        assertFalse(vecSmall.get(0));
        assertTrue(vecSmall.get(1));
        assertFalse(vecSmall.get(2));
        assertTrue(vecSmall.get(3));
        
        vecMedium.flip(6, 17);
        assertTrue(vecMedium.toString().equals("00110110001110101110"));
    }

    @Test
    public void testNextSetBit(){
        assertTrue(vecTiny.nextSetBit(1) == -1);
        assertTrue(vecSmall.nextSetBit(0) == 2);
        assertTrue(vecMedium.nextSetBit(6) == 7);
        assertTrue(vecMedium.nextSetBit(10) == 13);
        assertTrue(vecLarge.nextSetBit(32) == 32);
        assertTrue(vecLarge.nextSetBit(40) == 43);
    }

    @Test
    public void testNextUnsetBit(){
        assertTrue(vecTiny.nextUnsetBit(0) == 1);
        assertTrue(vecSmall.nextUnsetBit(0) == 0);
        assertTrue(vecSmall.nextUnsetBit(2) == -1);
        assertTrue(vecMedium.nextUnsetBit(7) == 10);
        assertTrue(vecMedium.nextUnsetBit(15) == 16);
        assertTrue(vecLarge.nextUnsetBit(34) == 34);
        assertTrue(vecLarge.nextUnsetBit(53) == 57);
    }
    
    @Test
    public void testPreviousSetBit(){
        assertTrue(vecTiny.previousSetBit(0) == 0);
        assertTrue(vecTiny.previousSetBit(1) == 0);
        assertTrue(vecSmall.previousSetBit(2) == 2);
        assertTrue(vecMedium.previousSetBit(12) == 9);
        assertTrue(vecMedium.previousSetBit(19) == 18);
        assertTrue(vecLarge.previousSetBit(31) == 29);
        assertTrue(vecLarge.previousSetBit(23) == 19);
    }
    
    @Test
    public void testPreviousUnsetBit(){
        assertTrue(vecTiny.previousUnsetBit(0) == -1);
        assertTrue(vecSmall.previousUnsetBit(3) == 1);
        assertTrue(vecMedium.previousUnsetBit(8) == 6);
        assertTrue(vecMedium.previousUnsetBit(9) == 6);
        assertTrue(vecLarge.previousUnsetBit(29) == 25);
        assertTrue(vecLarge.previousUnsetBit(56) == 52);
    }
    
    @Test
    public void testSize(){
        vecSmall.add(Bit._0);
        vecSmall.add(Bit._0);
        assertTrue(vecSmall.size() == (vecSmallSize + 2));
        vecSmall.add(Bit._1);
        assertTrue(vecSmall.size() == (vecSmallSize + 3));
        vecSmall.add(Bit._1);
        assertTrue(vecSmall.size() == (vecSmallSize + 4));
        vecSmall.set(1, Bit._0);
        vecSmall.set(3, Bit._1);
        assertTrue(vecSmall.size() == (vecSmallSize + 4));
        vecSmall.insertAt(0, Bit._1);
        vecSmall.insertAt(1, Bit._0);
        vecSmall.insertAt(5, Bit._1);
        assertTrue(vecSmall.size() == (vecSmallSize + 7));
        vecSmall.and(3, Bit._1);
        vecSmall.or(4, Bit._1);
        vecSmall.xor(5, Bit._1);
        assertTrue(vecSmall.size() == (vecSmallSize + 7));
        vecSmall.shiftLeft(3);
        assertTrue(vecSmall.size() == (vecSmallSize + 7));
        vecSmall.shiftRight(3);
        assertTrue(vecSmall.size() == (vecSmallSize + 7));
        vecSmall.clear();
        assertTrue(vecSmall.size() == (vecSmallSize + 7));
    }
    
    @Test
    public void testCapacity(){
        vecSmall.add(Bit._0);
        vecSmall.add(Bit._0);
        assertTrue(vecSmall.capacity() == 8);
        vecSmall.add(Bit._1);
        vecSmall.add(Bit._1);
        assertTrue(vecSmall.capacity() == 8);
        vecSmall.set(1, Bit._0);
        assertTrue(vecSmall.capacity() == 8);
        vecSmall.set(3, Bit._1);
        assertTrue(vecSmall.capacity() == 8);
        vecSmall.insertAt(0, Bit._1);
        vecSmall.insertAt(1, Bit._0);
        vecSmall.insertAt(5, Bit._1);
        assertTrue(vecSmall.capacity() == 16);
        vecSmall.and(3, Bit._1);
        vecSmall.or(4, Bit._1);
        vecSmall.xor(5, Bit._1);
        assertTrue(vecSmall.capacity() == 16);
        vecSmall.shiftLeft(3);
        assertTrue(vecSmall.capacity() == 16);
        vecSmall.shiftRight(3);
        assertTrue(vecSmall.capacity() == 16);
        vecSmall.clear();
        assertTrue(vecSmall.capacity() == 16);
        for(int i=0; i<6; ++i){
            vecSmall.add(Bit._1);
        }
        assertTrue(vecSmall.capacity() == 32);
    }
    
    @Test
    public void testCountBitsSet(){
        assertTrue(vecTiny.bitsSet() == 1);
        assertTrue(vecSmall.bitsSet() == 2);
        assertTrue(vecMedium.bitsSet() == 10);
        vecMedium.add(Bit._0);
        assertTrue(vecMedium.bitsSet() == 10);
        vecMedium.add(Bit._1);
        assertTrue(vecMedium.bitsSet() == 11);
        vecMedium.set(1, Bit._1);
        assertTrue(vecMedium.bitsSet() == 12);
        vecMedium.insertAt(16, Bit._0);
        assertTrue(vecMedium.bitsSet() == 12);
        vecMedium.insertAt(16, Bit._1);
        assertTrue(vecMedium.bitsSet() == 13);
        vecMedium.and(0, Bit._1);
        assertTrue(vecMedium.bitsSet() == 13);
        vecMedium.or(0, Bit._1);
        assertTrue(vecMedium.bitsSet() == 14);
        vecMedium.xor(0, Bit._1);
        assertTrue(vecMedium.bitsSet() == 13);
        assertTrue(vecLarge.bitsSet() == 106);
    }
    
    @Test
    public void testCountBitsUnset(){
        assertTrue(vecTiny.bitsUnset() == 1);
        assertTrue(vecSmall.bitsUnset() == 2);
        assertTrue(vecMedium.bitsUnset() == 10);
        vecMedium.add(Bit._0);
        assertTrue(vecMedium.bitsUnset() == 11);
        vecMedium.add(Bit._1);
        assertTrue(vecMedium.bitsUnset() == 11);
        vecMedium.set(1, Bit._1);
        assertTrue(vecMedium.bitsUnset() == 10);
        vecMedium.insertAt(16, Bit._0);
        assertTrue(vecMedium.bitsUnset() == 11);
        vecMedium.insertAt(16, Bit._1);
        assertTrue(vecMedium.bitsUnset() == 11);
        vecMedium.and(0, Bit._0);
        assertTrue(vecMedium.bitsUnset() == 11);
        vecMedium.or(0, Bit._1);
        assertTrue(vecMedium.bitsUnset() == 10);
        vecMedium.xor(0, Bit._1);
        assertTrue(vecMedium.bitsUnset() == 11);
        assertTrue(vecLarge.bitsUnset() == (vecLargeSize - 106));
    }
    
    @Test
    public void testShiftLeft(){
        vecTiny.shiftLeft(2);
        assertTrue(vecTiny.toString().equals("00"));
        vecSmall.shiftLeft(3);
        assertTrue(vecSmall.toString().equals("1000"));
        vecMedium.shiftLeft(5);
        assertTrue(vecMedium.toString().equals("10111000101011000000"));
        vecMedium.shiftLeft(12);
        assertTrue(vecMedium.toString().equals("11000000000000000000"));
        vecLarge.shiftLeft(34);
        assertTrue(vecLarge.toString().equals(
                  "010101000110100100011110001000"
                + "01111101101110000010001000111111"
                + "00101000101101001000100100100100"
                + "11000011101010101010101010110101"
                + "11010011011011010100001000101101"
                + "01111010101110001001000000110100"
                + "0000000000000000000000000000000000"));
        
    }

    @Test
    public void testShiftRight(){
        vecTiny.shiftRight(2);
        assertTrue(vecTiny.toString().equals("00"));
        vecSmall.shiftRight(1);
        assertTrue(vecSmall.toString().equals("0001"));
        vecMedium.shiftRight(5);
        assertTrue(vecMedium.toString().equals("00000001101011100010"));
        vecMedium.shiftRight(12);
        assertTrue(vecMedium.toString().equals("00000000000000000001"));
        vecLarge.shiftRight(34);
        assertTrue(vecLarge.toString().equals(
                  "0000000000000000000000000000000000"
                + "11010010111100010111000010111100"
                + "10010101000110100100011110001000"
                + "01111101101110000010001000111111"
                + "00101000101101001000100100100100"
                + "11000011101010101010101010110101"
                + "110100110110110101000010001011"));
    }
    
    @Test
    public void testRotateLeft(){
        vecSmall.rotateLeft(1);
        assertTrue(vecSmall.toString().equals("0110"));
        vecSmall.rotateLeft(7);
        assertTrue(vecSmall.toString().equals("0011"));
        vecMedium.rotateLeft(5);
        assertTrue(vecMedium.toString().equals("10111000101011000110"));
        vecMedium.rotateLeft(12);
        assertTrue(vecMedium.toString().equals("11000110101110001010"));
        vecMedium.rotateLeft(3);
        assertTrue(vecMedium.toString().equals("00110101110001010110"));
        vecMedium.rotateLeft(45);
        assertTrue(vecMedium.toString().equals("10111000101011000110"));
    }
    
    @Test
    public void testRotateRight(){
        vecSmall.rotateRight(1);
        assertTrue(vecSmall.toString().equals("1001"));
        vecSmall.rotateRight(7);
        assertTrue(vecSmall.toString().equals("0011"));
        vecMedium.rotateRight(5);
        assertTrue(vecMedium.toString().equals("10110001101011100010"));
        vecMedium.rotateRight(12);
        assertTrue(vecMedium.toString().equals("10101110001010110001"));
        vecMedium.rotateRight(3);
        assertTrue(vecMedium.toString().equals("00110101110001010110"));
        vecMedium.rotateRight(45);
        assertTrue(vecMedium.toString().equals("10110001101011100010"));
    }

    @Test
    public void testLogicAnd(){
        vecMedium.and(1,Bit._0);
        assertFalse(vecMedium.get(1));
        vecMedium.and(2,Bit._1);
        assertTrue(vecMedium.get(2));
    }

    @Test
    public void testLogicAndBitVector(){
        vecSmall.and(BitVector.valueOf("1001"));
        assertTrue(vecSmall.toString().equals("0001"));
    }

    @Test
    public void testLogicOr(){
        vecMedium.or(1,Bit._0);
        assertFalse(vecMedium.get(1));
        vecMedium.or(2,Bit._0);
        assertTrue(vecMedium.get(2));
        vecMedium.or(2,Bit._1);
        assertTrue(vecMedium.get(2));
    }

    @Test
    public void testLogicOrBitVector(){
        vecSmall.or(BitVector.valueOf("0110"));
        assertTrue(vecSmall.toString().equals("0111"));
    }

    @Test
    public void testLogicXor(){
        vecMedium.xor(1,Bit._0);
        assertFalse(vecMedium.get(1));
        vecMedium.xor(2,Bit._1);
        assertFalse(vecMedium.get(2));
        vecMedium.xor(2,Bit._0);
        assertFalse(vecMedium.get(2));
    }

    @Test
    public void testLogicXorBitVector(){
        vecSmall.xor(BitVector.valueOf("0110"));
        assertTrue(vecSmall.toString().equals("0101"));
    }
    
    @Test
    public void testAsByte(){
        byte b = vecTiny.asByte();
        assertTrue((0xff & b) == 0x80);
        b = vecSmall.asByte();
        assertTrue(b == 0x30);
        b = vecMedium.asByte();
        assertTrue(b == 0x35);
        b = vecLarge.asByte();
        assertTrue(b == -46);
    }
    
    @Test
    public void testAsShort(){
        short s = vecTiny.asShort();
        assertTrue((0xffff & s) == 0x8000);
        s = vecSmall.asShort();
        assertTrue(s == 0x3000);
        s = vecMedium.asShort();
        assertTrue(s == 0x35c5);
        s = vecLarge.asShort();
        assertTrue(s == -11535);
    }
    
    @Test
    public void testAsInt(){
        int i = vecTiny.asInt();
        assertTrue(i == 0x80000000);
        i = vecSmall.asInt();
        assertTrue(i == 0x30000000);
        i = vecMedium.asInt();
        assertTrue(i == 0x35c56000);
        i = vecLarge.asInt();
        assertTrue(i == -755928900);
    }
    
    @Test
    public void testAsLong(){
        long l = vecTiny.asLong();
        assertTrue(l == 0x8000000000000000L);
        l = vecSmall.asLong();
        assertTrue(l == 0x3000000000000000L);
        l = vecMedium.asLong();
        assertTrue(l == 0x35c5600000000000L);
        l = vecLarge.asLong();
        assertTrue(l == -3246689901099726968L);
    }
    
    @Test
    public void testAsFloat(){
        BitVector vec = BitVector.valueOf("00111110001000000000000000000000");
        assertTrue(vec.asfloat() == 0.15625);
    }
    
    @Test
    public void testAsDouble(){
        BitVector vec 
          = BitVector.valueOf("1011111111000001100000001001000000001000010010110101001100010000");
        
        assertTrue(vec.asDouble() == -0.13673592);
        vec = BitVector.valueOf("0111111111110000000000000000000000000000000000000000000000000000");
        assertTrue(vec.asDouble() == Double.POSITIVE_INFINITY);
        vec = BitVector.valueOf("0100000000010100000000000000000000000000000000000000000000000000");
        assertTrue(vec.asDouble() == 5.0);
    }
    
    @Test
    public void testToArray(){
        byte[] b = vecTiny.toArray();
        assertArrayEquals(new byte[]{-128}, b);
        b = vecSmall.toArray();
        assertArrayEquals(new byte[]{0x30}, b);
        b = vecMedium.toArray();
        assertArrayEquals(new byte[]{
                (byte)53, (byte) -59, (byte)96}, b);
    }
    
    @Test
    public void testClone(){
        BitVector vec = vecTiny.clone();
        assertTrue(vec.toString().equals("10"));
        vec = vecSmall.clone();
        assertTrue(vec.toString().equals("0011"));
        vec = vecMedium.clone();
        assertTrue(vec.toString().equals("00110101110001010110"));
    }
    
    @Test
    public void testToString(){
        assertTrue(vecTiny.toString().equals("10"));
        assertTrue(vecSmall.toString().equals("0011"));
        assertTrue(vecMedium.toString().equals("00110101110001010110"));
    }
    
    @Test
    public void testToFormattedString(){
        assertTrue(vecTiny.toFormattedString().equals("10"));
        assertTrue(vecSmall.toFormattedString().equals("0011"));
        assertTrue(vecMedium.toFormattedString().equals("00110101 11000101 0110"));
    }
    
    @Test
    public void testToHexString(){
        assertTrue(vecTiny.toHexString().equals("8"));
        assertTrue(vecSmall.toHexString().equals("3"));
        assertTrue(vecMedium.toHexString().equals("35c56"));
        BitVector vec = BitVector.valueOf("110100111101000111011");
        assertTrue(vec.toHexString().equals("d3d1d8"));
    }

    @Test
    public void testEqualsObject(){
        assertTrue(vecTiny.equals(BitVector.valueOf("10")));
        assertTrue(vecSmall.equals(BitVector.valueOf("0011")));
        assertTrue(vecMedium.equals(BitVector.valueOf("00110101110001010110")));
    }

    @Test
    public void testWrap(){
        BitVector vec = BitVector.wrap(new byte[]{1,2,3});
        assertTrue(vec.toString().equals("000000010000001000000011"));
        assertTrue(vec.size() == 24);
        assertTrue(vec.capacity() == 24);
    }

    @Test
    public void testValueOfString(){
        BitVector vec = BitVector.valueOf("110100111101000111011");
        assertTrue(vec.toString().equals("110100111101000111011"));
        assertTrue(vec.size() == 21);
        assertTrue(vec.capacity() == 24);
    }
    
    @Test
    public void testValueOfBytes(){
        BitVector vec = BitVector.valueOf(new byte[]{
                (byte) 211, (byte) 209, (byte) 216});
        
        assertTrue(vec.toString().equals("110100111101000111011000"));
        assertTrue(vec.size() == 24);
        assertTrue(vec.capacity() == 24);
    }
    
    @Test
    public void testValueOfBooleans(){
        BitVector vec = BitVector.valueOf(new boolean[]{
                true,true,false,true,false,false,true,true,
                true,true,false,true,false,false,false,true,
                true,true,false,true,true});
        
        assertTrue(vec.toString().equals("110100111101000111011"));
        assertTrue(vec.size() == 21);
        assertTrue(vec.capacity() == 24);
    }
    
    @Test
    public void testValueOfByte(){
        byte b = 4;
        BitVector vec = BitVector.valueOf(b);
        assertTrue(vec.toString().equals("00000100"));
        assertTrue(vec.size() == 8);
        assertTrue(vec.capacity() == 8);
        b = -1;
        vec = BitVector.valueOf(b);
        assertTrue(vec.toString().equals("11111111"));
    }
    
    @Test
    public void testValueOfShort(){
        short s = 45;
        BitVector vec = BitVector.valueOf(s);
        assertTrue(vec.toString().equals("0000000000101101"));
        assertTrue(vec.size() == 16);
        assertTrue(vec.capacity() == 16);
        s = -1;
        vec = BitVector.valueOf(s);
        assertTrue(vec.toString().equals("1111111111111111"));
    }
    
    @Test
    public void testValueOfInt(){
        int i = 234;
        BitVector vec = BitVector.valueOf(i);
        assertTrue(vec.toString().equals("00000000000000000000000011101010"));
        assertTrue(vec.size() == 32);
        assertTrue(vec.capacity() == 32);
        i = -1;
        vec = BitVector.valueOf(i);
        assertTrue(vec.toString().equals("11111111111111111111111111111111"));
    }
    
    @Test
    public void testValueOfLong(){
        long l = 119;
        BitVector vec = BitVector.valueOf(l);
        assertTrue(vec.toString()
                .equals("0000000000000000000000000000000000000000000000000000000001110111"));
        
        assertTrue(vec.size() == 64);
        assertTrue(vec.capacity() == 64);
        l = -1;
        vec = BitVector.valueOf(l);
        assertTrue(vec.toString()
                .equals("1111111111111111111111111111111111111111111111111111111111111111"));
    }
    
    @Test
    public void testValueOfFloat(){
        float f = 234.45f;
        BitVector vec = BitVector.valueOf(f);
        assertTrue(vec.toString().equals("01000011011010100111001100110011"));
        assertTrue(vec.size() == 32);
        assertTrue(vec.capacity() == 32);
        f = -1.0f;
        vec = BitVector.valueOf(f);
        assertTrue(vec.toString().equals("10111111100000000000000000000000"));
    }
    
    @Test
    public void testValueOfDouble(){
        double d = 4756.04f;
        BitVector vec = BitVector.valueOf(d);
        assertTrue(vec.toString()
                .equals("0100000010110010100101000000101001000000000000000000000000000000"));
        
        assertTrue(vec.size() == 64);
        assertTrue(vec.capacity() == 64);
        d = -1.0;
        vec = BitVector.valueOf(d);
        assertTrue(vec.toString()
                .equals("1011111111110000000000000000000000000000000000000000000000000000"));
    }
    
    @Test
    public void testCreateInitialized(){
        BitVector vec = BitVector.createInitialized(8, true);
        assertTrue(vec.size() == 8);
        assertTrue(vec.capacity() == 8);
        assertTrue(vec.bitsSet() == 8);
        assertTrue(vec.bitsUnset() == 0);
        for(int i=0; i<vec.size(); ++i){
            assertTrue(vec.get(i));
        }
        vec = BitVector.createInitialized(9, false);
        assertTrue(vec.size() == 9);
        assertTrue(vec.capacity() == 16);
        assertTrue(vec.bitsSet() == 0);
        assertTrue(vec.bitsUnset() == 9);
        for(int i=0; i<vec.size(); ++i){
            assertFalse(vec.get(i));
        }
        vec = BitVector.createInitialized(20, true);
        assertTrue(vec.size() == 20);
        assertTrue(vec.capacity() == 24);
        assertTrue(vec.bitsSet() == 20);
        assertTrue(vec.bitsUnset() == 0);
        for(int i=0; i<vec.size(); ++i){
            assertTrue(vec.get(i));
        }
        vec = BitVector.createInitialized(15, false);
        assertTrue(vec.size() == 15);
        assertTrue(vec.capacity() == 16);
        assertTrue(vec.bitsSet() == 0);
        assertTrue(vec.bitsUnset() == 15);
        for(int i=0; i<vec.size(); ++i){
            assertFalse(vec.get(i));
        }
    }
    
    @Test
    public void testFromHexString(){
        BitVector vec = BitVector.fromHexString("d3d1d8");
        assertTrue(vec.toString().equals("110100111101000111011000"));
        assertTrue(vec.size() == 24);
        assertTrue(vec.capacity() == 24);
    }
    
    //**************************//
    //        Stress Test       //
    //**************************//
    
    @Test
    public void doStressTest(){
        BitVector vec = BitVector.valueOf("1101001100001011");
        vec.add(Bit._0);
        assertTrue(vec.size() == 17);
        assertTrue(vec.capacity() == 32);
        assertTrue(vec.bitsSet() == 8);
        assertTrue(vec.toString().equals("11010011000010110"));
        vec.set(9, Bit._1);
        assertTrue(vec.size() == 17);
        assertTrue(vec.capacity() == 32);
        assertTrue(vec.bitsSet() == 9);
        assertTrue(vec.toString().equals("11010011010010110"));
        vec.set(10, 13, Bit._1);
        assertTrue(vec.size() == 17);
        assertTrue(vec.capacity() == 32);
        assertTrue(vec.bitsSet() == 11);
        assertTrue(vec.toString().equals("11010011011110110"));
        vec.add(BitVector.valueOf("0001"));
        assertTrue(vec.size() == 21);
        assertTrue(vec.capacity() == 32);
        assertTrue(vec.bitsSet() == 12);
        assertTrue(vec.toString().equals("110100110111101100001"));
        vec.remove(0);
        vec.remove(vec.size()-1);
        assertTrue(vec.size() == 19);
        assertTrue(vec.capacity() == 32);
        assertTrue(vec.bitsSet() == 10);
        assertTrue(vec.toString().equals("1010011011110110000"));
        vec.remove(10, vec.size());
        assertTrue(vec.size() == 10);
        assertTrue(vec.capacity() == 32);
        assertTrue(vec.bitsSet() == 6);
        assertTrue(vec.toString().equals("1010011011"));
        vec.add(BitVector.valueOf("110100"));
        assertTrue(vec.size() == 16);
        assertTrue(vec.capacity() == 32);
        assertTrue(vec.bitsSet() == 9);
        assertTrue(vec.toString().equals("1010011011110100"));
        vec.flip(4, 10);
        assertTrue(vec.size() == 16);
        assertTrue(vec.capacity() == 32);
        assertTrue(vec.bitsSet() == 7);
        assertTrue(vec.toString().equals("1010100100110100"));
        vec.rotateLeft(5);
        assertTrue(vec.size() == 16);
        assertTrue(vec.capacity() == 32);
        assertTrue(vec.bitsSet() == 7);
        assertTrue(vec.toString().equals("0010011010010101"));
        vec.rotateRight(17);
        assertTrue(vec.size() == 16);
        assertTrue(vec.capacity() == 32);
        assertTrue(vec.bitsSet() == 7);
        assertTrue(vec.toString().equals("1001001101001010"));
        vec.insertAt(9, BitVector.valueOf("10001111001"));
        assertTrue(vec.size() == 27);
        assertTrue(vec.capacity() == 32);
        assertTrue(vec.bitsSet() == 13);
        assertTrue(vec.toString().equals("100100110100011110011001010"));
        vec.insertAt(0, Bit._1);
        assertTrue(vec.size() == 28);
        assertTrue(vec.capacity() == 32);
        assertTrue(vec.bitsSet() == 14);
        assertTrue(vec.toString().equals("1100100110100011110011001010"));
        vec.remove(9, vec.size());
        assertTrue(vec.size() == 9);
        assertTrue(vec.capacity() == 32);
        assertTrue(vec.bitsSet() == 5);
        assertTrue(vec.toString().equals("110010011"));
        vec.clear();
        assertTrue(vec.size() == 9);
        assertTrue(vec.capacity() == 32);
        assertTrue(vec.bitsSet() == 0);
        assertTrue(vec.toString().equals("000000000"));
        vec.clear(Bit._1);
        assertTrue(vec.size() == 9);
        assertTrue(vec.capacity() == 32);
        assertTrue(vec.bitsSet() == 9);
        assertTrue(vec.toString().equals("111111111"));
    }
    
}
