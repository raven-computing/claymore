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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.raven.common.struct.DataFrame;
import com.raven.common.struct.DefaultDataFrame;
import com.raven.common.struct.DoubleColumn;
import com.raven.common.struct.IntColumn;
import com.raven.common.struct.NullableDataFrame;
import com.raven.common.struct.NullableDoubleColumn;
import com.raven.common.struct.NullableIntColumn;
import com.raven.common.struct.NullableStringColumn;
import com.raven.common.struct.StringColumn;

/**
 * Tests for the CSVWriter implementation.
 *
 */
public class CSVWriterTest {
    
    DataFrame df1 = new DefaultDataFrame(
            new IntColumn("AttrA", new int[]{1,2,3}),
            new DoubleColumn("AttrB", new double[]{1.1,2.2,3.3}),
            new StringColumn("AttrC", new String[]{"C1","C2","C,3"}));
    
    DataFrame df2Nullable = new NullableDataFrame(
            new NullableIntColumn("AttrA", new Integer[]{null,2,3}),
            new NullableDoubleColumn("AttrB", new Double[]{1.1,null,3.3}),
            new NullableStringColumn("AttrC", new String[]{"C1","C2",null}));
    
    //regex which splits a string by a comma
    //if it is not enclosed by double quotes
    Pattern pattern = Pattern.compile(","
            + "(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
    
    @BeforeClass
    public static void setUpBeforeClass(){ }
    
    @AfterClass
    public static void tearDownAfterClass(){ }
    
    @Before
    public void setUp(){ }
    
    @After
    public void tearDown(){ }
    
    @Test
    public void testStreamWritePlain() throws IOException{
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        new CSVWriter(baos).write(df1);
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        BufferedReader reader = new BufferedReader(new InputStreamReader(bais));
        String line = "";
        int i = 0;
        reader.readLine();//Skip first line header
        while((line = reader.readLine()) != null){
            String[] s = pattern.split(line, 0);
            Object[] row = df1.getRow(i++);
            assertTrue("Row length does not match", s.length == row.length);
            for(int j=0; j<row.length; ++j){
                assertEquals(row[j].toString(), normalize(s[j]));
            }
        }
    }
    
    @Test
    public void testStreamWritePlainNoHeader() throws IOException{
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        new CSVWriter(baos).withHeader(false).write(df1);
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        BufferedReader reader = new BufferedReader(new InputStreamReader(bais));
        String line = "";
        int i = 0;
        while((line = reader.readLine()) != null){
            String[] s = pattern.split(line, 0);
            Object[] row = df1.getRow(i++);
            assertTrue("Row length does not match", s.length == row.length);
            for(int j=0; j<row.length; ++j){
                assertEquals(row[j].toString(), normalize(s[j]));
            }
        }
    }
    
    @Test
    public void testStreamWriteAsync() throws Exception{
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Future<Void> future = new CSVWriter(baos).withHeader(false).writeAsync(df1);
        future.get();
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        BufferedReader reader = new BufferedReader(new InputStreamReader(bais));
        String line = "";
        int i = 0;
        while((line = reader.readLine()) != null){
            String[] s = pattern.split(line, 0);
            Object[] row = df1.getRow(i++);
            assertTrue("Row length does not match", s.length == row.length);
            for(int j=0; j<row.length; ++j){
                assertEquals(row[j].toString(), normalize(s[j]));
            }
        }
    }
    
    @Test
    public void testStreamWriteNullable() throws IOException{
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        new CSVWriter(baos).withHeader(false).write(df2Nullable);
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        BufferedReader reader = new BufferedReader(new InputStreamReader(bais));
        String line = "";
        int i = 0;
        while((line = reader.readLine()) != null){
            String[] s = pattern.split(line, 0);
            Object[] row = df2Nullable.getRow(i++);
            assertTrue("Row length does not match", s.length == row.length);
            for(int j=0; j<row.length; ++j){
                if(row[j] != null){
                    assertEquals(row[j].toString(), normalize(s[j]));
                }else{
                    assertEquals("null", s[j]);
                }
            }
        }
    }
    
    @Test(expected = IllegalStateException.class)
    public void testWriteIllegaState() throws IOException{
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CSVWriter csv = new CSVWriter(baos);
        csv.write(df1);
        csv.write(df1);
    }
    
    @Test(expected = IllegalStateException.class)
    public void testWriteIllegaStateAsync() throws IOException{
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CSVWriter csv = new CSVWriter(baos);
        csv.writeAsync(df1);
        csv.writeAsync(df1);
    }
    
    private String normalize(String str){
        if((str == null) || str.isEmpty()){
            return str;
        }
        if((str.charAt(0) == '"') && (str.charAt(str.length()-1) == '"')){
            return str.substring(1, str.length()-1);
        }
        return str;
    }
    
}
