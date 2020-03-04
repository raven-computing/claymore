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

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.concurrent.CompletableFuture;

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
 * Tests for the CSVReader implementation.
 *
 */
public class CSVReaderTest {
    
    String csv1 = "/test.csv";
    
    DataFrame df1 = new DefaultDataFrame(
            new IntColumn("AttrA", new int[]{1,2,3}),
            new DoubleColumn("AttrB", new double[]{1.1,2.2,3.3}),
            new StringColumn("AttrC", new String[]{"C1","C2","C,3"}));
    
    DataFrame df1StringsOnly = new DefaultDataFrame(
            new StringColumn("AttrA", new String[]{"1","2","3"}),
            new StringColumn("AttrB", new String[]{"1.1","2.2","3.3"}),
            new StringColumn("AttrC", new String[]{"C1","C2","C,3"}));
    
    String csv1NoHeader = "/test_noheader.csv";
    
    String csv2Nullable = "/test_nullable.csv";
    
    DataFrame df2Nullable = new NullableDataFrame(
            new NullableIntColumn("AttrA", new Integer[]{null,2,3}),
            new NullableDoubleColumn("AttrB", new Double[]{1.1,null,3.3}),
            new NullableStringColumn("AttrC", new String[]{"C1","C2",null}));
    
    DataFrame df2NullableStringsOnly = new NullableDataFrame(
            new NullableStringColumn("AttrA", new String[]{null,"2","3"}),
            new NullableStringColumn("AttrB", new String[]{"1.1",null,"3.3"}),
            new NullableStringColumn("AttrC", new String[]{"C1","C2",null}));
    
    @BeforeClass
    public static void setUpBeforeClass(){ }
    
    @AfterClass
    public static void tearDownAfterClass(){ }
    
    @Before
    public void setUp(){ }
    
    @After
    public void tearDown(){ }
    
    @Test
    public void testFileReadPlain() throws IOException{
        URL url = this.getClass().getResource(csv1);
        if(url == null){
            fail("Test resource \"" + csv1 + "\" was not found");
            return;
        }
        File file = new File(url.getFile());
        DataFrame df = new CSVReader(file).read();
        assertEquals("DataFrames do not match", df1StringsOnly, df);
    }
    
    @Test
    public void testFileReadWithTypes() throws IOException{
        URL url = this.getClass().getResource(csv1);
        if(url == null){
            fail("Test resource \"" + csv1 + "\" was not found");
            return;
        }
        File file = new File(url.getFile());
        DataFrame df = new CSVReader(file)
                .useColumnTypes(Integer.class, Double.class, String.class)
                .read();
        
        assertEquals("DataFrames do not match", df1, df);
    }
    
    @Test
    public void testFileReadPlainAsync() throws Exception{
        URL url = this.getClass().getResource(csv1);
        if(url == null){
            fail("Test resource \"" + csv1 + "\" was not found");
            return;
        }
        File file = new File(url.getFile());
        CompletableFuture<DataFrame> future = new CSVReader(file).readAsync();
        DataFrame df = future.get();
        assertEquals("DataFrames do not match", df1StringsOnly, df);
    }
    
    @Test
    public void testFileReadPlainNoHeader() throws IOException{
        URL url = this.getClass().getResource(csv1NoHeader);
        if(url == null){
            fail("Test resource \"" + csv1NoHeader + "\" was not found");
            return;
        }
        File file = new File(url.getFile());
        DataFrame df = new CSVReader(file).withHeader(false).read();
        //Remove column names to match expected 
        df1StringsOnly.removeColumnNames();
        assertEquals("DataFrames do not match", df1StringsOnly, df);
    }
    
    @Test
    public void testFileReadWithTypesNoHeader() throws IOException{
        URL url = this.getClass().getResource(csv1NoHeader);
        if(url == null){
            fail("Test resource \"" + csv1NoHeader + "\" was not found");
            return;
        }
        File file = new File(url.getFile());
        DataFrame df = new CSVReader(file)
                .withHeader(false)
                .useColumnTypes(Integer.class, Double.class, String.class)
                .read();
        
        //Remove column names to match expected 
        df1.removeColumnNames();
        assertEquals("DataFrames do not match", df1, df);
    }
    
    @Test
    public void testFileReadPlainNullable() throws IOException{
        URL url = this.getClass().getResource(csv2Nullable);
        if(url == null){
            fail("Test resource \"" + csv2Nullable + "\" was not found");
            return;
        }
        File file = new File(url.getFile());
        DataFrame df = new CSVReader(file).read();
        assertEquals("DataFrames do not match", df2NullableStringsOnly, df);
    }
    
    @Test
    public void testFileReadWithTypesNullable() throws IOException{
        URL url = this.getClass().getResource(csv2Nullable);
        if(url == null){
            fail("Test resource \"" + csv2Nullable + "\" was not found");
            return;
        }
        File file = new File(url.getFile());
        DataFrame df = new CSVReader(file)
                .useColumnTypes(Integer.class, Double.class, String.class)
                .read();
        
        assertEquals("DataFrames do not match", df2Nullable, df);
    }
    
    @Test(expected = IllegalStateException.class)
    public void testReadIllegaState() throws IOException{
        URL url = this.getClass().getResource(csv1);
        if(url == null){
            fail("Test resource \"" + csv1 + "\" was not found");
            return;
        }
        File file = new File(url.getFile());
        CSVReader csv = new CSVReader(file);
        csv.read();
        csv.read();
    }
    
    @Test(expected = IllegalStateException.class)
    public void testReadIllegaStateAsync() throws IOException{
        URL url = this.getClass().getResource(csv1);
        if(url == null){
            fail("Test resource \"" + csv1 + "\" was not found");
            return;
        }
        File file = new File(url.getFile());
        CSVReader csv = new CSVReader(file);
        csv.readAsync();
        csv.readAsync();
    }
    
}
