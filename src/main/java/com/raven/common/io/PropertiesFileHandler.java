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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;

/**
 * Handles reading and writing of <code>.properties</code> files.<br>
 * This class is immutable, i.e. the file passed to the constructor cannot
 * be changed afterwards.
 * 
 * <p>This class also provides static utility methods for reading
 * PropertiesFile objects from an InputStream and
 * writing PropertiesFile objects to an OutputStream.
 * 
 * @author Phil Gaiser
 * @see PropertiesFile
 * @since 1.0.0
 *
 */
public class PropertiesFileHandler {

    private File file;
    private InputStream is;
    private OutputStream os;

    /**
     * Constructs a new <code>PropertiesFileHandler</code> for the specified file
     * 
     * @param file The file to be handled by this PropertiesFileHandler
     */
    public PropertiesFileHandler(final String file){
        this(new File(file));
    }

    /**
     * Constructs a new <code>PropertiesFileHandler</code> for the specified file object
     * 
     * @param file The file to be handled by this PropertiesFileHandler
     */
    public PropertiesFileHandler(final File file){
        this.file = file;
    }

    /**
     * Constructs a new <code>PropertiesFileHandler</code> for 
     * reading the specified input stream
     * 
     * @param is The input stream to read from
     */
    private PropertiesFileHandler(final InputStream is){
        this.is = is;
    }

    /**
     * Constructs a new <code>PropertiesFileHandler</code> for 
     * writing to the specified output stream
     * 
     * @param os The output stream to write to
     */
    private PropertiesFileHandler(final OutputStream os){
        this.os = os;
    }

    /**
     * Reads the <i>.properties</i> file and returns a {@link PropertiesFile}
     * representing the content of it
     * 
     * @return A <code>PropertiesFile</code> representing the file
     * @throws IOException If the file cannot be opened or read
     */
    public PropertiesFile read() throws IOException{
        final PropertiesFile properties = new PropertiesFile();
        final BufferedReader reader = createReader();
        String line = "";
        try{
            while((line = reader.readLine()) != null){
                if(!isCommentedLine(line) && !line.isEmpty()){
                    final String[] block = line.split("=", 2);
                    properties.setProperty(block[0], (block.length < 2 ? null : block[1]));
                }
            }
        }finally{
            reader.close();
        }
        properties.setFile(this.file);
        return properties;
    }

    /**
     * Persists the specified {@link PropertiesFile} to the filesystem
     * 
     * @param properties The <code>PropertiesFile</code> to persist
     * @throws IOException If the file cannot be opened or written
     */
    public void write(final PropertiesFile properties) throws IOException{
        final BufferedWriter writer = createWriter();
        final String nl = System.lineSeparator();
        final Iterator<Map.Entry<String, String>> iter = properties.allProperties().iterator();
        try{
            while(iter.hasNext()){
                final Map.Entry<String, String> e = iter.next();
                writer.write(e.getKey()+"="+e.getValue()+nl);
            }
        }finally{
            writer.close();
        }
    }

    /**
     * Reads a properties file from the specified input stream.
     * 
     * <p>The input stream will be closed automatically before this method returns
     * 
     * @param is The <code>InputStream</code> to read from. Must not be null
     * @return A <code>PropertiesFile</code> read from the specified InputStream
     * @throws IOException If the input stream cannot be read, or if the
     *                     properties file has the wrong format
     */
    public static PropertiesFile readFrom(final InputStream is) throws IOException{
        return new PropertiesFileHandler(is).read();
    }

    /**
     * Writes the specified properties file to the specified output stream.
     * 
     * <p>The output stream will be closed automatically before this method returns
     * 
     * @param os The <code>OutputStream</code> to write to. Must not be null
     * @param config The <code>PropertiesFile</code> to write to
     *               the specified OutputStream
     * @throws IOException If the output stream cannot be written to
     */
    public static void writeTo(final OutputStream os, final PropertiesFile config)
            throws IOException{

        new PropertiesFileHandler(os).write(config);
    }

    /**
     * Creates a BufferedReader for this PropertiesFileHandler instance
     * 
     * @return A <code>BufferedReader</code> for reading characters
     *         from an input stream or file
     * @throws FileNotFoundException If this PropertiesFileHandler was constructed to
     *                               use a File object and the corresponding
     *                               file was not found
     */
    private BufferedReader createReader() throws FileNotFoundException{
        if(file != null){
            return new BufferedReader(new FileReader(file));

        }else if(is != null){
            return new BufferedReader(
                    new InputStreamReader(this.is, StandardCharsets.UTF_8));
        }else{
            throw new IllegalStateException("No read source");
        }
    }

    /**
     * Creates a BufferedWriter for this PropertiesFileHandler instance
     * 
     * @return A <code>BufferedWriter</code> for writing characters
     *         to an output stream or file
     * @throws IOException If the file is a directory or cannot be opened
     */
    private BufferedWriter createWriter() throws IOException{
        if(file != null){
            return new BufferedWriter(new FileWriter(this.file));

        }else if(os != null){
            return new BufferedWriter(
                    new OutputStreamWriter(this.os, StandardCharsets.UTF_8));

        }else{
            throw new IllegalStateException("No write target");
        }
    }

    /**
     * Indicates whether the specified string represents a comment
     * 
     * @param line The line the check
     * @return True, if the specified line is a comment, false otherwise
     */
    private boolean isCommentedLine(final String line){
        return (line.startsWith("#") || line.startsWith("["));
    }
}
