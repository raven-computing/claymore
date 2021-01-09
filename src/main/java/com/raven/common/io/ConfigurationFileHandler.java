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

import com.raven.common.io.ConfigurationFile.Section;

/**
 * Handles reading and writing of <code>.config</code> files.<br>
 * This class is immutable, i.e. the file passed to the constructor cannot
 * be changed afterwards. Comments (lines that start with '#') are preserved
 * when writing to a file.
 * 
 * <p>This class also provides static utility methods for reading
 * ConfigurationFile objects from an InputStream and
 * writing ConfigurationFile objects to an OutputStream.
 * 
 * @author Phil Gaiser
 * @see ConfigurationFile
 * @since 1.0.0
 *
 */
public final class ConfigurationFileHandler {

    private File file;
    private InputStream is;
    private OutputStream os;

    /**
     * Constructs a new <code>ConfigurationFileHandler</code> for
     * the specified file
     * 
     * @param file The file to be handled by this ConfigurationFileHandler
     */
    public ConfigurationFileHandler(final String file){
        this.file = new File(file);
    }

    /**
     * Constructs a new <code>ConfigurationFileHandler</code> for 
     * the specified file object
     * 
     * @param file The file to be handled by this ConfigurationFileHandler
     */
    public ConfigurationFileHandler(final File file){
        this.file = file;
    }

    /**
     * Constructs a new <code>ConfigurationFileHandler</code> for 
     * reading the specified input stream
     * 
     * @param is The input stream to read from
     */
    private ConfigurationFileHandler(final InputStream is){
        this.is = is;
    }

    /**
     * Constructs a new <code>ConfigurationFileHandler</code> for 
     * writing to the specified output stream
     * 
     * @param os The output stream to write to
     */
    private ConfigurationFileHandler(final OutputStream os){
        this.os = os;
    }

    /**
     * Reads the <i>.config</i> file and returns a {@link ConfigurationFile} 
     * representing the content of it
     * 
     * @return A <code>ConfigurationFile</code> representing the file
     * @throws IOException If the file cannot be opened or read, 
     * 					   or if it has the wrong format
     */
    public ConfigurationFile read() throws IOException{
        final ConfigurationFile configs = new ConfigurationFile();
        final BufferedReader reader = createReader();
        String line = "";
        Section section = new Section();
        boolean inSection = false;
        int i = 0;
        try{
            while((line = reader.readLine()) != null){
                ++i;
                if(inSection){
                    if(isComment(line)){
                        section.addComment(line);;
                    }else if(isSection(line)){
                        configs.addSection(section);
                        section = new Section(trim(line, i));
                    }else{
                        final String[] block = line.split("=", 2);
                        section.set(block[0], (block.length < 2 ? null : block[1]));
                    }
                }else{
                    if(isComment(line)){
                        configs.addComment(line);
                    }else if(isSection(line)){
                        section = new Section(trim(line, i));
                        inSection = true;
                    }else{
                        reader.close();
                        throw new IOException(String.format(
                                "Improperly formatted configuration file: '%s' (at line %s)",
                                line, i));
                    }
                }
            }
            if(inSection){
                configs.addSection(section);
            }
        }finally{
            reader.close();
        }
        configs.setFile(this.file);
        return configs;
    }

    /**
     * Persists the specified {@link ConfigurationFile} to the filesystem
     * 
     * @param config The <code>ConfigurationFile</code> to persist
     * @throws IOException If the file cannot be opened or written
     */
    public void write(final ConfigurationFile config) throws IOException{
        final BufferedWriter writer = createWriter();
        try{
            writer.write(config.toString());
        }finally{
            writer.close();
        }
    }

    /**
     * Reads a configuration file from the specified input stream.
     * 
     * <p>The input stream will be closed automatically before this method returns
     * 
     * @param is The <code>InputStream</code> to read from. Must not be null
     * @return A <code>ConfigurationFile</code> read from the specified InputStream
     * @throws IOException If the input stream cannot be read, or if the
     *                     configuration file has the wrong format
     */
    public static ConfigurationFile readFrom(final InputStream is) throws IOException{
        return new ConfigurationFileHandler(is).read();
    }

    /**
     * Writes the specified configuration file to the specified output stream.
     * 
     * <p>The output stream will be closed automatically before this method returns
     * 
     * @param os The <code>OutputStream</code> to write to. Must not be null
     * @param config The <code>ConfigurationFile</code> to write to
     *               the specified OutputStream
     * @throws IOException If the output stream cannot be written to
     */
    public static void writeTo(final OutputStream os, final ConfigurationFile config)
            throws IOException{

        new ConfigurationFileHandler(os).write(config);
    }

    /**
     * Creates a BufferedReader for this ConfigurationFileHandler instance
     * 
     * @return A <code>BufferedReader</code> for reading characters
     *         from an input stream or file
     * @throws FileNotFoundException If this ConfigurationFileHandler was constructed to
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
     * Creates a BufferedWriter for this ConfigurationFileHandler instance
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
     * Trims a configuration file section name of the mandatory square brackets
     * at the beginning and end
     * 
     * @param section The name of the section to trim
     * @param index The line index. Used for error mesages
     * @return The section name without the brackets
     * @throws IOException If the given section name is inproperly formatted
     */
    private String trim(final String section, final int index) throws IOException{
        final String tmp = section.trim();
        if(!tmp.startsWith("[") || !tmp.endsWith("]")){
            throw new IOException(String.format(
                    "Improperly formatted configuration file: '%s' (at line %s)",
                    tmp, index));
            
        }
        return tmp.substring(1, tmp.length()-1);
    }

    /**
     * Indicates whether the specified string represents a comment
     * 
     * @param line The line to check
     * @return True, if the specified line is a comment, false otherwise
     */
    protected static boolean isComment(final String line){
        return (line.trim().startsWith("#") || line.isEmpty());
    }

    /**
     * Indicates whether the specified string represents a section header
     * 
     * @param line The line to check
     * @return True, if the specified line is the beginning of a section, false otherwise
     */
    protected static boolean isSection(final String line){
        return line.trim().startsWith("[");
    }

}
