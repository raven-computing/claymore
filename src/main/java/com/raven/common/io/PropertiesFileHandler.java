/* 
 * Copyright (C) 2019 Raven Computing
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
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Handles reading and writing of <code>.properties</code> files.<br>
 * This class is immutable, i.e. the file passed to the constructor cannot
 * be changed afterwards.
 * 
 * @author Phil Gaiser
 * @see PropertiesFile
 * @since 1.0.0
 *
 */
public class PropertiesFileHandler {
	
	private File file;

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
	 * Reads the <i>.properties</i> file and returns a {@link PropertiesFile} representing the 
	 * content of it
	 * 
	 * @return A <code>PropertiesFile</code> representing the file
	 * @throws IOException If the file cannot be opened or read
	 */
	public PropertiesFile read() throws IOException{
		final PropertiesFile properties = new PropertiesFile();
		final BufferedReader reader = new BufferedReader(new FileReader(file));
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
		final BufferedWriter writer = new BufferedWriter(new FileWriter(this.file));
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
	 * Indicates whether the specified string represents a comment
	 * 
	 * @param line The line the check
	 * @return True, if the specified line is a comment, false otherwise
	 */
	private boolean isCommentedLine(final String line){
		return (line.startsWith("#") || line.startsWith("["));
	}

}
