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

import com.raven.common.io.ConfigurationFile.Section;

/**
 * Handles reading and writing of <code>.config</code> files.<br>
 * This class is immutable, i.e. the file passed to the constructor cannot
 * be changed afterwards. Comments (lines that start with '#') are preserved
 * when writing to a file.
 * 
 * @author Phil Gaiser
 * @see ConfigurationFile
 * @since 1.0.0
 *
 */
public class ConfigurationFileHandler {
	
	private File file;

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
	 * Reads the <i>.config</i> file and returns a {@link ConfigurationFile} 
	 * representing the content of it
	 * 
	 * @return A <code>ConfigurationFile</code> representing the file
	 * @throws IOException If the file cannot be opened or read, 
	 * 					   or if it has the wrong format
	 */
	public ConfigurationFile read() throws IOException{
		final ConfigurationFile configs = new ConfigurationFile();
		final BufferedReader reader = new BufferedReader(new FileReader(file));
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
		final BufferedWriter writer = new BufferedWriter(new FileWriter(this.file));
		try{
			writer.write(config.toString());
		}finally{
			writer.close();
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
