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

import java.io.File;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.raven.common.io.ConfigurationFileHandler.isComment;

/**
 * Represents a <code>.config</code> file from the filesystem.<br>
 * You can access a configuration section by calling <code>getSection()</code>
 * <br>All configurations in each section are available by calling 
 * <code>valueOf()</code> of that section.
 * 
 * <p>A ConfigurationFile is {@link Iterable}
 * 
 * @author Phil Gaiser
 * @see ConfigurationFileHandler
 * @since 1.0.0
 *
 */
public class ConfigurationFile implements Iterable<ConfigurationFile.Section> {

	private File file;
	private Map<String, Section> sections;
	private List<String> content;

	/**
	 * Constructs a new empty <code>ConfigurationFile</code>
	 */
	public ConfigurationFile(){
		this.sections = new HashMap<String, Section>();
		this.content = new LinkedList<String>();
	}

	/**
	 * Adds a new section to the end of this <code>ConfigurationFile</code>
	 * 
	 * @param section The <code>ConfigurationFile.Section</code> to be added
	 */
	public void addSection(final Section section){
		final String name = section.getName();
		if(this.sections.put(name, section) == null){
			this.content.add(name);
		}
	}

	/**
	 * Gets the configuration section with the specified name
	 * 
	 * @param name The name of the section to get
	 * @return The <code>ConfigurationFile.Section</code> with the specifeid 
	 * 		   name, or null if the section does not exist
	 */
	public Section getSection(final String name){
		return this.sections.get(name);
	}
	
	/**
	 * Removes the configuration section with the specified name.<br>
	 * The entire content of that section will be removed
	 * 
	 * @param name The name of the section to remove
	 */
	public void removeSection(final String name){
		if(sections.remove(name) != null){
			this.content.remove(name);
		}
	}
	
	/**
	 * Gets a {@link Date} object indicating the date and time the file this 
	 * <code>ConfigurationFile</code> object is associated with was last modified.<br>
	 * The date and time returned by this method is according to any modification of the 
	 * file in the filesystem
	 * 
	 * @return A date object representing the date and time the underlying file was
	 * 		   last modified
	 */
	public Date lastModified(){
		if(file != null){
			return new Date(file.lastModified());
		}
		return null;
	}

	@Override
	public String toString(){
		final StringBuilder sb = new StringBuilder();
		final String nl = System.lineSeparator();
		final Iterator<String> iter = content.iterator();
		Section section = null;
		while(iter.hasNext()){
			final String line = iter.next();
			if(isComment(line)){
				sb.append(line);
				sb.append(nl);
			}else{//is a section
				section = this.sections.get(line);
				sb.append("["+section.getName()+"]");
				sb.append(nl);
				final Iterator<String> sIter = section.sectionContent.iterator();
				while(sIter.hasNext()){
					final String sLine = sIter.next();
					if(isComment(sLine)){
						sb.append(sLine);
						sb.append(nl);
					}else{
						if(sLine.isEmpty()){
							sb.append(nl);
						}else{
							sb.append(sLine);
							sb.append("=");
							sb.append(section.configs.get(sLine));
							sb.append(nl);
						}
					}
				}
			}
		}
		return sb.toString();
	}
	
	@Override
	public Iterator<ConfigurationFile.Section> iterator(){
		return new SectionIterator();
	}
	
	protected void addComment(final String comment){
		this.content.add(comment);
	}
	
	protected void setFile(final File file){
		this.file = file;
	}

	/**
	 * Models a section within a <i>.config</i> file.<br>
	 * Every section must start with <i>'[xxxx]'</i>, where <i>xxxx</i> is
	 * the name of the section. All configurations of each section follow 
	 * immediately after the section name.<br>
	 * A commenting line always starts with <i>'#'</i>.
	 *
	 */
	public static class Section {

		private Map<String, String> configs;
		private List<String> sectionContent;
		private String name;
		
		/**
		 * Constructs a new empty <code>ConfigurationFile.Section</code>
		 * with the specified name
		 * 
		 * @param name The name of the configuration section
		 */
		public Section(String name){
			this();
			this.setName(name);
		}

		/**
		 * Protected constructor returning a 
		 * new <code>ConfigurationFile.Section</code>
		 */
		protected Section(){
			this.configs = new HashMap<String, String>();
			this.sectionContent = new LinkedList<String>();
		}

		/**
		 * Returns the configuration value for the specified key in this section
		 * 
		 * @param key The key of the configuration to get
		 * @return The value in this section currently associated with the specified
		 * 		   key, or null if this section does not hold a configuration with 
		 * 		   the specified key 
		 */
		public String valueOf(final String key){
			return this.configs.get(key);
		}

		/**
		 * Sets and possibly overrides the specified configuration with the specified value
		 * 
		 * @param key The key of the configuration to set
		 * @param value The new value of the specified configuration
		 */
		public void set(final String key, final String value){
			if(configs.put(key, value) == null){
				this.sectionContent.add(key);
			}
		}
		
		/**
		 * Removes the configuration with the specified key from this section
		 * 
		 * @param key The key of the configuration to remove
		 */
		public void remove(final String key){
			if(configs.remove(key) != null){
				this.sectionContent.remove(key);
			}
		}

		/**
		 * Gets the name of this configuration section
		 * 
		 * @return The name of this section
		 */
		public String getName(){
			return this.name;
		}

		/**
		 * Sets the name of this configuration section
		 * 
		 * @param name The name to set
		 */
		public void setName(String name){
			name = name.trim();
			if(name.startsWith("[")){
				name = name.substring(1, name.length());
			}
			if(name.endsWith("]")){
				name = name.substring(0, name.length()-1);
			}
			this.name = name;
		}

		protected void addComment(final String comment){
			this.sectionContent.add(comment);
		}
	}
	
	/**
	 * An iterator over a configuration file, iterating over each section.<br>
	 * Enables a ConfigurationFile to be target of the for-each-loop.
	 * 
	 * @since 1.0.0
	 *
	 */
	private class SectionIterator implements Iterator<ConfigurationFile.Section> {
		
		private Iterator<String> iter;
		private String line;
		
		/**
		 * Constructs a new <code>SectionIterator</code>
		 */
		private SectionIterator(){
			this.iter = content.iterator();
		}

		@Override
		public boolean hasNext(){
			while(iter.hasNext()){
				line = iter.next();
				if(!isComment(line)){
					return true;
				}
			}
			return false;
		}

		@Override
		public Section next(){
			return sections.get(line);
		}
	}

}
