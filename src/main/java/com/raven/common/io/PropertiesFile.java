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

import java.io.File;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Represents a <code>.properties</code> file from the filesystem.<br>
 * You may get/set properties by calling either <code>getProperty()</code>
 * or <code>setProperty()</code>.
 * 
 * <p>A PropertiesFile is {@link Cloneable}.
 * 
 * <p>This implementation is NOT thread-safe.
 * 
 * @author Phil Gaiser
 * @see PropertiesFileHandler
 * @since 1.0.0
 *
 */
public class PropertiesFile implements Cloneable {

    private File file;
    private Map<String, String> properties;

    /**
     * Constructs a new <code>PropertiesFile</code> with no properties set
     */
    public PropertiesFile(){
        this.properties = new HashMap<String, String>();
    }

    /**
     * Constructs a new <code>PropertiesFile</code> from the key-value pairs in
     * the specified {@link Map}
     * 
     * @param properties The Map holding the key-value pairs of the constructed
     * 					 PropertiesFile
     */
    public PropertiesFile(final Map<String, String> properties){
        this.properties = properties;
    }

    /**
     * Gets the property with the specified key, or null if the specified
     * property is not set
     * 
     * @param key The key of the property to get
     * @return The property associated with the specified key. May be null
     */
    public String getProperty(final String key){
        return this.properties.get(key);
    }

    /**
     * Sets the property with the specified key to the specified value
     * 
     * @param key The key of the property to set
     * @param value The value to be associated with above key 
     */
    public void setProperty(final String key, final String value){
        this.properties.put(key, value);
    }

    /**
     * Gets a {@link Set} of all properties of this <code>PropertiesFile</code>
     * 
     * @return A set holding all properties
     */
    public Set<Map.Entry<String, String>> allProperties(){
        return this.properties.entrySet();
    }

    /**
     * Gets a {@link Instant} object indicating the date and time the file this 
     * <code>PropertiesFile</code> object is associated with was last modified.<br>
     * The date and time returned by this method is according to any modification of the 
     * file in the filesystem
     * 
     * @return An Instant object representing the date and time the underlying file was
     *         last modified. May be null if the last modified time cannot be determnined
     */
    public Instant lastModified(){
        if(file != null){
            return Instant.ofEpochMilli(file.lastModified());
        }
        return null;
    }

    /**
     * Returns the total amount of properties set
     * 
     * @return The number of properties
     */
    public int total(){
        return this.properties.size();
    }

    @Override
    public Object clone(){
        final PropertiesFile clone = new PropertiesFile(
                new HashMap<String, String>(this.properties));

        clone.setFile(this.file);
        return clone;
    }

    protected void setFile(final File file){
        this.file = file;
    }

}
