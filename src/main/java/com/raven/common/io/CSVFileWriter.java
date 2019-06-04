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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;

import com.raven.common.struct.DataFrame;

/**
 * Convenience class for writing CSV-files. The constructors of this class assume 
 * that the default character encoding and the default separator should be used.<br>
 * To specify these values yourself, call <code>useCharset()</code> and
 * <code>useSeparator()</code> respectively.
 * 
 * <p>If set, the column names of the {@link DataFrame} passed to the 
 * <code>write()</code> method are used as the header of the created CSV-file.
 * 
 * @author Phil Gaiser
 * @see CSVFileReader
 * @see DataFrameSerializer
 * @since 1.0.0
 *
 */
public class CSVFileWriter {
	
	private BufferedWriter writer;
	private File file;
	private String separator = ",";
	
	/** Used for concurrent write operations **/
	private ConcurrentCSVWriter parallel;

	/**
	 * Constructs a new <code>CSVFileWriter</code> for the specified file
	 * 
	 * @param file The file to write. May be a path to a file
	 */
	public CSVFileWriter(final String file){
		if((file == null) || (file.isEmpty())){
			throw new IllegalArgumentException(
					"File argument must not be null or empty");
		}
		this.file = new File(file);
	}
	
	/**
	 * Constructs a new <code>CSVFileWriter</code> for the specified file object
	 * 
	 * @param file The {@link File} to write
	 */
	public CSVFileWriter(final File file){
		if(file == null){
			throw new IllegalArgumentException(
					"File argument must not be null");
		}
		this.file = file;
	}
	
	/**
	 * Writes the content of the specified DataFrame to the CSV-file.<br>
	 * The entries of each column are separated by the standard separator, 
	 * unless directly specified by <code>useSeparator()</code>
	 * <p>This method can only be called once. Subsequent calls will result in an
	 * <code>IOException</code>.<br>
	 * 
	 * @param df The DataFrame to write to a CSV-file. Must not be null
	 * @throws IOException If the file cannot be opened or written, or if this
						   method has already been called
	 */
	public void write(final DataFrame df) throws IOException{
		if(df == null){
			throw new IllegalArgumentException("DataFrame must not be null");
		}
		if(writer == null){
			this.writer = new BufferedWriter(new FileWriter(this.file));
		}
		try{
			final String nl = System.lineSeparator();
			final int cols = df.columns();//cache

			if(df.hasColumnNames()){//add header if available
				final String[] names = df.getColumnNames();
				for(int i=0; i<names.length; ++i){
					writer.write(escape(names[i]));
					if(i<names.length-1){
						writer.write(separator);
					}
				}
				writer.write(nl);
			}
			for(int i=0; i<df.rows(); ++i){//add rows
				final Object[] row = df.getRowAt(i);
				for(int j=0; j<cols; ++j){
					writer.write(row[j] != null ? escape(row[j].toString()) : "null");
					if(j<cols-1){
						writer.write(separator);
					}
				}
				writer.write(nl);
			}
		}catch(RuntimeException ex){
			throw new IOException(ex);
		}finally{
			writer.close();
		}
	}
	
	/**
	 * Creates a background thread which will write the CSV-file and execute
	 * the provided callback when finished.<br>
	 * This method can only be called once. Subsequent calls will result in an
	 * <code>IOException</code>.<br>
	 * <p>This method is meant to be used for large DataFrames/CSV-files.<br>
	 * <p>When called, this method will return immediately. The result of the 
	 * background operation will be passed to the {@link ConcurrentWriter} callback.
	 * <p>Please note that any IOExceptions encountered by the launched background 
	 * thread will be silently dropped. Check the File object passed to the callback
	 * against null in order to spot any errors
	 * 
	 * <p>See also: {@link #write(DataFrame)}
	 * 
	 * @param df The DataFrame to write to a CSV-file
	 * @param delegate The callback for the result of this operation. May be null
	 * @throws IOException If this method has already been called
	 */
	public void parallelWrite(DataFrame df, ConcurrentWriter delegate) throws IOException{
		if(parallel != null){
			throw new IOException("parallelWrite() already called");
		}
		this.parallel = new ConcurrentCSVWriter(df, delegate);
		this.parallel.execute();
	}
	
	/**
	 * Instructs this <code>CSVFileWriter</code> to use the specified separator when
	 * writing to the CSV-file.<br>
	 * The default separator is a comma (','). 
	 * 
	 * @param separator The character to be used as a separator
	 * @return This CSVFileWriter instance
	 */
	public CSVFileWriter useSeparator(final char separator){
		if(separator == '"'){
			throw new IllegalArgumentException(
					"Cannot use double quotes as separator character");
		}
		this.separator = String.valueOf(separator);
		return this;
	}
	
	/**
	 * Instructs this <code>CSVFileWriter</code> to use the specified Charset when
	 * writing to the CSV-file.<br>
	 * For example: <code>"UTF-8"</code>, which is unicode encoded as UTF-8
	 * 
	 * @param charset The charset to be used when writing
	 * @return This CSVFileWriter instance
	 * @throws UnsupportedEncodingException When the specified charset is
	 * 										invalid or not supported
	 * @throws FileNotFoundException If the file represents a directory, cannot be 
	 * 								 created or opened for some reason
	 */
	public CSVFileWriter useCharset(final String charset)
			throws UnsupportedEncodingException, FileNotFoundException{
		
		 this.writer = new BufferedWriter(
				 new OutputStreamWriter(new FileOutputStream(this.file), charset));
		 
		 return this;
	}
	
	/**
	 * Escapes a string by enclosing it with double quotes if applicable
	 * 
	 * @param str The String to escape
	 * @return The escaped String
	 */
	private String escape(final String str){
		if(str.contains(separator)){
			return "\"" + str + "\"";
		}
		return str;
	}
	
	/**
	 * Background thread for concurrent write operations of CSV-files.
	 *
	 */
	private class ConcurrentCSVWriter implements Runnable {
		
		private ConcurrentWriter delegate;
		private DataFrame df;
		
		/**
		 * Constructs a new <code>ConcurrentCSVWriter</code>
		 * 
		 * @param df The DataFrame to write
		 * @param delegate The delegate for the callback
		 */
		ConcurrentCSVWriter(final DataFrame df, final ConcurrentWriter delegate){
			this.df = df;
			this.delegate = delegate;
		}
		
		/**
		 * Starts this Runnable in its own thread
		 */
		public void execute(){
			new Thread(this).start();
		}

		@Override
		public void run(){
			try{
				write(df);
			}catch(IOException ex){
				if(delegate != null){
					delegate.onWritten(null);
				}
				return;
			}
			
			if(delegate != null){
				delegate.onWritten(file);
			}
		}
	}
}
