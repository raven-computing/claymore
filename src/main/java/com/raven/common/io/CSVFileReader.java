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
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.regex.Pattern;

import javax.lang.model.type.NullType;

import com.raven.common.struct.BooleanColumn;
import com.raven.common.struct.ByteColumn;
import com.raven.common.struct.CharColumn;
import com.raven.common.struct.Column;
import com.raven.common.struct.DataFrame;
import com.raven.common.struct.DataFrameException;
import com.raven.common.struct.DefaultDataFrame;
import com.raven.common.struct.DoubleColumn;
import com.raven.common.struct.FloatColumn;
import com.raven.common.struct.IntColumn;
import com.raven.common.struct.LongColumn;
import com.raven.common.struct.NullableDataFrame;
import com.raven.common.struct.ShortColumn;
import com.raven.common.struct.StringColumn;

/**
 * Convenience class for reading CSV-files. The constructors of this class assume 
 * that the default character encoding and the default separator should be used.<br>
 * To specify these values yourself, call <code>useCharset()</code> and
 * <code>useSeparator()</code> respectively.
 * 
 * <p>If not further specified, constructors assume that the CSV-file has 
 * <b>NO HEADER</b>. A boolean should always be passed to the constructor to indicate 
 * whether the file has a header.
 * 
 * <p>The {@link DataFrame} returned by the <code>read()</code> method uses the
 * <i>String</i> type for all columns by default. However, the constructors let you specify 
 * each type individually. Passing the types either directly to the costructor or the
 * <code>useColumnTypes()</code> method will ensure that the columns of the returned 
 * DataFrame are of the corresponding type. The order of the types in the argument 
 * will specify to which column that type is assigned to.
 * 
 * <p>There is only one restriction regarding the separator and the content of
 * the CSV-file to read.<br>
 * Any character except <i>double quotes</i> can be used as a separator. If a data value
 * contains one or more instances of the used separator character, then that data value
 * must be enclosed with double quotes. For that reason <i>double quotes</i> cannot be used as
 * a separator character or occur inside data values.
 * 
 * <p>If a data value in the CSV-file is nonexistent or empty, then it must have a
 * text representation of <i>null</i> inside the file to read. Simply omitting nonexistent
 * or empty values, causing separators to be pasted together, is not allowed.
 * 
 * @author Phil Gaiser
 * @see CSVFileWriter
 * @since 1.0.0
 *
 */
public class CSVFileReader {
	
	private BufferedReader reader;
	private File file;
	private String separator = ",";
	private Column[] types;
	private boolean hasHeader;
	
	/** Used for concurrent read operations **/
	private ConcurrentCSVReader parallel;

	/**
	 * Constructs a new <code>CSVFileReader</code> for the specified file.<br>
	 * It's assumed that the CSV-file has <b>no header</b>.<br>
	 * All columns of the returned DataFrame will be of type <i>String</i>.
	 * 
	 * @param file The file to read. May be a path to a file
	 * @throws FileNotFoundException If the file was not found or represents a directory
	 */
	public CSVFileReader(final String file) throws FileNotFoundException{
		this(new File(file), false, NullType.class);
	}
	
	/**
	 * Constructs a new <code>CSVFileReader</code> for the specified file object.<br>
	 * It's assumed that the CSV-file has <b>no header</b>.<br>
	 * All columns of the returned DataFrame will be of type <i>String</i>.
	 * 
	 * @param file The {@link File} to read
	 * @throws FileNotFoundException If the file was not found or represents a directory
	 */
	public CSVFileReader(final File file) throws FileNotFoundException{
		this(file, false, NullType.class);
	}
	
	/**
	 * Constructs a new <code>CSVFileReader</code> for the specified file.<br>
	 * It must be specified whether the CSV-file has a header.<br>
	 * All columns of the returned DataFrame will be of type <i>String</i>.
	 * 
	 * @param file The file to read. May be a path to a file
	 * @param hasHeader Indicates whether the first line of the specified file 
	 * 					represents a header
	 * @throws FileNotFoundException If the file was not found or represents a directory
	 */
	public CSVFileReader(final String file, final boolean hasHeader) 
			throws FileNotFoundException{
		
		this(new File(file), hasHeader, NullType.class);
	}
	
	/**
	 * Constructs a new <code>CSVFileReader</code> for the specified file object.<br>
	 * It must be specified whether the CSV-file has a header.<br>
	 * All columns of the returned DataFrame will be of type <i>String</i>.
	 * 
	 * @param file The {@link File} to read
	 * @param hasHeader Indicates whether the first line of the specified file 
	 * 					represents a header
	 * @throws FileNotFoundException If the file was not found or represents a directory
	 */
	public CSVFileReader(final File file, final boolean hasHeader) 
			throws FileNotFoundException{
		
		this(file, hasHeader, NullType.class);
	}
	
	/**
	 * Constructs a new <code>CSVFileReader</code> for the specified file.<br>
	 * It must be specified whether the CSV-file has a header.<br>
	 * The type of each column must be specified.<br>
	 * <p><i>Example:</i><br>
	 * <pre><code> 
	 * new CSVFileReader("file.csv", true, Integer.class, String.class, Float.class);
	 * </code></pre>
	 * 
	 * The above code will construct a <code>CSVFileReader</code> which can read a
	 * file called "file.csv" which has a header. The first column consists of integers,
	 * the second column consists of strings and the third column consists of floats
	 * 
	 * @param file The file to read. May be a path to a file
	 * @param hasHeader Indicates whether the first line of the specified file 
	 * 					represents a header
	 * @param types The types corresponding to each column
	 * @throws FileNotFoundException If the file was not found or represents a directory
	 */
	public CSVFileReader(final String file, final boolean hasHeader,
			final Class<?>... types) throws FileNotFoundException{
		
		this(new File(file), hasHeader, types);
	}
	
	/**
	 * Constructs a new <code>CSVFileReader</code> for the specified file object.<br>
	 * It must be specified whether the CSV-file has a header.<br>
	 * The type of each column must be specified.<br>
	 * <p><i>Example:</i><br>
	 * <pre><code> 
	 * new CSVFileReader("file.csv", true, Integer.class, String.class, Float.class);
	 * </code></pre>
	 * 
	 * The above code will construct a <code>CSVFileReader</code> which can read a
	 * file called "file.csv" which has a header. The first column consists of integers,
	 * the second column consists of strings and the third column consists of floats
	 * 
	 * @param file The {@link File} to read
	 * @param hasHeader Indicates whether the first line of the specified file 
	 * 					represents a header
	 * @param types The types corresponding to each column
	 * @throws FileNotFoundException If the file was not found or represents a directory
	 */
	public CSVFileReader(final File file, final boolean hasHeader, 
			final Class<?>... types) throws FileNotFoundException{
		
		this.file = file;
		this.hasHeader = hasHeader;
		if((types != null) && !isNullType(types[0])){
			inferColumnTypes(types);
		}
		if((!this.file.exists()) || (this.file.isDirectory())){
			throw new FileNotFoundException(String.format(
					"File %s does not exist or is a directory", file));
		}
		this.reader = new BufferedReader(new FileReader(file));
	}

	/**
	 * Reads the CSV-file and returns a DataFrame representing its content.<br>
	 * This method can only be called once. Subsequent calls will result in an
	 * <code>IOException</code>.<br>
	 * If this <code>CSVFileReader</code> was constructed to use headers, then
	 * the first line of the file is used to create the column names of the
	 * returned DataFrame.
	 * <p>Resources will be closed automatically before this method returns
	 * 
	 * @return A DataFrame holding the content of the CSV-file read
	 * @throws IOException If the file cannot be opened or read, if this
	 *                     method has already been called, or if the file 
	 *                     content is improperly formatted
	 */
	public DataFrame read() throws IOException{
		//regex which splits a string by the provided
		//separator if it is not enclosed by double quotes
		final Pattern pattern = Pattern.compile(separator
                + "(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
		
		DataFrame df = new DefaultDataFrame();
		String line = "";
		int lineIndex = 0;
		try{
			if(types != null){
				for(int i=0; i<types.length; ++i){
					df.addColumn(types[i]);
				}
				if(hasHeader){
					final String[] header = pattern.split(reader.readLine(), 0);
					for(int i=0; i<header.length; ++i){
						header[i] = normalize(header[i]);
					}
					df.setColumnNames(header);
					++lineIndex;
				}
				while((line = reader.readLine()) != null){
					++lineIndex;
					if(line.isEmpty()){//skip empty lines
						continue;
					}
					final String[] blocks = pattern.split(line, 0);
					final Object[] converted = new Object[blocks.length];
					for(int i=0; i<blocks.length; ++i){
						converted[i] = convertType(i, normalize(blocks[i]));
					}
					try{
						df.addRow(converted);
					}catch(DataFrameException ex){//null value in row
						df = DataFrame.convert(df, NullableDataFrame.class);
						df.addRow(converted);
					}
				}
			}else{
				if(hasHeader){
					final String[] header = pattern.split(reader.readLine(), 0);
					for(int i=0; i<header.length; ++i){
						header[i] = normalize(header[i]);
						df.addColumn(new StringColumn());
					}
					df.setColumnNames(header);
				}else{
					final String[] first = pattern.split(reader.readLine(), 0);
					for(int i=0; i<first.length; ++i){
						first[i] = normalize(first[i]);
						df.addColumn(new StringColumn());
					}
					df.addRow(first);
				}
				++lineIndex;
				while((line = reader.readLine()) != null){
					++lineIndex;
					if(line.isEmpty()){//skip empty lines
						continue;
					}
					final String[] blocks = pattern.split(line, 0);
					for(int i=0; i<blocks.length; ++i){
						if(blocks[i].equals("null")){
							blocks[i] = null;
						}else{
							blocks[i] = normalize(blocks[i]);
						}
					}
					try{
						df.addRow(blocks);
					}catch(DataFrameException ex){//null value in row
						df = DataFrame.convert(df, NullableDataFrame.class);
						df.addRow(blocks);
					}
				}
			}
		}catch(RuntimeException ex){
			throw new IOException(String.format(
					"Improperly formatted CSV file at line: %s", lineIndex), ex);
			
		}finally{
			this.reader.close();
		}
		return df;
	}
	
	/**
	 * Creates a background thread which will read the CSV-file and return a 
	 * DataFrame representing its content to the specified callback.<br>
	 * This method can only be called once. Subsequent calls will result in an
	 * <code>IOException</code>.<br>
	 * <p>This method is meant to be used for large CSV-files.<br>
	 * <p>When called, this method will return immediately. The result of the 
	 * background operation will be passed to the {@link ConcurrentReader} callback.
	 * <p>Please note that any IOExceptions encountered by the launched background 
	 * thread will be silently dropped. Check the DataFrame passed to the callback
	 * against null in order to spot any errors
	 * 
	 * <p>See also: {@link #read()}
	 * 
	 * @param delegate The callback for the result of this operation
	 * @throws IOException If this method has already been called
	 */
	public void parallelRead(ConcurrentReader delegate) throws IOException{
		if(parallel != null){
			throw new IOException("parallelRead() already called");
		}
		this.parallel = new ConcurrentCSVReader(delegate);
		this.parallel.execute();
	}
	
	/**
	 * Instructs this <code>CSVFileReader</code> to use the specified separator when
	 * reading the CSV-file.<br>
	 * The default separator is a comma (','). 
	 * 
	 * @param separator The character to be used as a separator
	 * @return This CSVFileReader instance
	 */
	public CSVFileReader useSeparator(final char separator){
		if(separator == '"'){
			throw new IllegalArgumentException(
					"Cannot use double quotes as separator character");
		}
		this.separator = normalizeSeparator(separator);
		return this;
	}
	
	/**
	 * Instructs this <code>CSVFileReader</code> to use the specified Charset when
	 * reading the CSV-file.<br>
	 * For example: <code>"UTF-8"</code>, which is unicode encoded as UTF-8
	 * 
	 * @param charset The charset to be used when reading
	 * @return This CSVFileReader instance
	 * @throws UnsupportedEncodingException When the specified charset is
	 * 										invalid or not supported
	 * @throws FileNotFoundException If the file was not found or represents a directory
	 */
	public CSVFileReader useCharset(final String charset)
			throws UnsupportedEncodingException, FileNotFoundException{
		
		 this.reader = new BufferedReader(
				 new InputStreamReader(new FileInputStream(this.file), charset));
		 
		 return this;
	}
	
	/**
	 * Instructs this <code>CSVFileReader</code> to use the specified types when
	 * reading the CSV-file.<br>
	 * These types will correspond to the column types of the DataFrame returned by 
	 * <code>read()</code>.
	 * <p><i>Example:</i><br>
	 * <pre><code> 
	 * myReader.useColumnTypes(Integer.class, String.class, Float.class);
	 * </code></pre>
	 * 
	 * The above code will instruct this <code>CSVFileReader</code> to treat all values
	 * in the first column of the CSV-file as integers, the second column as strings 
	 * and the third column as floats
	 * 
	 * @param types The types corresponding to each column
	 * @return This CSVFileReader instance
	 */
	public CSVFileReader useColumnTypes(Class<?>... types){
		if((types != null) && !isNullType(types[0])){
			inferColumnTypes(types);
		}else{
			this.types = null;
		}
		return this;
	}
	
	/**
	 * Converts the provided string object to the defined type
	 * 
	 * @param i The index of the column type to look up
	 * @param object The object to convert
	 * @return The converted object
	 */
	private Object convertType(final int i, final String object){
		if(object.equals("null")){
			return null;
		}
		switch(types[i].typeCode()){
		case StringColumn.TYPE_CODE:
			return object;
		case ByteColumn.TYPE_CODE:
			return Byte.valueOf(object);
		case ShortColumn.TYPE_CODE:
			return Short.valueOf(object);
		case IntColumn.TYPE_CODE:
			return Integer.valueOf(object);
		case LongColumn.TYPE_CODE:
			return Long.valueOf(object);
		case FloatColumn.TYPE_CODE:
			return Float.valueOf(object);
		case DoubleColumn.TYPE_CODE:
			return Double.valueOf(object);
		case BooleanColumn.TYPE_CODE:
			return Boolean.valueOf(object);
		case CharColumn.TYPE_CODE:
			return Character.valueOf(object.charAt(0));
		}
		return object;
	}

	/**
	 * Populates the Column array with the right column type according to the 
	 * arguments passed by the caller
	 * 
	 * @param types The types to infer
	 */
	private void inferColumnTypes(Class<?>[] types){
		this.types = new Column[types.length];
		for(int i=0; i<types.length; ++i){
			final Class<?> type = types[i];
			switch(type.getSimpleName()){
			case "String":
				this.types[i] = new StringColumn();
				break;
			case "Byte":
				this.types[i] = new ByteColumn();
				break;
			case "Short":
				this.types[i] = new ShortColumn();
				break;
			case "Integer":
				this.types[i] = new IntColumn();
				break;
			case "Long":
				this.types[i] = new LongColumn();
				break;
			case "Float":
				this.types[i] = new FloatColumn();
				break;
			case "Double":
				this.types[i] = new DoubleColumn();
				break;
			case "Boolean":
				this.types[i] = new BooleanColumn();
				break;
			case "Character":
				this.types[i] = new CharColumn();	
				break;
			default:
				throw new IllegalArgumentException("Unrecognized type: "+type);	
			}
		}
	}
	
	/**
	 * Normalizes a string by removing enclosing quotes
	 * 
	 * @param str The String to normalize 
	 * @return The normalized String
	 */
	private String normalize(final String str){
		if((str.charAt(0) == '"') && (str.charAt(str.length()-1) == '"')){
			return str.substring(1, str.length()-1);
		}
		return str;
	}
	
	/**
	 * Normalizes the separator character by escaping special regex characters
	 * 
	 * @param separator The separator charcater to normalize
	 * @return A String containing the normalized separator character
	 */
	private String normalizeSeparator(final char separator){
		switch(separator){
		case '[':
			return "\\[";
		case ']':
			return "\\]";
		case '\\':
			return "\\\\";
		case '{':
			return "\\{";
		case '}':
			return "\\}";
		case '(':
			return "\\(";
		case ')':
			return "\\)";
		case '*':
			return "\\*";
		case '+':
			return "\\+";
		case '?':
			return "\\?";
		case '.':
			return "\\.";
		case '^':
			return "\\^";
		case '$':
			return "\\$";
		case '|':
			return "\\|";
		case '#':
			return "\\#";
			default:
				return String.valueOf(separator);
		}
	}
	
	/**
	 * Indicates whether the specified type is the <code>NullType</code>
	 * 
	 * @param type The type to check
	 * @return True if the specified type is the NullType, false otherwise
	 */
	private boolean isNullType(final Class<?> type){
		return (type.getSimpleName().equals("NullType"));
	}
	
	/**
	 * Background thread for concurrent read operations of CSV-files.
	 *
	 */
	private class ConcurrentCSVReader implements Runnable {
		
		private ConcurrentReader delegate;
		
		/**
		 * Constructs a new <code>ConcurrentCSVReader</code>
		 * 
		 * @param delegate The delegate for the callback
		 */
		ConcurrentCSVReader(final ConcurrentReader delegate){
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
			DataFrame df = null;
			try{
				df = read();
			}catch(IOException ex){ }
			
			if(delegate != null){
				delegate.onRead(df);
			}
		}
	}
}
