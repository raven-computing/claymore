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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Base64;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import com.raven.common.struct.BooleanColumn;
import com.raven.common.struct.ByteColumn;
import com.raven.common.struct.CharColumn;
import com.raven.common.struct.Column;
import com.raven.common.struct.DataFrame;
import com.raven.common.struct.DefaultDataFrame;
import com.raven.common.struct.DoubleColumn;
import com.raven.common.struct.FloatColumn;
import com.raven.common.struct.IntColumn;
import com.raven.common.struct.LongColumn;
import com.raven.common.struct.NullableBooleanColumn;
import com.raven.common.struct.NullableByteColumn;
import com.raven.common.struct.NullableCharColumn;
import com.raven.common.struct.NullableDataFrame;
import com.raven.common.struct.NullableDoubleColumn;
import com.raven.common.struct.NullableFloatColumn;
import com.raven.common.struct.NullableIntColumn;
import com.raven.common.struct.NullableLongColumn;
import com.raven.common.struct.NullableShortColumn;
import com.raven.common.struct.NullableStringColumn;
import com.raven.common.struct.ShortColumn;
import com.raven.common.struct.StringColumn;

/**
 * Serializes and deserializes {@link DataFrame} instances.<br>
 * This class cannot be instantiated. All offered operations are provided
 * by static methods.
 * 
 * <p>The method {@link DataFrameSerializer#serialize(DataFrame)} will serialize 
 * the provided DataFrame to an array of bytes.
 * 
 * <p>Passing such an array to the {@link DataFrameSerializer#deserialize(byte[])} 
 * method will deserialize those bytes into a DataFrame.
 * 
 * <p>This class is also used to work with <code>.df</code> files. 
 * You can persist a <code>DataFrame</code> to a file by passing it to one of the 
 * <code>DataFrameSerializer.writeFile()</code> methods.
 * By calling one of the <code>DataFrameSerializer.readFile()</code> methods you 
 * can get the original <code>DataFrame</code> back from the file.
 * 
 * <p>Additionally, this class is also capable to serialize a <code>DataFrame</code>
 * to a <code>Base64</code> encoded string.
 * 
 * @author Phil Gaiser
 * @see CSVFileReader
 * @see CSVFileWriter
 * @since 1.0.0
 *
 */
public class DataFrameSerializer {
	
	/** The file extension used for DataFrames **/
	public static final String DF_FILE_EXTENSION = ".df";
	
	private static final byte DF_BYTE0 = 0x64;
	private static final byte DF_BYTE1 = 0x66;
	
	/** The character set used for serialization and deserialization of Strings **/
	private static final Charset UTF_8 = Charset.forName("UTF-8");

	private DataFrameSerializer(){ }
	
	/**
	 * Deserializes the given <code>Base64</code> encoded string to a DataFrame
	 * 
	 * @param string The Base64 encoded string representing the DataFrame to deserialize
	 * @return A DataFrame from the given string
	 * @throws IOException If any errors occur during deserialization
	 */
	public static DataFrame fromBase64(final String string) throws IOException{
		return deserialize(Base64.getDecoder().decode(string));
	}
	
	/**
	 * Serializes the given DataFrame to a <code>Base64</code> encoded string
	 * 
	 * @param df The DataFrame to serialize to a Base64 encoded string
	 * @return A string representing the given DataFrame
	 * @throws IOException If any errors occur during serialization
	 */
	public static String toBase64(final DataFrame df) throws IOException{
		return Base64.getEncoder().encodeToString(compress(serialize(df)));
	}
	
	/**
	 * Reads the specified file and returns a DataFrame constituted by the 
	 * content of that file
	 * 
	 * @param file The file to read. Must be a <code>.df</code> file
	 * @return A DataFrame from the specified file
	 * @throws IOException If any errors occur during deserialization or file reading
	 */
	public static DataFrame readFile(final File file) throws IOException{
		final BufferedInputStream is = new BufferedInputStream(new FileInputStream(file));
		byte[] bytes = new byte[2048];
		final ByteArrayOutputStream baos = new ByteArrayOutputStream(bytes.length);
		try{
			while((is.read(bytes, 0, bytes.length)) != -1){
				baos.write(bytes, 0, bytes.length);
			}
		}finally{
			is.close();
		}
		bytes = baos.toByteArray();
		if(bytes[0] != DF_BYTE0 || bytes[1] != DF_BYTE1){
			throw new IOException(String.format("Is not a %s file. Starts with 0x%02X 0x%02X",
					DF_FILE_EXTENSION, bytes[0], bytes[1]));
		}
		return deserialize(decompress(bytes));
	}
	
	/**
	 * Reads the specified file and returns a DataFrame constituted by the 
	 * content of that file
	 * 
	 * @param file The file to read. Must be a <code>.df</code> file
	 * @return A DataFrame from the specified file
	 * @throws IOException If any errors occur during deserialization or file reading
	 */
	public static DataFrame readFile(final String file) throws IOException{
		return readFile(new File(file));
	}
	
	/**
	 * Creates a background thread which will read the df-file and return a
	 * DataFrame to the specified callback.
	 * <p>This method is meant to be used for large df-files.<br>
	 * <p>When called, this method will return immediately. The result of the
	 * background operation will be passed to the {@link ConcurrentReader} callback.
	 * <p>Please note that any IOExceptions encountered by the launched background
	 * thread will be silently dropped. Check the DataFrame passed to the callback
	 * against null in order to spot any errors
	 * 
	 * @param file The file to read. Must be a <code>.df</code> file
	 * @param delegate The callback for the result of this operation
	 * @throws IllegalArgumentException If the file argument is null
	 */
	public static void parallelReadFile(final File file, final ConcurrentReader delegate)
			throws IllegalArgumentException{
		
		if(file == null){
			throw new IllegalArgumentException("The File argument must not be null");
		}
		final ConcurrentDFReader reader = new ConcurrentDFReader(file, delegate);
		reader.execute();
	}
	
	/**
	 * Creates a background thread which will read the df-file and return a
	 * DataFrame to the specified callback.
	 * <p>This method is meant to be used for large df-files.<br>
	 * <p>When called, this method will return immediately. The result of the
	 * background operation will be passed to the {@link ConcurrentReader} callback.
	 * <p>Please note that any IOExceptions encountered by the launched background
	 * thread will be silently dropped. Check the DataFrame passed to the callback
	 * against null in order to spot any errors
	 * 
	 * @param file The file to read. Must be a <code>.df</code> file
	 * @param delegate The callback for the result of this operation. May be null
	 * @throws IllegalArgumentException If the file argument is null
	 */
	public static void parallelReadFile(final String file, final ConcurrentReader delegate)
			throws IllegalArgumentException{
		
		parallelReadFile(new File(file), delegate);
	}
	
	/**
	 * Persists the given DataFrame to the specified file
	 * 
	 * @param file The file to write the DataFrame to
	 * @param df The DataFrame to persist. Passing null to this 
	 *           parameter will result in a NullPointerException
	 * @throws IOException If any errors occur during serialization
	 */
	public static void writeFile(File file, DataFrame df) throws IOException{
		if(!file.getName().endsWith(DF_FILE_EXTENSION)){
			file = new File(file.getAbsolutePath()+DF_FILE_EXTENSION);
		}
		final BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream(file));
		try{
			os.write(compress(serialize(df)));
		}finally{
			os.close();
		}
	}
	
	/**
	 * Persists the given DataFrame to the specified file
	 * 
	 * @param file The file to write the DataFrame to
	 * @param df The DataFrame to persist
	 * @throws IOException If any errors occur during serialization or file persistence
	 */
	public static void writeFile(String file, DataFrame df) throws IOException{
		writeFile(new File(file), df);
	}
	
	/**
	 * Creates a background thread which will persist the given DataFrame to the specified
	 * file and execute the provided callback when finished.<br>
	 * <p>This method is meant to be used for large DataFrames/df-files.<br>
	 * <p>When called, this method will return immediately. The result of the background operation
	 * will be passed to the {@link ConcurrentWriter} callback.
	 * <p>Please note that any IOExceptions encountered by the launched background thread will be
	 * silently dropped. Check the File object passed to the callback against null in
	 * order to spot any errors
	 * 
	 * @param file The file to write the DataFrame to
	 * @param df The DataFrame to persist
	 * @param delegate The callback for the result of this operation. May be null
	 * @throws IllegalArgumentException If the file or DataFrame argument is null
	 */
	public static void parallelWriteFile(File file, DataFrame df, ConcurrentWriter delegate)
			throws IllegalArgumentException{
		
		if(file == null){
			throw new IllegalArgumentException("The File argument must not be null");
		}
		if(df == null){
			throw new IllegalArgumentException("The DataFrame argument must not be null");
		}
		final ConcurrentDFWriter writer = new ConcurrentDFWriter(file, df, delegate);
		writer.execute();
	}
	
	/**
	 * Creates a background thread which will persist the given DataFrame to the specified
	 * file and execute the provided callback when finished.<br>
	 * <p>This method is meant to be used for large DataFrames/df-files.<br>
	 * <p>When called, this method will return immediately. The result of the background operation 
	 * will be passed to the {@link ConcurrentWriter} callback.
	 * <p>Please note that any IOExceptions encountered by the launched background thread will be 
	 * silently dropped. Check the File object passed to the callback against null in
	 * order to spot any errors
	 * 
	 * @param file The file to write the DataFrame to
	 * @param df The DataFrame to persist
	 * @param delegate The callback for the result of this operation. May be null
	 * @throws IllegalArgumentException If the file or DataFrame argument is null
	 */
	public static void parallelWriteFile(String file, DataFrame df, ConcurrentWriter delegate) 
			throws IllegalArgumentException{
		
		parallelWriteFile(new File(file), df, delegate);
	}
	
	/**
	 * Serializes the given <code>DataFrame</code> to an array of bytes.<br>
	 * The returned array is not compressed. The compression of the returned array can be 
	 * controlled by passing an additional boolean flag to the arguments.<br>
	 * See {@link DataFrameSerializer#serialize(DataFrame, boolean)}
	 * 
	 * @param df The DataFrame to serialize
	 * @return A byte array representing the given DataFrame
	 * @throws IOException If any errors occur during serialization
	 */
	public static byte[] serialize(final DataFrame df) throws IOException{
		return serialize(df, false);
	}
	
	/**
	 * Serializes the given <code>DataFrame</code> to an array of bytes.<br>
	 * The compression of the returned array is controlled by the additional boolean 
	 * flag of this method.
	 * 
	 * @param df The DataFrame to serialize
	 * @param compress A boolean flag indicating whether to compress the serialized bytes
	 * @return A byte array representing the given DataFrame,,
	 * @throws IOException If any errors occur during serialization or compression
	 */
	public static byte[] serialize(final DataFrame df, final boolean compress) throws IOException{
		try{
			return (compress ? compress(serializeImplv2(df)) : serializeImplv2(df));
		}catch(RuntimeException ex){
			//catch any unchecked runtime exception which at this point can
			//only be caused by improper or malicious usage of the DataFrame API
			throw new IOException("Serialization failed due to an invalid DataFrame format");
		}
	}
	
	/**
	 * Deserializes the given array of bytes to a <code>DataFrame</code>.
	 * 
	 * <p>If the given byte array is compressed, it will be automatically decompressed before
	 * the deserialization is executed. The byte array of the provided reference may be 
	 * affected by this operation as long as decompression is in process. The original state,
	 * however, will be restored after decompression. This approach helps avoid additional copy
	 * operations and should be considered when writing multi-threaded code that uses
	 * deserialization as provided by this method.
	 * 
	 * <p>Deserialization of uncompressed arrays does only require read access and therefore will
	 * never alter the content of the provided array
	 * 
	 * @param bytes The byte array representing the DataFrame to deserialize
	 * @return A DataFrame from the given array of bytes
	 * @throws IOException If any errors occur during deserialization or decompression, or if 
	 *                     the given byte array does not constitute a DataFrame
	 */
	public static DataFrame deserialize(byte[] bytes) throws IOException{
		try{
			if((bytes[0] == DF_BYTE0) && (bytes[1] == DF_BYTE1)){
				bytes = decompress(bytes);
			}
			//validate the first bytes of the header and the used format version
			//must start with '{v:'
			if((bytes[0] != 0x7b) || (bytes[1] != 0x76) || (bytes[2] != 0x3a)
                    || ((bytes[3] != 0x32) && (bytes[3] != 0x31))){//version 2 and 1 supported
				
				throw new IOException(String.format("Unsupported encoding (v:%s)",
						((char)bytes[3])));
			}
			return ((bytes[3] == 0x32)//is version 2
                       ? deserializeImplv2(bytes) 
                       : deserializeImplv1(bytes));
			
		}catch(RuntimeException ex){
			//catch any unchecked runtime exception which at
			//this point can only be caused by an invalid format
			throw new IOException("Deserialization failed due to an invalid DataFrame format");
		}
	}
	
	/**
	 * Serialization to the binary-based <b>version 2</b> format (v2).<br>
	 * 
	 * @param df The DataFrame to serialize
	 * @return A byte array representing the given DataFrame
	 * @throws IOException If any errors occur during serialization
	 */
	private static byte[] serializeImplv2(final DataFrame df) throws IOException{
		int ptr = 4;//place on write ready position
		byte[] bytes = new byte[2048];
		//HEADER
		//must start with {v:2;
		bytes[0] = 0x7b; bytes[1] = 0x76; bytes[2] = 0x3a;
		bytes[3] = 0x32; bytes[4] = 0x3b;
		
		//impl: default=0x64 nullable=0x6e
		bytes[++ptr] = (df.isNullable() ? (byte)0x6e : (byte)0x64);
		
		final int rows = df.rows();
		bytes[++ptr] = (byte) ((rows & 0xff000000) >> 24);
		bytes[++ptr] = (byte) ((rows & 0xff0000) >> 16);
		bytes[++ptr] = (byte) ((rows & 0xff00) >> 8);
		bytes[++ptr] = (byte)  (rows & 0xff);
		
		final int cols = df.columns();
		bytes[++ptr] = (byte) ((cols & 0xff000000) >> 24);
		bytes[++ptr] = (byte) ((cols & 0xff0000) >> 16);
		bytes[++ptr] = (byte) ((cols & 0xff00) >> 8);
		bytes[++ptr] = (byte)  (cols & 0xff);
		
		if(df.hasColumnNames()){
			for(final String name : df.getColumnNames()){
				bytes = ensureCapacity(bytes, ptr+name.length()+2);
				for(final byte b : name.getBytes(UTF_8)){
					bytes[++ptr] = b;
				}
				++ptr;//add null character as name delimeter
			}
		}else{
			//set indices as strings
			for(int i=0; i<cols; ++i){
				final String name = String.valueOf(i);
				bytes = ensureCapacity(bytes, ptr+name.length()+2);
				for(final byte b : name.getBytes(UTF_8)){
					bytes[++ptr] = b;
				}
				++ptr;
			}
		}
		bytes = ensureCapacity(bytes, ptr+cols+4);
		for(final Column col : df){
			bytes[++ptr] = col.typeCode();
		}
		
		
		if(df.isNullable()){//NullableDataFrame
			//The specification requires a lookup list for differentiating between 
			//default values (for example: zeros for numbers) and actual null values.
			//This is implemented here as a bit vector initialized with all bits
			//set to zero. The resizing and bounds checks are also handled here to
			//avoid additional function calls.
			//As the lookup list is part of the header, we must first serialize the
			//entire payload and build the lookup list and then bind all the parts
			//together at the end
			final byte[] header = copyBytes(bytes, 0, ptr+1);
			bytes = new byte[2048];
			ptr = -1;//reset index pointer
			
			//the lookup list
			byte[] lookupBits = new byte[512];
			//index pointer pointing to the current valid byte block
			//for write operations to the lookup list
			int ptrB = 0;
			//list index pointing to the next writable bit within the lookup list
			long li = 0l;
			//PAYLOAD
			for(final Column col : df){
				switch(col.typeCode()){
				case NullableByteColumn.TYPE_CODE:{
					bytes = ensureCapacity(bytes, ptr+rows+2);
					final Byte[] val = ((NullableByteColumn)col).asArray();
					for(int i=0; i<rows; ++i){
						if(val[i] == null){
							++ptr;
					        if(ptrB >= lookupBits.length){
					            lookupBits = resize(lookupBits, ptrB);
					        }
					        lookupBits[ptrB] |= (1 << (7-(li%8L)));
							ptrB = (int) ((++li)/8L);
						}else if(val[i] == 0){
							++ptr;
							ptrB = (int) ((++li)/8L);
						}else{
							bytes[++ptr] = val[i];
						}
					}
					break;
				}case NullableShortColumn.TYPE_CODE:{
					bytes = ensureCapacity(bytes, ptr+(rows*2)+2);
					final Short[] val = ((NullableShortColumn)col).asArray();
					for(int i=0; i<rows; ++i){
						if(val[i] == null){
							ptr += 2;
					        if(ptrB >= lookupBits.length){
					            lookupBits = resize(lookupBits, ptrB);
					        }
							lookupBits[ptrB] |= (1 << (7-(li%8L)));
							ptrB = (int) ((++li)/8L);
						}else if(val[i] == 0){
							ptr += 2;
							ptrB = (int) ((++li)/8L);
						}else{
							bytes[++ptr] = (byte) ((val[i] & 0xff00) >> 8);
							bytes[++ptr] = (byte)  (val[i] & 0xff);
						}
					}
					break;
				}case NullableIntColumn.TYPE_CODE:{
					bytes = ensureCapacity(bytes, ptr+(rows*4)+2);
					final Integer[] val = ((NullableIntColumn)col).asArray();
					for(int i=0; i<rows; ++i){
						if(val[i] == null){
							ptr += 4;
					        if(ptrB >= lookupBits.length){
					            lookupBits = resize(lookupBits, ptrB);
					        }
					        lookupBits[ptrB] |= (1 << (7-(li%8L)));
							ptrB = (int) ((++li)/8L);
						}else if(val[i] == 0){
							ptr += 4;
							ptrB = (int) ((++li)/8L);
						}else{
							bytes[++ptr] = (byte) ((val[i] & 0xff000000) >> 24);
							bytes[++ptr] = (byte) ((val[i] & 0xff0000) >> 16);
							bytes[++ptr] = (byte) ((val[i] & 0xff00) >> 8);
							bytes[++ptr] = (byte)  (val[i] & 0xff);
						}
					}
					break;
				}case NullableLongColumn.TYPE_CODE:{
					bytes = ensureCapacity(bytes, ptr+(rows*8)+2);
					final Long[] val = ((NullableLongColumn)col).asArray();
					for(int i=0; i<rows; ++i){
						if(val[i] == null){
							ptr += 8;
					        if(ptrB >= lookupBits.length){
					            lookupBits = resize(lookupBits, ptrB);
					        }
					        lookupBits[ptrB] |= (1 << (7-(li%8L)));
							ptrB = (int) ((++li)/8L);
						}else if(val[i] == 0){
							ptr += 8;
							ptrB = (int) ((++li)/8L);
						}else{
							bytes[++ptr] = (byte) ((val[i] & 0xff00000000000000L) >> 56);
							bytes[++ptr] = (byte) ((val[i] & 0xff000000000000L) >> 48);
							bytes[++ptr] = (byte) ((val[i] & 0xff0000000000L) >> 40);
							bytes[++ptr] = (byte) ((val[i] & 0xff00000000L) >> 32);
							bytes[++ptr] = (byte) ((val[i] & 0xff000000L) >> 24);
							bytes[++ptr] = (byte) ((val[i] & 0xff0000L) >> 16);
							bytes[++ptr] = (byte) ((val[i] & 0xff00L) >> 8);
							bytes[++ptr] = (byte)  (val[i] & 0xffL);
						}
					}
					break;
				}case NullableStringColumn.TYPE_CODE:{
					final String[] val = ((NullableStringColumn)col).asArray();
					for(int i=0; i<rows; ++i){
						if(val[i] == null){
							bytes = ensureCapacity(bytes, ptr+1);
					        if(ptrB >= lookupBits.length){
					            lookupBits = resize(lookupBits, ptrB);
					        }
					        lookupBits[ptrB] |= (1 << (7-(li%8L)));
							ptrB = (int) ((++li)/8L);
						}else if(val[i].isEmpty()){
							bytes = ensureCapacity(bytes, ptr+1);
							ptrB = (int) ((++li)/8L);
						}else{
							final byte[] b = val[i].getBytes(UTF_8);
							final int length = b.length;
							bytes = ensureCapacity(bytes, ptr+length+2);
							for(int j=0; j<length; ++j){
								bytes[++ptr] = b[j];
							}
						}
						++ptr;//add null character as string delimeter
					}
					break;
				}case NullableFloatColumn.TYPE_CODE:{
					bytes = ensureCapacity(bytes, ptr+(rows*4)+2);
					final Float[] val = ((NullableFloatColumn)col).asArray();
					for(int i=0; i<rows; ++i){
						if(val[i] == null){
							ptr += 4;
					        if(ptrB >= lookupBits.length){
					            lookupBits = resize(lookupBits, ptrB);
					        }
					        lookupBits[ptrB] |= (1 << (7-(li%8L)));
							ptrB = (int) ((++li)/8L);
						}else{
							final int f = Float.floatToIntBits(val[i]);
							if(f == 0){
								ptr += 4;
								ptrB = (int) ((++li)/8L);
							}else{
								bytes[++ptr] = (byte) ((f & 0xff000000) >> 24);
								bytes[++ptr] = (byte) ((f & 0xff0000) >> 16);
								bytes[++ptr] = (byte) ((f & 0xff00) >> 8);
								bytes[++ptr] = (byte)  (f & 0xff);
							}
						}
					}
					break;
				}case NullableDoubleColumn.TYPE_CODE:{
					bytes = ensureCapacity(bytes, ptr+(rows*8)+2);
					final Double[] val = ((NullableDoubleColumn)col).asArray();
					for(int i=0; i<rows; ++i){
						if(val[i] == null){
							ptr += 8;
					        if(ptrB >= lookupBits.length){
					            lookupBits = resize(lookupBits, ptrB);
					        }
					        lookupBits[ptrB] |= (1 << (7-(li%8L)));
							ptrB = (int) ((++li)/8L);
						}else{
							final long f = Double.doubleToLongBits(val[i]);
							if(f == 0){
								ptr += 8;
								ptrB = (int) ((++li)/8L);
							}else{
								bytes[++ptr] = (byte) ((f & 0xff00000000000000L) >> 56);
								bytes[++ptr] = (byte) ((f & 0xff000000000000L) >> 48);
								bytes[++ptr] = (byte) ((f & 0xff0000000000L) >> 40);
								bytes[++ptr] = (byte) ((f & 0xff00000000L) >> 32);
								bytes[++ptr] = (byte) ((f & 0xff000000L) >> 24);
								bytes[++ptr] = (byte) ((f & 0xff0000L) >> 16);
								bytes[++ptr] = (byte) ((f & 0xff00L) >> 8);
								bytes[++ptr] = (byte)  (f & 0xffL);
							}
						}
					}
					break;
				}case NullableCharColumn.TYPE_CODE:{
					bytes = ensureCapacity(bytes, ptr+(rows*2)+2);
					final Character[] val = ((NullableCharColumn)col).asArray();
					for(int i=0; i<rows; ++i){
						if(val[i] == null){
							ptr += 2;
					        if(ptrB >= lookupBits.length){
					            lookupBits = resize(lookupBits, ptrB);
					        }
					        lookupBits[ptrB] |= (1 << (7-(li%8L)));
							ptrB = (int) ((++li)/8L);
						}else{
							final char c = val[i].charValue();
							bytes[++ptr] = (byte) ((c & 0xff00) >> 8);
							bytes[++ptr] = (byte)  (c & 0xff);
							if(c == '\u0000'){
								ptrB = (int) ((++li)/8L);
							}
						}
					}
					break;
				}case NullableBooleanColumn.TYPE_CODE:{
					final int length = ((rows%8==0) ? (rows/8) : ((rows/8)+1));
					bytes = ensureCapacity(bytes, ptr+length+2);
					final Boolean[] val = ((NullableBooleanColumn)col).asArray();
					++ptr;//focus on next writable position
					int ptrBoolB = 0;
					long boolLi = 0L;
					for(int i=0; i<rows; ++i){
						if(val[i] == null){
							ptrBoolB = (int) ((++boolLi)/8L);
					        if(ptrB >= lookupBits.length){
					            lookupBits = resize(lookupBits, ptrB);
					        }
					        lookupBits[ptrB] |= (1 << (7-(li%8L)));
							ptrB = (int) ((++li)/8L);
						}else if(val[i] == false){
							ptrBoolB = (int) ((++boolLi)/8L);
							ptrB = (int) ((++li)/8l);
						}else{
							bytes[ptr+ptrBoolB] |= (1 << (7-(boolLi%8L)));
							ptrBoolB = (int) ((++boolLi)/8L);
						}
					}
					//let the base pointer jump forward to the last written byte
					ptr += (length-1);
					break;
				  }
				}
			}//END PAYLOAD
			//copy operations to stick everything together
			
			//make sure to include all valid blocks in the lookup list
	        if(ptrB >= lookupBits.length){
	            lookupBits = resize(lookupBits, ptrB);
	        }
			final byte[] payload = bytes;
			final int payloadLength = ptr+1;
			ptr = -1;
			//allocate bytes for the final result
			bytes = new byte[header.length+ptrB+6+payloadLength];
			for(int i=0; i<header.length; ++i){
				bytes[++ptr] = header[i];
			}
			//Number of byte blocks of the lookup list.
			//The specification requires that the lookup
			//list has a minimum length of one block
			final int bLength = (int) (((li-1)/8L)+1);
			bytes[++ptr] = (byte) ((bLength & 0xff000000) >> 24);
			bytes[++ptr] = (byte) ((bLength & 0xff0000) >> 16);
			bytes[++ptr] = (byte) ((bLength & 0xff00) >> 8);
			bytes[++ptr] = (byte)  (bLength & 0xff);
			//copy lookup bits
			for(int i=0; i<bLength; ++i){
				bytes[++ptr] = lookupBits[i];
			}
			//add header closing brace '}'
			bytes[++ptr] = 0x7d;
			//copy payload bytes
			for(int i=0; i<payloadLength; ++i){
				bytes[++ptr] = payload[i];
			}
			
		}else{//DefaultDataFrame
			bytes[++ptr] = 0x7d;//add header closing brace '}'
			//END HEADER
			//As DefaultDataFrames do not have null values, no lookup list
			//is required and we just serialize all bytes as they are to
			//the payload section
			//PAYLOAD
			for(final Column col : df){
				switch(col.typeCode()){
				case ByteColumn.TYPE_CODE:{
					bytes = ensureCapacity(bytes, ptr+rows+2);
					final byte[] val = ((ByteColumn)col).asArray();
					for(int i=0; i<rows; ++i){
						bytes[++ptr] = val[i];
					}
					break;
				}case ShortColumn.TYPE_CODE:{
					bytes = ensureCapacity(bytes, ptr+(rows*2)+2);
					final short[] val = ((ShortColumn)col).asArray();
					for(int i=0; i<rows; ++i){
						bytes[++ptr] = (byte) ((val[i] & 0xff00) >> 8);
						bytes[++ptr] = (byte)  (val[i] & 0xff);
					}
					break;
				}case IntColumn.TYPE_CODE:{
					bytes = ensureCapacity(bytes, ptr+(rows*4)+2);
					final int[] val = ((IntColumn)col).asArray();
					for(int i=0; i<rows; ++i){
						bytes[++ptr] = (byte) ((val[i] & 0xff000000) >> 24);
						bytes[++ptr] = (byte) ((val[i] & 0xff0000) >> 16);
						bytes[++ptr] = (byte) ((val[i] & 0xff00) >> 8);
						bytes[++ptr] = (byte)  (val[i] & 0xff);
					}
					break;
				}case LongColumn.TYPE_CODE:{
					bytes = ensureCapacity(bytes, ptr+(rows*8)+2);
					final long[] val = ((LongColumn)col).asArray();
					for(int i=0; i<rows; ++i){
						bytes[++ptr] = (byte) ((val[i] & 0xff00000000000000L) >> 56);
						bytes[++ptr] = (byte) ((val[i] & 0xff000000000000L) >> 48);
						bytes[++ptr] = (byte) ((val[i] & 0xff0000000000L) >> 40);
						bytes[++ptr] = (byte) ((val[i] & 0xff00000000L) >> 32);
						bytes[++ptr] = (byte) ((val[i] & 0xff000000L) >> 24);
						bytes[++ptr] = (byte) ((val[i] & 0xff0000L) >> 16);
						bytes[++ptr] = (byte) ((val[i] & 0xff00L) >> 8);
						bytes[++ptr] = (byte)  (val[i] & 0xffL);
					}
					break;
				}case StringColumn.TYPE_CODE:{
					final String[] val = ((StringColumn)col).asArray();
					for(int i=0; i<rows; ++i){
						final byte[] b = val[i].getBytes(UTF_8);
						final int length = b.length;
						bytes = ensureCapacity(bytes, ptr+length+2);
						for(int j=0; j<length; ++j){
							bytes[++ptr] = b[j];
						}
						++ptr;//add null character as string delimeter
					}
					break;
				}case FloatColumn.TYPE_CODE:{
					bytes = ensureCapacity(bytes, ptr+(rows*4)+2);
					final float[] val = ((FloatColumn)col).asArray();
					for(int i=0; i<rows; ++i){
						final int f = Float.floatToIntBits(val[i]);
						bytes[++ptr] = (byte) ((f & 0xff000000) >> 24);
						bytes[++ptr] = (byte) ((f & 0xff0000) >> 16);
						bytes[++ptr] = (byte) ((f & 0xff00) >> 8);
						bytes[++ptr] = (byte)  (f & 0xff);
					}
					break;
				}case DoubleColumn.TYPE_CODE:{
					bytes = ensureCapacity(bytes, ptr+(rows*8)+2);
					final double[] val = ((DoubleColumn)col).asArray();
					for(int i=0; i<rows; ++i){
						long d = Double.doubleToLongBits(val[i]);
						bytes[++ptr] = (byte) ((d & 0xff00000000000000L) >> 56);
						bytes[++ptr] = (byte) ((d & 0xff000000000000L) >> 48);
						bytes[++ptr] = (byte) ((d & 0xff0000000000L) >> 40);
						bytes[++ptr] = (byte) ((d & 0xff00000000L) >> 32);
						bytes[++ptr] = (byte) ((d & 0xff000000L) >> 24);
						bytes[++ptr] = (byte) ((d & 0xff0000L) >> 16);
						bytes[++ptr] = (byte) ((d & 0xff00L) >> 8);
						bytes[++ptr] = (byte)  (d & 0xffL);
					}
					break;
				}case CharColumn.TYPE_CODE:{
					bytes = ensureCapacity(bytes, ptr+(rows*2)+2);
					final char[] val = ((CharColumn)col).asArray();
					for(int i=0; i<rows; ++i){
						bytes[++ptr] = (byte) ((val[i] & 0xff00) >> 8);
						bytes[++ptr] = (byte)  (val[i] & 0xff);
					}
					break;
				}case BooleanColumn.TYPE_CODE:{
					final int length = ((rows%8==0) ? (rows/8) : ((rows/8)+1));
					bytes = ensureCapacity(bytes, ptr+length+2);
					final boolean[] val = ((BooleanColumn)col).asArray();
					++ptr;//focus on next writable position
					final int b0 = ptr;//cache first position as constant
					int ptrBoolB = 0;
					long boolLi = 0l;
					for(int i=0; i<rows; ++i){
						if(val[i] == true){
							bytes[b0+ptrBoolB] |= (1 << (7-(boolLi%8l)));
						}
						ptrBoolB = (int) ((++boolLi)/8l);
					}
					//let the base pointer jump forward to the last written byte
					ptr += (length-1);
					break;
				  }
				}
			}//END PAYLOAD
			bytes = copyBytes(bytes, 0, ptr+1);//trim operation
		}
		return bytes;
	}

	/**
	 * Deserialization from the binary-based <b>version 2</b> format (v2).<br>
	 * 
	 * @param bytes The byte array representing the DataFrame to deserialize
	 * @return A DataFrame from the given array of bytes
	 * @throws IOException If any errors occur during deserialization
	 */
	private static DataFrame deserializeImplv2(final byte[] bytes) throws IOException{
		//HEADER
		int ptr = 5;//first bytes have already been validated at this point
		final byte dfType = bytes[ptr];
		if((dfType != 0x64) && (dfType != 0x6e)){
			throw new IOException("Unsupported DataFrame implementation");	
		}
		//header format is {v:2;irrrrccccName1.Name2.ttllllbbb}0x...
		
		//code of the DataFrame implementation
		final boolean implDefault = (dfType == 0x64);
        final int rows = ((bytes[++ptr] & 0xff) << 24
                        | (bytes[++ptr] & 0xff) << 16
                        | (bytes[++ptr] & 0xff) << 8
                        | (bytes[++ptr] & 0xff));
		
        final int cols = ((bytes[++ptr] & 0xff) << 24
                        | (bytes[++ptr] & 0xff) << 16
                        | (bytes[++ptr] & 0xff) << 8
                        | (bytes[++ptr] & 0xff));
        
        //column labels
		final String[] names = new String[cols];
		for(int i=0; i<cols; ++i){
			final int c0 = ptr+1;//first char
			while(bytes[++ptr] != 0);
			names[i] = new String(copyBytes(bytes, c0, ptr), UTF_8);
		}
		//column types
		final byte[] types = new byte[cols];
		for(int i=0; i<cols; ++i){
			types[i] = bytes[++ptr];
		}
		DataFrame df = null;
		final Column[] columns = new Column[cols];
		if(!implDefault){//NullableDataFrame
			//first read the entire lookup list into memory
			final int lookupLength = ((bytes[++ptr] & 0xff) << 24
                                    | (bytes[++ptr] & 0xff) << 16
                                    | (bytes[++ptr] & 0xff) << 8
                                    | (bytes[++ptr] & 0xff));
			
	        final byte[] lookupBits = new byte[lookupLength];
	        for(int i=0; i<lookupLength; ++i){
	        	lookupBits[i] = bytes[++ptr];
	        }
			//index pointer pointing to the current valid byte block
			//for read operations from the lookup list
			int ptrB = 0;
			//list index pointing to the next readable bit within the lookup list
			long li = 0l;
	        if(bytes[++ptr] != 0x7d){//header closing brace '}' missing
	        	throw new IOException("Invalid format");
	        }
	        //END HEADER
	        
	        //PAYLOAD
	        for(int i=0; i<cols; ++i){
	        	switch(types[i]){
				case NullableByteColumn.TYPE_CODE:{
					final Byte[] val = new Byte[rows];
					for(int j=0; j<rows; ++j){
						final byte b = bytes[++ptr];
						if(b == 0){
					        if((lookupBits[ptrB] & (1 << (7-(li%8L)))) == 0){
					        	val[j] = 0;
					        }
							ptrB = (int) ((++li)/8L);
						}else{
							val[j] = b;
						}
					}
					columns[i] = new NullableByteColumn(val);
					break;
				}case NullableShortColumn.TYPE_CODE:{
					final Short[] val = new Short[rows];
					for(int j=0; j<rows; ++j){
						final short s = (short) (((bytes[++ptr] & 0xff) << 8) 
                                                | (bytes[++ptr] & 0xff));
						
						if(s == 0){
					        if((lookupBits[ptrB] & (1 << (7-(li%8L)))) == 0){
					        	val[j] = 0;
					        }
							ptrB = (int) ((++li)/8L);
						}else{
							val[j] = s;
						}
					}
					columns[i] = new NullableShortColumn(val);
					break;
				}case NullableIntColumn.TYPE_CODE:{
					final Integer[] val = new Integer[rows];
					for(int j=0; j<rows; ++j){
						final int in = (((bytes[++ptr] & 0xff) << 24) 
                                      | ((bytes[++ptr] & 0xff) << 16) 
								      | ((bytes[++ptr] & 0xff) << 8) 
                                      |  (bytes[++ptr] & 0xff));
						
						if(in == 0){
					        if((lookupBits[ptrB] & (1 << (7-(li%8L)))) == 0){
					        	val[j] = 0;
					        }
							ptrB = (int) ((++li)/8L);
						}else{
							val[j] = in;
						}
					}
					columns[i] = new NullableIntColumn(val);
					break;
				}case NullableLongColumn.TYPE_CODE:{
					final Long[] val = new Long[rows];
					for(int j=0; j<rows; ++j){						
						final long l = (((bytes[++ptr] & 0xffL) << 56)
                                      | ((bytes[++ptr] & 0xffL) << 48)
                                      | ((bytes[++ptr] & 0xffL) << 40)
                                      | ((bytes[++ptr] & 0xffL) << 32)
                                      | ((bytes[++ptr] & 0xffL) << 24) 
                                      | ((bytes[++ptr] & 0xffL) << 16) 
                                      | ((bytes[++ptr] & 0xffL) << 8) 
                                      |  (bytes[++ptr] & 0xffL));
						
						if(l == 0){
					        if((lookupBits[ptrB] & (1 << (7-(li%8L)))) == 0){
					        	val[j] = 0L;
					        }
							ptrB = (int) ((++li)/8L);
						}else{
							val[j] = l;
						}
					}
					columns[i] = new NullableLongColumn(val);
					break;
				}case NullableStringColumn.TYPE_CODE:{
					final String[] val = new String[rows];
					for(int j=0; j<rows; ++j){
						int c0 = ptr+1;//marks the first character of each string
						while(bytes[++ptr] != 0);
						if((ptr-c0) == 0){
					        if((lookupBits[ptrB] & (1 << (7-(li%8L)))) == 0){
					        	val[j] = "";
					        }
							ptrB = (int) ((++li)/8L);
						}else{
							val[j] = new String(copyBytes(bytes, c0, ptr), UTF_8);
						}
					}
					columns[i] = new NullableStringColumn(val);
					break;
				}case NullableFloatColumn.TYPE_CODE:{
					final Float[] val = new Float[rows];
					for(int j=0; j<rows; ++j){
						final float f = Float.intBitsToFloat(
								 (((bytes[++ptr] & 0xff) << 24) 
                                | ((bytes[++ptr] & 0xff) << 16) 
                                | ((bytes[++ptr] & 0xff) << 8) 
                                |  (bytes[++ptr] & 0xff)));
						
						if(f == 0.0f){
					        if((lookupBits[ptrB] & (1 << (7-(li%8L)))) == 0){
					        	val[j] = 0.0f;
					        }
							ptrB = (int) ((++li)/8L);
						}else{
							val[j] = f;
						}
					}
					columns[i] = new NullableFloatColumn(val);
					break;
				}case NullableDoubleColumn.TYPE_CODE:{
					final Double[] val = new Double[rows];
					for(int j=0; j<rows; ++j){
						final double d = Double.longBitsToDouble(
								 (((bytes[++ptr] & 0xffL) << 56)
                                | ((bytes[++ptr] & 0xffL) << 48)
                                | ((bytes[++ptr] & 0xffL) << 40)
                                | ((bytes[++ptr] & 0xffL) << 32)
                                | ((bytes[++ptr] & 0xffL) << 24) 
                                | ((bytes[++ptr] & 0xffL) << 16) 
                                | ((bytes[++ptr] & 0xffL) << 8) 
                                |  (bytes[++ptr] & 0xffL)));
						
						if(d == 0.0){
					        if((lookupBits[ptrB] & (1 << (7-(li%8L)))) == 0){
					        	val[j] = 0.0;
					        }
							ptrB = (int) ((++li)/8L);
						}else{
							val[j] = d;
						}
					}
					columns[i] = new NullableDoubleColumn(val);
					break;
				}case NullableCharColumn.TYPE_CODE:{
					final Character[] val = new Character[rows];
					for(int j=0; j<rows; ++j){
						final short s = (short) (((bytes[++ptr] & 0xff) << 8) 
                                                | (bytes[++ptr] & 0xff));
						
						if(s == 0){
					        if((lookupBits[ptrB] & (1 << (7-(li%8L)))) == 0){
					        	val[j] = '\u0000';
					        }
							ptrB = (int) ((++li)/8L);
						}else{
							val[j] = (char)s;
						}
					}
					columns[i] = new NullableCharColumn(val);
					break;
				}case NullableBooleanColumn.TYPE_CODE:{
					final int length = ((rows%8==0) ? (rows/8) : ((rows/8)+1));
					final Boolean[] val = new Boolean[rows];
					++ptr;//focus on next writable position
					final int b0 = ptr;//cache first position as constant
					int ptrBoolB = 0;
					long boolLi = 0l;
					for(int j=0; j<rows; ++j){
						if((bytes[b0+ptrBoolB] & (1 << (7-(boolLi%8L)))) == 0){
					        if((lookupBits[ptrB] & (1 << (7-(li%8L)))) == 0){
					        	val[j] = false;
					        }
							ptrB = (int) ((++li)/8L);
						}else{
							val[j] = true;
						}
						ptrBoolB = (int) ((++boolLi)/8L);
					}
					//let the base pointer jump forward to the last written byte
					ptr += (length-1);
					columns[i] = new NullableBooleanColumn(val);
					break;
				  }
	        	}
	        }//END PAYLOAD
	        if(cols == 0){//uninitialized instance
	        	df = new NullableDataFrame();
	        }else{
	        	df = new NullableDataFrame(names, columns);
	        }
		}else{//DefaultDataFrame
	        if(bytes[++ptr] != 0x7d){//header closing brace '}'
	        	throw new IOException("Invalid format");
	        }
	        //END HEADER
	        
	        //PAYLOAD
	        for(int i=0; i<cols; ++i){
	        	switch(types[i]){
				case ByteColumn.TYPE_CODE:{
					final byte[] val = new byte[rows];
					for(int j=0; j<rows; ++j){
						val[j] = bytes[++ptr];
					}
					columns[i] = new ByteColumn(val);
					break;
				}case ShortColumn.TYPE_CODE:{
					final short[] val = new short[rows];
					for(int j=0; j<rows; ++j){
						val[j] = (short) (((bytes[++ptr] & 0xff) << 8) 
                                         | (bytes[++ptr] & 0xff));
						
					}
					columns[i] = new ShortColumn(val);
					break;
				}case IntColumn.TYPE_CODE:{
					final int[] val = new int[rows];
					for(int j=0; j<rows; ++j){
						val[j] = (((bytes[++ptr] & 0xff) << 24) 
                                | ((bytes[++ptr] & 0xff) << 16) 
                                | ((bytes[++ptr] & 0xff) << 8) 
                                |  (bytes[++ptr] & 0xff));
						
					}
					columns[i] = new IntColumn(val);
					break;
				}case LongColumn.TYPE_CODE:{
					final long[] val = new long[rows];
					for(int j=0; j<rows; ++j){
						val[j] = (((bytes[++ptr] & 0xffL) << 56)
                                | ((bytes[++ptr] & 0xffL) << 48)
                                | ((bytes[++ptr] & 0xffL) << 40)
                                | ((bytes[++ptr] & 0xffL) << 32)
                                | ((bytes[++ptr] & 0xffL) << 24) 
                                | ((bytes[++ptr] & 0xffL) << 16) 
                                | ((bytes[++ptr] & 0xffL) << 8) 
                                |  (bytes[++ptr] & 0xffL));
						
					}
					columns[i] = new LongColumn(val);
					break;
				}case StringColumn.TYPE_CODE:{
					final String[] val = new String[rows];
					for(int j=0; j<rows; ++j){
						int c0 = ptr+1;
						while(bytes[++ptr] != 0);
						if((ptr-c0) == 0){
					        val[j] = StringColumn.PLACEHOLDER_EMPTY;
						}else{
							val[j] = new String(copyBytes(bytes, c0, ptr), UTF_8);
						}
						c0 = ptr+1;
					}
					columns[i] = new StringColumn(val);
					break;
				}case FloatColumn.TYPE_CODE:{
					final float[] val = new float[rows];
					for(int j=0; j<rows; ++j){
						val[j] = Float.intBitsToFloat(
								(((bytes[++ptr] & 0xff) << 24)
                               | ((bytes[++ptr] & 0xff) << 16) 
                               | ((bytes[++ptr] & 0xff) << 8) 
                               |  (bytes[++ptr] & 0xff)));
					}
					columns[i] = new FloatColumn(val);
					break;
				}case DoubleColumn.TYPE_CODE:{
					final double[] val = new double[rows];
					for(int j=0; j<rows; ++j){						
						val[j] = Double.longBitsToDouble(
								(((bytes[++ptr] & 0xffL) << 56)
                               | ((bytes[++ptr] & 0xffL) << 48)
                               | ((bytes[++ptr] & 0xffL) << 40)
                               | ((bytes[++ptr] & 0xffL) << 32)
                               | ((bytes[++ptr] & 0xffL) << 24) 
                               | ((bytes[++ptr] & 0xffL) << 16) 
                               | ((bytes[++ptr] & 0xffL) << 8) 
                               |  (bytes[++ptr] & 0xffL)));
					}
					columns[i] = new DoubleColumn(val);
					break;
				}case CharColumn.TYPE_CODE:{
					final char[] val = new char[rows];
					for(int j=0; j<rows; ++j){
						val[j] = (char) ((short) (((bytes[++ptr] & 0xff) << 8) 
                                                 | (bytes[++ptr] & 0xff)));
						
					}
					columns[i] = new CharColumn(val);
					break;
				}case BooleanColumn.TYPE_CODE:{
					final int length = ((rows%8==0) ? (rows/8) : ((rows/8)+1));
					final boolean[] val = new boolean[rows];
					++ptr;//focus on next writable position
					final int b0 = ptr;//cache first position as constant
					int ptrBoolB = 0;
					long boolLi = 0l;
					for(int j=0; j<rows; ++j){
						val[j] = ((bytes[b0+ptrBoolB] & (1 << (7-(boolLi%8L)))) != 0);
						ptrBoolB = (int) ((++boolLi)/8L);
					}
					//let the base pointer jump forward to the last written byte
					ptr += (length-1);
					columns[i] = new BooleanColumn(val);
					break;
				  }
	        	}
	        }//END PAYLOAD
	        if(cols == 0){//uninitialized instance
	        	df = new DefaultDataFrame();
	        }else{
	        	df = new DefaultDataFrame(names, columns);	
	        }
		}
		return df;
	}
	
	/**
	 * Serialization to the legacy text-based <b>version 1</b> format (v1).<br>
	 * This code is not used anymore and may get removed in the future
	 */
	@SuppressWarnings("unused")
	private static byte[] serializeImplv1(final DataFrame df) throws IOException{
		int ptr = -1;
		byte[] bytes = new byte[2048];
		
		//HEADER
		for(final byte b : "{v:1;i:".getBytes()){
			bytes[++ptr] = b;
		}
		if(df.isNullable()){
			for(final byte b : "nullable;".getBytes()){
				bytes[++ptr] = b;
			}
		}else{
			for(final byte b : "default;".getBytes()){
				bytes[++ptr] = b;
			}
		}
		for(final byte b : ("r:"+df.rows()+";").getBytes()){
			bytes[++ptr] = b;
		}
		for(final byte b : ("c:"+df.columns()+";").getBytes()){
			bytes[++ptr] = b;
		}
		for(final byte b : ("n:").getBytes()){
			bytes[++ptr] = b;
		}
		if(df.hasColumnNames()){
			for(final String name : escapeColumnNames(df.getColumnNames())){
				bytes = ensureCapacity(bytes, ptr+name.length()+1);
				for(final byte b : (name+",").getBytes()){
					bytes[++ptr] = b;
				}
			}
		}
		bytes = ensureCapacity(bytes, ptr+3);
		bytes[++ptr] = ';';
		bytes[++ptr] = 't';
		bytes[++ptr] = ':';
		for(final Column col : df){
			final String name = col.getClass().getSimpleName();
			bytes = ensureCapacity(bytes, ptr+name.length()+1);
			for(final byte b : name.getBytes()){
				bytes[++ptr] = b;
			}
			bytes[++ptr] = ',';
		}
		bytes = ensureCapacity(bytes, ptr+2);
		bytes[++ptr] = ';';
		bytes[++ptr] = '}';
		//END HEADER
		
		//PAYLOAD
		for(final Column col : df){
			if((col instanceof StringColumn) 
					|| (col instanceof CharColumn) 
					|| (col instanceof NullableStringColumn) 
					|| (col instanceof NullableCharColumn)){
				
				for(int i=0; i<df.rows(); ++i){
					final Object o = col.getValueAt(i);
					final byte[] b = (o == null ? "null,".getBytes() : escapeString(o));
					bytes = ensureCapacity(bytes, ptr+b.length);
					for(int j=0; j<b.length; ++j){
						bytes[++ptr] = b[j];
					}
				}
			}else{
				for(int i=0; i<df.rows(); ++i){
					final Object o = col.getValueAt(i);
					final byte[] b = (o == null 
                            ? "null,".getBytes() 
                            : (o.toString()+",").getBytes());
					bytes = ensureCapacity(bytes, ptr+b.length);
					for(int j=0; j<b.length; ++j){
						bytes[++ptr] = b[j];
					}
				}
			}

		}
		//END PAYLOAD
		final byte[] b = new byte[ptr+1];//trim
		for(int i=0; i<b.length; ++i){
			b[i] = bytes[i];
		}
		bytes = b;
		return bytes;
	}
	
	/**
	 * Deserialization from the legacy text-based <b>version 1</b> format (v1).<br>
	 * This code is here purely for backwards compatibility and may get removed in the future.<br>
	 * 
	 * @param bytes The byte array representing the DataFrame to deserialize
	 * @return A DataFrame from the given array of bytes
	 * @throws IOException If any errors occur during deserialization or if the
	 * 					   given byte array does not constitute a DataFrame v1
	 */
	private static DataFrame deserializeImplv1(final byte[] bytes) throws IOException{
		if(bytes[3] != '1'){
			throw new IOException("Unsupported encoding");
		}
		DataFrame df = null;
		int rows = 0;
		int cols = 0;
		String dfType = null;
		String[] columnNames = null;
		String[] columnTypes = new String[0];//avoid null warning
		Column[] columns = null;
		
		//HEADER
		@SuppressWarnings("unused")
		byte b = 0;//only used in while loops. Triggers 'unused' warning
		int i1 = 7;//'begin' pointer
		int i2 = 6;//'end' pointer
		while((b = bytes[++i2]) != ';');
		byte[] tmp = copyBytes(bytes, i1, i2);
		dfType = new String(tmp);
		if(!dfType.equals("default") && !dfType.equals("nullable")){
			throw new IOException("Unsupported DataFrame implementation");	
		}
		i1 = i2+3;
		i2 += 2;
		while((b = bytes[++i2]) != ';');
		tmp = copyBytes(bytes, i1, i2);
		rows = Integer.valueOf(new String(tmp));
		i1 = i2+3;
		i2 += 2;
		while((b = bytes[++i2]) != ';');
		tmp = copyBytes(bytes, i1, i2);
		cols = Integer.valueOf(new String(tmp));
		if(bytes[i2+3] != ';'){//has column names
			columnNames = new String[cols];
			i1 = i2+3;
			i2 += 2;
			for(int j=0; j<cols; ++j){
				while((b = bytes[++i2]) != ',' || ((bytes[i2-1] == '<') && (bytes[i2+1] == '>')));
				columnNames[j] = new String(copyBytes(bytes, i1, i2))
						.replace("<,>", ",").replace("<<>", "<");
				
				i1 = i2+1;
			}
			i2 += 3;
			i1 = i2+1;
		}else{
			i2 += 5;
			i1 = i2+1;
		}
		if(cols > 0){//is not empty
			columnTypes = new String[cols];
			for(int j=0; j<cols; ++j){
				while((b = bytes[++i2]) != ',');
				columnTypes[j] = new String(copyBytes(bytes, i1, i2));
				i1 = i2+1;
			}
			i2 += 1;
		}
		//END HEADER
		
		//PAYLOAD
		columns = new Column[cols];
		i1 += 2;
		if(dfType.equals("default")){
			for(int j=0; j<cols; ++j){
				switch(columnTypes[j]){
				case "StringColumn":
					final String[] stringCol = new String[rows];
					for(int k=0; k<rows; ++k){
						while((b = bytes[++i2]) != ',' || ((bytes[i2-1] == '<') 
								&& (bytes[i2+1] == '>')));
						
						stringCol[k] = new String(copyBytes(bytes, i1, i2))
								.replace("<,>", ",").replace("<<>", "<");
						
						i1 = i2+1;
					}
					columns[j] = new StringColumn(stringCol);
					break;
				case "ByteColumn":
					final byte[] byteCol = new byte[rows];
					for(int k=0; k<rows; ++k){
						while((b = bytes[++i2]) != ',');
						byteCol[k] = Byte.valueOf(new String(copyBytes(bytes, i1, i2)));
						i1 = i2+1;
					}
					columns[j] = new ByteColumn(byteCol);
					break;
				case "ShortColumn":
					final short[] shortCol = new short[rows];
					for(int k=0; k<rows; ++k){
						while((b = bytes[++i2]) != ',');
						shortCol[k] = Short.valueOf(new String(copyBytes(bytes, i1, i2)));
						i1 = i2+1;
					}
					columns[j] = new ShortColumn(shortCol);
					break;
				case "IntColumn":
					final int[] intCol = new int[rows];
					for(int k=0; k<rows; ++k){
						while((b = bytes[++i2]) != ',');
						intCol[k] = Integer.valueOf(new String(copyBytes(bytes, i1, i2)));
						i1 = i2+1;
					}
					columns[j] = new IntColumn(intCol);
					break;
				case "LongColumn":
					final long[] longCol = new long[rows];
					for(int k=0; k<rows; ++k){
						while((b = bytes[++i2]) != ',');
						longCol[k] = Long.valueOf(new String(copyBytes(bytes, i1, i2)));
						i1 = i2+1;
					}
					columns[j] = new LongColumn(longCol);
					break;
				case "FloatColumn":
					final float[] floatCol = new float[rows];
					for(int k=0; k<rows; ++k){
						while((b = bytes[++i2]) != ',');
						floatCol[k] = Float.valueOf(new String(copyBytes(bytes, i1, i2)));
						i1 = i2+1;
					}
					columns[j] = new FloatColumn(floatCol);
					break;
				case "DoubleColumn":
					final double[] doubleCol = new double[rows];
					for(int k=0; k<rows; ++k){
						while((b = bytes[++i2]) != ',');
						doubleCol[k] = Double.valueOf(new String(copyBytes(bytes, i1, i2)));
						i1 = i2+1;
					}
					columns[j] = new DoubleColumn(doubleCol);
					break;
				case "BooleanColumn":
					final boolean[] booleanCol = new boolean[rows];
					for(int k=0; k<rows; ++k){
						while((b = bytes[++i2]) != ',');
						booleanCol[k] = Boolean.valueOf(new String(copyBytes(bytes, i1, i2)));
						i1 = i2+1;
					}
					columns[j] = new BooleanColumn(booleanCol);
					break;
				case "CharColumn":
					final char[] charCol = new char[rows];
					for(int k=0; k<rows; ++k){
						while((b = bytes[++i2]) != ',' || ((bytes[i2-1] == '<') 
								&& (bytes[i2+1] == '>')));
						
						charCol[k] = new String(copyBytes(bytes, i1, i2))
								.replace("<,>", ",").charAt(0);
						
						i1 = i2+1;
					}
					columns[j] = new CharColumn(charCol);
					break;
				}
			}
		}else if(dfType.equals("nullable")){
			for(int j=0; j<cols; ++j){
				switch(columnTypes[j]){
				case "NullableStringColumn":
					final String[] stringCol = new String[rows];
					for(int k=0; k<rows; ++k){
						while((b = bytes[++i2]) != ',' || ((bytes[i2-1] == '<') 
								&& (bytes[i2+1] == '>')));
						
						final String s = new String(copyBytes(bytes, i1, i2))
								.replace("<,>", ",").replace("<<>", "<");
						
						stringCol[k] = (!s.equals("null") ? s : null);
						i1 = i2+1;
					}
					columns[j] = new NullableStringColumn(stringCol);
					break;
				case "NullableByteColumn":
					final Byte[] byteCol = new Byte[rows];
					for(int k=0; k<rows; ++k){
						while((b = bytes[++i2]) != ',');
						final String s = new String(copyBytes(bytes, i1, i2));
						byteCol[k] = (!s.equals("null") ? Byte.valueOf(s) : null);
						i1 = i2+1;
					}
					columns[j] = new NullableByteColumn(byteCol);
					break;
				case "NullableShortColumn":
					final Short[] shortCol = new Short[rows];
					for(int k=0; k<rows; ++k){
						while((b = bytes[++i2]) != ',');
						final String s = new String(copyBytes(bytes, i1, i2));
						shortCol[k] = (!s.equals("null") ? Short.valueOf(s) : null);
						i1 = i2+1;
					}
					columns[j] = new NullableShortColumn(shortCol);
					break;
				case "NullableIntColumn":
					final Integer[] intCol = new Integer[rows];
					for(int k=0; k<rows; ++k){
						while((b = bytes[++i2]) != ',');
						final String s = new String(copyBytes(bytes, i1, i2));
						intCol[k] = (!s.equals("null") ? Integer.valueOf(s) : null);
						i1 = i2+1;
					}
					columns[j] = new NullableIntColumn(intCol);
					break;
				case "NullableLongColumn":
					final Long[] longCol = new Long[rows];
					for(int k=0; k<rows; ++k){
						while((b = bytes[++i2]) != ',');
						final String s = new String(copyBytes(bytes, i1, i2));
						longCol[k] = (!s.equals("null") ? Long.valueOf(s) : null);
						i1 = i2+1;
					}
					columns[j] = new NullableLongColumn(longCol);
					break;
				case "NullableFloatColumn":
					final Float[] floatCol = new Float[rows];
					for(int k=0; k<rows; ++k){
						while((b = bytes[++i2]) != ',');
						final String s = new String(copyBytes(bytes, i1, i2));
						floatCol[k] = (!s.equals("null") ? Float.valueOf(s) : null);
						i1 = i2+1;
					}
					columns[j] = new NullableFloatColumn(floatCol);
					break;
				case "NullableDoubleColumn":
					final Double[] doubleCol = new Double[rows];
					for(int k=0; k<rows; ++k){
						while((b = bytes[++i2]) != ',');
						final String s = new String(copyBytes(bytes, i1, i2));
						doubleCol[k] = (!s.equals("null") ? Double.valueOf(s) : null);
						i1 = i2+1;
					}
					columns[j] = new NullableDoubleColumn(doubleCol);
					break;
				case "NullableBooleanColumn":
					final Boolean[] booleanCol = new Boolean[rows];
					for(int k=0; k<rows; ++k){
						while((b = bytes[++i2]) != ',');
						final String s = new String(copyBytes(bytes, i1, i2));
						booleanCol[k] = (!s.equals("null") ? Boolean.valueOf(s) : null);
						i1 = i2+1;
					}
					columns[j] = new NullableBooleanColumn(booleanCol);
					break;
				case "NullableCharColumn":
					final Character[] charCol = new Character[rows];
					for(int k=0; k<rows; ++k){
						while((b = bytes[++i2]) != ',' || ((bytes[i2-1] == '<')
								&& (bytes[i2+1] == '>')));
						
						final String s = new String(copyBytes(bytes, i1, i2));
						charCol[k] = (!s.equals("null") 
								? new String(copyBytes(bytes, i1, i2))
										.replace("<,>", ",")
										.charAt(0) 
								: null);
						
						i1 = i2+1;
					}
					columns[j] = new NullableCharColumn(charCol);
					break;
				}
			}
		}
		//END PAYLOAD
		
		switch(dfType){
		case "default":
			if(columns.length == 0){
				df = new DefaultDataFrame();
			}else if(columnNames == null){
				df = new DefaultDataFrame(columns);
			}else{
				df = new DefaultDataFrame(columnNames, columns);
			}
			break;
		case "nullable":
			if(columns.length == 0){
				df = new NullableDataFrame();
			}else if(columnNames == null){
				df = new NullableDataFrame(columns);
			}else{
				df = new NullableDataFrame(columnNames, columns);
			}
			break;
		}
		return df;
	}
	
	/**
	 * Compresses the given array of bytes and modifies the first two bytes of the compressed 
	 * instance to represent a serialized DataFrame
	 * 
	 * @param bytes The bytes to compress
	 * @return The compressed array of bytes
	 * @throws IOException If any errors occur during compression
	 */
	private static byte[] compress(byte[] bytes) throws IOException{
		final Deflater deflater = new Deflater();
		deflater.setInput(bytes);
		final ByteArrayOutputStream os = new ByteArrayOutputStream(bytes.length);
		deflater.finish();
		byte[] buffer = new byte[2048];
		while(!deflater.finished()){
			os.write(buffer, 0, deflater.deflate(buffer));
		}
		bytes = os.toByteArray();
		bytes[0] = DF_BYTE0;
		bytes[1] = DF_BYTE1;
		return bytes;
	}

	/**
	 * Decompresses the given array of bytes
	 * 
	 * @param bytes The bytes to decompress
	 * @return The decompressed array of bytes
	 * @throws IOException If any errors occur during decompression
	 */
	private static byte[] decompress(final byte[] bytes) throws IOException{
		final Inflater inflater = new Inflater();
		//remember the first two bytes in order to reset them after
		//decompression so that we don't have to create a copy of
		//the entire array here. This only introduces problems when
		//trying to read the provided array while decompression is
		//still in process
		final byte b0 = bytes[0];
		final byte b1 = bytes[1];
		//set zlib compression magic numbers
		bytes[0] = (byte)0x78;
		bytes[1] = (byte)0x9c;
		inflater.setInput(bytes);
		final ByteArrayOutputStream os = new ByteArrayOutputStream(bytes.length);
		byte[] buffer = new byte[2048];
		try{
			while(!inflater.finished()){
				os.write(buffer, 0, inflater.inflate(buffer));
			}
		}catch(DataFormatException ex){
			throw new IOException("Invalid data format");
		}
		//reset original first two bytes
		bytes[0] = b0;
		bytes[1] = b1;
		return os.toByteArray();
	}


	/**
	 * Escapes special characters in all given column names
	 * 
	 * @param names The column names to potentially escape
	 * @return The escaped version of all column names
	 */
	private static String[] escapeColumnNames(final String[] names){
		final String[] escaped = new String[names.length];
		for(int i=0; i<names.length; ++i){
			escaped[i] = names[i].replace("<", "<<>");
			escaped[i] = escaped[i].replace(",", "<,>");
		}
		return escaped;
	}
	
	/**
	 * Escapes special characters in String- and Character objects
	 * 
	 * @param obj The String- or Character object to potentially escape
	 * @return The escaped string or character encoded as a byte array
	 */
	private static byte[] escapeString(final Object obj){
		return obj.toString().replace("<", "<<>")
                             .replace(",", "<,>")
                             .concat(",")
                             .getBytes(UTF_8);
	}
	
	/**
	 * Ensures that the provided byte array has at least
	 * the specified minimum capacity
	 * 
	 * @param bytes The byte array to check for the specified minimum capacity
	 * @param min The minumum capacity of the specified byte array
	 * @return The potentially enlarged byte array
	 */
	private static byte[] ensureCapacity(final byte[] bytes, final int min){
        if((min-bytes.length) >= 0){
            return resize(bytes, min);
        }
        return bytes;
    }
	
	/**
	 * Resizes the provided byte array to make sure it can hold at least the specifed
	 * amount of bytes
	 * 
	 * @param bytes The byte array to resize
	 * @param min The minimum capacity of the byte array to be resized
	 * @return The enlarged byte array
	 */
	private static byte[] resize(final byte[] bytes, final int min){
        int newCapacity = 0;
        int shift = 0;
        while(newCapacity < min){
        	newCapacity = bytes.length << ++shift;
        	if(newCapacity >= (1 << 30)){
        		newCapacity = Integer.MAX_VALUE;
        		break;
        	}
        }
        return Arrays.copyOf(bytes, newCapacity);
	}
	
	/**
	 * Copies all bytes from the given byte array from (inclusive) the specified index 
	 * to (exclusive) the specified index
	 * 
	 * @param bytes The array of bytes
	 * @param from The position to copy from
	 * @param to The position to copy to
	 * @return An array holding all bytes from the given array from the specified position 
	 * 		   to the specified position
	 */
	private static byte[] copyBytes(final byte[] bytes, final int from, final int to){
		int j = -1;
		final byte[] b = new byte[to-from];
		for(int i=from; i<to; ++i){
			b[++j] = bytes[i];
		}
		return b;
	}
	
	/**
	 * Background thread for concurrent write operations of DataFrames files.
	 *
	 */
	private static class ConcurrentDFWriter implements Runnable {
		
		private ConcurrentWriter delegate;
		private File file;
		private DataFrame df;
		
		/**
		 * Constructs a new <code>ConcurrentDFWriter</code>
		 * 
		 * @param file The file to write
		 * @param df The DataFrame to write
		 * @param delegate The delegate for the callback
		 */
		ConcurrentDFWriter(final File file, final DataFrame df, 
				final ConcurrentWriter delegate){
			
			this.file = file;
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
				writeFile(file, df);
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
	
	/**
	 * Background thread for concurrent read operations of DataFrames files.
	 *
	 */
	private static class ConcurrentDFReader implements Runnable {
		
		private ConcurrentReader delegate;
		private File file;
		
		/**
		 * Constructs a new <code>ConcurrentDFReader</code>
		 * 
		 * @param file The file to read
		 * @param delegate The delegate for the callback
		 */
		ConcurrentDFReader(final File file, final ConcurrentReader delegate){
			this.file = file;
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
				df = readFile(file);
			}catch(IOException ex){ }
			
			if(delegate != null){
				delegate.onRead(df);
			}
		}
	}
}
