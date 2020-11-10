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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Base64;
import java.util.concurrent.CompletableFuture;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import com.raven.common.struct.BinaryColumn;
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
import com.raven.common.struct.NullableBinaryColumn;
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
 * method will deserialize bytes into a DataFrame.
 * 
 * <p>This class is also used to work with <code>.df</code> files. 
 * You can persist a <code>DataFrame</code> to a file by passing it to the 
 * {@link DataFrameSerializer#writeFile(File, DataFrame)} method.
 * By calling the {@link DataFrameSerializer#readFile(File)} method you 
 * can get the original <code>DataFrame</code> back from the file.
 * 
 * <p>One may also directly write a DataFrame to an output stream with
 * the {@link DataFrameSerializer#writeTo(OutputStream, DataFrame)} method or
 * read a DataFrame directly from an input stream via
 * the {@link DataFrameSerializer#readFrom(InputStream)} method.
 * 
 * <p>Additionally, this class is also capable of serializing a <code>DataFrame</code>
 * to a <code>Base64</code> encoded string with
 * the {@link DataFrameSerializer#toBase64(DataFrame)} method and deserialize such
 * a <code>Base64</code> encoded string with
 * the {@link DataFrameSerializer#fromBase64(String)} method.
 * 
 * <p>Asynchronous reading and writing of files is supported through the
 * {@link DataFrameSerializer#readFileAsync(File)} and
 * {@link DataFrameSerializer#writeFileAsync(File, DataFrame)} method respectively.
 * 
 * @author Phil Gaiser
 * @see CSVReader
 * @see CSVWriter
 * @since 1.0.0
 *
 */
public final class DataFrameSerializer {

    /** The file extension used for DataFrames **/
    public static final String DF_FILE_EXTENSION = ".df";

    /**
     * Flag indicating that a DataFrame should be serialized with compression
     */
    public static final boolean MODE_COMPRESSED = true;

    /**
     * Flag indicating that a DataFrame should only be serialized but not compressed
     */
    public static final boolean MODE_UNCOMPRESSED = false;

    private static final byte DF_BYTE0 = 0x64;
    private static final byte DF_BYTE1 = 0x66;

    /** The character set used for serialization and deserialization of Strings **/
    private static final Charset UTF_8 = Charset.forName("UTF-8");

    /**
     * Deserializes the given <code>Base64</code> encoded string to a DataFrame
     * 
     * @param string The Base64 encoded string representing the DataFrame to deserialize
     * @return A DataFrame from the given Base64 string
     * @throws SerializationException If any errors occur during deserialization
     */
    public static DataFrame fromBase64(final String string) throws SerializationException{
        if(string == null){
            throw new SerializationException("String argument must not be null");
        }
        return deserialize(Base64.getDecoder().decode(string));
    }

    /**
     * Serializes the given DataFrame to a <code>Base64</code> encoded string
     * 
     * @param df The DataFrame to serialize to a Base64 encoded string
     * @return A Base64 encoded string representing the given DataFrame
     * @throws SerializationException If any errors occur during serialization
     */
    public static String toBase64(final DataFrame df) throws SerializationException{
        return Base64.getEncoder().encodeToString(compress(serialize(df)));
    }

    /**
     * Reads the specified file and returns a DataFrame constituted by the 
     * content of that file
     * 
     * @param file The file to read. Must be a <code>.df</code> file
     * @return A DataFrame from the specified file
     * @throws IOException If any errors occur during file reading
     * @throws SerializationException If any errors occur during deserialization
     */
    public static DataFrame readFile(final String file)
            throws IOException, SerializationException{

        return readFile(new File(file));
    }

    /**
     * Reads the specified file and returns a DataFrame constituted by the 
     * content of that file
     * 
     * @param file The file to read. Must be a <code>.df</code> file
     * @return A DataFrame from the specified file
     * @throws IOException If any errors occur during file reading
     * @throws SerializationException If any errors occur during deserialization
     */
    public static DataFrame readFile(final File file)
            throws IOException, SerializationException{

        return readFrom(new FileInputStream(file));
    }

    /**
     * Creates a background thread which will read the df-file asynchronously and
     * supply a DataFrame representing its content to the CompletableFuture returned
     * by this method.
     * 
     * <p>This method is meant to be used for large df-files.
     * 
     * <p>When called, this method will return immediately. The result of the 
     * background operation will be passed to the {@link CompletableFuture} instance.
     * 
     * <p>Please note that any IOExceptions encountered by the launched background 
     * thread will result in the CompletableFuture being completed exceptionally.
     * 
     * @param file The file to read. Must be a <code>.df</code> file
     * @return A <code>CompletableFuture</code> for the asynchronous read operation
     * @throws IllegalArgumentException If the file argument is null
     */
    public static CompletableFuture<DataFrame> readFileAsync(final String file)
            throws IllegalArgumentException{

        return readFileAsync(new File(file));
    }

    /**
     * Creates a background thread which will read the df-file asynchronously and
     * supply a DataFrame representing its content to the CompletableFuture returned
     * by this method.
     * 
     * <p>This method is meant to be used for large df-files.
     * 
     * <p>When called, this method will return immediately. The result of the 
     * background operation will be passed to the {@link CompletableFuture} instance.
     * 
     * <p>Please note that any IOExceptions encountered by the launched background 
     * thread will result in the CompletableFuture being completed exceptionally.
     * 
     * @param file The file to read. Must be a <code>.df</code> file
     * @return A <code>CompletableFuture</code> for the asynchronous read operation
     * @throws IllegalArgumentException If the file argument is null
     */
    public static CompletableFuture<DataFrame> readFileAsync(final File file)
            throws IllegalArgumentException{

        if(file == null){
            throw new IllegalArgumentException("The File argument must not be null");
        }
        return new ConcurrentDFReader(file).execute();
    }

    /**
     * Persists the given DataFrame to the specified file
     * 
     * @param file The file to write the DataFrame to. Must not be null
     * @param df The DataFrame to persist. Must not be null
     * @throws IOException If any errors occur during file persistence
     * @throws SerializationException If any errors occur during serialization
     */
    public static void writeFile(final String file, final DataFrame df)
            throws IOException, SerializationException{

        writeFile(new File(file), df);
    }

    /**
     * Persists the given DataFrame to the specified file
     * 
     * @param file The file to write the DataFrame to. Must not be null
     * @param df The DataFrame to persist. Must not be null
     * @throws IOException If any errors occur during file persistence
     * @throws SerializationException If any errors occur during serialization
     */
    public static void writeFile(File file, final DataFrame df)
            throws IOException, SerializationException{

        if(!file.getName().endsWith(DF_FILE_EXTENSION)){
            file = new File(file.getAbsolutePath()+DF_FILE_EXTENSION);
        }
        writeTo(new FileOutputStream(file), df);
    }

    /**
     * Creates a background thread which will asynchronously persist the given
     * DataFrame to the specified file.<br>
     * The CompletableFuture returned by this method does not return
     * anything upon completion.<br>
     * 
     * <p>This method is meant to be used for large DataFrames/df-files.<br>
     * When called, this method will return immediately.
     * 
     * <p>Please note that any IOExceptions encountered by the launched background 
     * thread will result in the CompletableFuture being completed exceptionally.
     * 
     * @param file The file to write the DataFrame to. Must not be null
     * @param df The DataFrame to persist. Must not be null
     * @return A <code>CompletableFuture</code> for the asynchronous write operation
     * @throws IllegalArgumentException If the file or DataFrame argument is null
     */
    public static CompletableFuture<Void> writeFileAsync(final String file,
            final DataFrame df) throws IllegalArgumentException{

        return writeFileAsync(new File(file), df);
    }

    /**
     * Creates a background thread which will asynchronously persist the given
     * DataFrame to the specified file.<br>
     * The CompletableFuture returned by this method does not return
     * anything upon completion.<br>
     * 
     * <p>This method is meant to be used for large DataFrames/df-files.<br>
     * When called, this method will return immediately.
     * 
     * <p>Please note that any IOExceptions encountered by the launched background 
     * thread will result in the CompletableFuture being completed exceptionally.
     * 
     * @param file The file to write the DataFrame to
     * @param df The DataFrame to persist
     * @return A <code>CompletableFuture</code> for the asynchronous write operation
     * @throws IllegalArgumentException If the file or DataFrame argument is null
     */
    public static CompletableFuture<Void> writeFileAsync(final File file,
            final DataFrame df) throws IllegalArgumentException{

        if(file == null){
            throw new IllegalArgumentException(
                    "The File argument must not be null");
        }
        if(df == null){
            throw new IllegalArgumentException(
                    "The DataFrame argument must not be null");
        }
        return new ConcurrentDFWriter(file, df).execute();
    }

    /**
     * Serializes the given <code>DataFrame</code> to an array of bytes.<br>
     * The returned array is not compressed. The compression of the array can be 
     * controlled by passing an additional boolean flag to the arguments.
     * 
     * <p>See {@link DataFrameSerializer#serialize(DataFrame, boolean)}
     * 
     * @param df The DataFrame to serialize. Must not be null
     * @return A byte array representing the given DataFrame in a serialized form
     * @throws SerializationException If any errors occur during serialization
     */
    public static byte[] serialize(final DataFrame df) throws SerializationException{
        return serialize(df, MODE_UNCOMPRESSED);
    }

    /**
     * Serializes the given <code>DataFrame</code> to an array of bytes.<br>
     * The compression of the returned array is controlled by the additional boolean 
     * flag of this method.
     * 
     * @param df The DataFrame to serialize. Must not be null
     * @param compress A boolean flag indicating whether to compress the serialized bytes.
     *                 Must be either {@link DataFrameSerializer#MODE_COMPRESSED}
     *                 or {@link DataFrameSerializer#MODE_UNCOMPRESSED}
     * @return A byte array representing the given DataFrame in a serialized form
     * @throws SerializationException If any errors occur during serialization
     */
    public static byte[] serialize(final DataFrame df, final boolean compress)
            throws SerializationException{

        if(df == null){
            throw new SerializationException("DataFrame argument must not be null");
        }
        try{
            return (compress ? compress(serializeImplv2(df)) : serializeImplv2(df));
        }catch(Exception ex){
            //catch any unchecked runtime exception which at this point can
            //only be caused by improper or malicious usage of the DataFrame API
            throw new SerializationException(
                    "Serialization failed due to an invalid DataFrame format", ex);
        }
    }

    /**
     * Deserializes the given array of bytes to a <code>DataFrame</code>.
     * 
     * <p>If the given byte array is compressed, it will be automatically
     * decompressed before the deserialization is executed. The byte array of
     * the provided reference may be affected by this operation as long as
     * decompression is in process. The original state, however, will be restored
     * after decompression. This approach helps avoid additional copy operations and
     * should be considered when writing multi-threaded code that uses deserialization
     * as provided by this method.
     * 
     * <p>Deserialization of uncompressed arrays does only require read access and
     * therefore will never alter the content of the provided array
     * 
     * @param bytes The byte array representing the DataFrame to deserialize.
     *              Must not be null
     * @return A DataFrame from the given array of bytes
     * @throws SerializationException If any errors occur during deserialization
     *                                or decompression, or if the given byte array
     *                                does not constitute a DataFrame
     */
    public static DataFrame deserialize(byte[] bytes) throws SerializationException{
        if(bytes == null){
            throw new SerializationException("Array argument must not be null");
        }
        try{
            if((bytes[0] == DF_BYTE0) && (bytes[1] == DF_BYTE1)){
                bytes = decompress(bytes);
            }
            //validate the first bytes of the header and the used format version
            //must start with '{v:'
            if((bytes[0] != 0x7b) || (bytes[1] != 0x76) || (bytes[2] != 0x3a)
                    || ((bytes[3] != 0x32) && (bytes[3] != 0x31))){//version 2 and 1 supported

                throw new SerializationException(String.format("Unsupported encoding (v:%s)",
                        ((char)bytes[3])));
            }
            if(bytes[3] == 0x32){//encoding version 2
                return deserializeImplv2(bytes);
            }else{
                throw new SerializationException(
                        String.format("Unsupported encoding version (v:%s)",
                        ((char)bytes[3])));
            }
        }catch(Exception ex){
            //catch any unchecked exception which at
            //this point can only be caused by an invalid format
            throw new SerializationException(
                    "Deserialization failed due to an invalid DataFrame format", ex);
        }
    }

    /**
     * Reads all bytes from the specified InputStream and deserializes them
     * to a DataFrame.
     * 
     * <p>The input stream will be closed automatically before this method returns
     * 
     * @param is The <code>InputStream</code> to read from. Must not be null
     * @return A <code>DataFrame</code> from the bytes of the specified InputStream
     * @throws IOException If any errors occur when reading from the input stream
     *                     or during decompression
     * @throws SerializationException If any errors occur during deserialization, or if 
     *                     the bytes from the input stream do not constitute a DataFrame
     */
    public static DataFrame readFrom(final InputStream is)
            throws IOException, SerializationException{

        if(is == null){
            throw new SerializationException("InputStream argument must not be null");
        }
        final BufferedInputStream buffer = new BufferedInputStream(is);
        byte[] bytes = new byte[2048];
        final ByteArrayOutputStream baos = new ByteArrayOutputStream(bytes.length);
        try{
            while((buffer.read(bytes, 0, bytes.length)) != -1){
                baos.write(bytes, 0, bytes.length);
            }
        }finally{
            buffer.close();
        }
        bytes = baos.toByteArray();
        if(bytes[0] != DF_BYTE0 || bytes[1] != DF_BYTE1){
            throw new IOException(String.format(
                    "Is not a %s file. Starts with 0x%02X 0x%02X",
                    DF_FILE_EXTENSION, bytes[0], bytes[1]));

        }
        return deserialize(decompress(bytes));
    }

    /**
     * Serializes the specified DataFrame and writes the bytes
     * to the specified OutputStream.<br>
     * The bytes written to the output stream will be compressed.
     * 
     * <p>The output stream will be closed automatically before this method returns
     * 
     * @param os The <code>OutputStream</code> to write the DataFrame to.
     *           Must not be null
     * @param df The <code>DataFrame</code> to write to the specified OutputStream.
     *           Must not be null
     * @throws IOException If any errors occur when writing to the output stream
     *                     or during compression
     * @throws SerializationException If any errors occur during serialization
     */
    public static void writeTo(final OutputStream os, final DataFrame df)
            throws IOException, SerializationException{

        if(os == null){
            throw new SerializationException("OutputStream argument must not be null");
        }
        final BufferedOutputStream buffer = new BufferedOutputStream(os);
        try{
            buffer.write(compress(serialize(df)));
        }finally{
            buffer.close();
        }
    }

    /**
     * Serialization to the binary-based <b>version 2</b> format (v2).<br>
     * 
     * @param df The DataFrame to serialize
     * @return A byte array representing the given DataFrame
     * @throws SerializationException If any errors occur during serialization
     */
    private static byte[] serializeImplv2(final DataFrame df) throws SerializationException{
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
                            ptr += 1;
                        }else{
                            bytes[++ptr] = (byte) (val[i].charValue() & 0xff);
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
                }case NullableBinaryColumn.TYPE_CODE:{
                    final byte[][] val = ((NullableBinaryColumn)col).asArray();
                    for(int i=0; i<rows; ++i){
                        final byte[] data = val[i];
                        final int dataLength = (data != null) ? data.length : 0;
                        bytes[++ptr] = (byte) ((dataLength & 0xff000000) >> 24);
                        bytes[++ptr] = (byte) ((dataLength & 0xff0000) >> 16);
                        bytes[++ptr] = (byte) ((dataLength & 0xff00) >> 8);
                        bytes[++ptr] = (byte)  (dataLength & 0xff);
                        if(data != null){//fixes NP warning
                            for(int j=0; j<dataLength; ++j){
                                bytes[++ptr] = data[j];
                            }
                        }
                    }
                    break;
                }
                default:
                    throw new SerializationException("Unknown column type: "
                            + col.getClass().getName());
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
                        bytes[++ptr] = (byte) (val[i] & 0xff);
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
                }case BinaryColumn.TYPE_CODE:{
                    final byte[][] val = ((BinaryColumn)col).asArray();
                    for(int i=0; i<rows; ++i){
                        final byte[] data = val[i];
                        final int dataLength = data.length;
                        bytes[++ptr] = (byte) ((dataLength & 0xff000000) >> 24);
                        bytes[++ptr] = (byte) ((dataLength & 0xff0000) >> 16);
                        bytes[++ptr] = (byte) ((dataLength & 0xff00) >> 8);
                        bytes[++ptr] = (byte)  (dataLength & 0xff);
                        for(int j=0; j<dataLength; ++j){
                            bytes[++ptr] = data[j];
                        }
                    }
                    break;
                }
                default:
                    throw new SerializationException("Unknown column type: "
                            + col.getClass().getName());
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
     * @throws SerializationException If any errors occur during deserialization
     */
    private static DataFrame deserializeImplv2(final byte[] bytes) throws SerializationException{
        //HEADER
        int ptr = 5;//first bytes have already been validated at this point
        final byte dfType = bytes[ptr];
        if((dfType != 0x64) && (dfType != 0x6e)){
            throw new SerializationException("Unsupported DataFrame implementation");
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
                throw new SerializationException("Invalid format");
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
                        final byte b = bytes[++ptr];
                        if(b == 0){
                            val[j] = null;
                        }else{
                            val[j] = (char) b;
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
                }case NullableBinaryColumn.TYPE_CODE:{
                    final byte[][] val = new byte[rows][0];
                    for(int j=0; j<rows; ++j){
                        final int dataLength = ((bytes[++ptr] & 0xff) << 24
                                              | (bytes[++ptr] & 0xff) << 16
                                              | (bytes[++ptr] & 0xff) << 8
                                              | (bytes[++ptr] & 0xff));

                        if(dataLength == 0){
                            val[j] = null;
                        }else{
                            final byte[] data = new byte[dataLength];
                            for(int k=0; k<dataLength; ++k){
                                data[k] = bytes[++ptr];
                            }
                            val[j] = data;
                        }
                    }
                    columns[i] = new NullableBinaryColumn(val);
                    break;
                }
                default:
                    throw new SerializationException("Unknown column with type code: "
                            + types[i]);

                }
            }//END PAYLOAD
            if(cols == 0){//uninitialized instance
                df = new NullableDataFrame();
            }else{
                df = new NullableDataFrame(names, columns);
            }
        }else{//DefaultDataFrame
            if(bytes[++ptr] != 0x7d){//header closing brace '}'
                throw new SerializationException("Invalid format");
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
                            val[j] = StringColumn.DEFAULT_VALUE;
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
                        val[j] = (char) bytes[++ptr];
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
                }case BinaryColumn.TYPE_CODE:{
                    final byte[][] val = new byte[rows][0];
                    for(int j=0; j<rows; ++j){
                        final int dataLength = ((bytes[++ptr] & 0xff) << 24
                                              | (bytes[++ptr] & 0xff) << 16
                                              | (bytes[++ptr] & 0xff) << 8
                                              | (bytes[++ptr] & 0xff));

                        final byte[] data = new byte[dataLength];
                        for(int k=0; k<dataLength; ++k){
                            data[k] = bytes[++ptr];
                        }
                        val[j] = data;
                    }
                    columns[i] = new BinaryColumn(val);
                    break;
                }
                default:
                    throw new SerializationException("Unknown column with type code: "
                            + types[i]);

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
     * Compresses the given array of bytes and modifies the first two bytes of the compressed 
     * instance to represent a serialized DataFrame
     * 
     * @param bytes The bytes to compress
     * @return The compressed array of bytes
     */
    private static byte[] compress(byte[] bytes){
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
            throw new IOException("Invalid compressed data format");
        }
        //reset original first two bytes
        bytes[0] = b0;
        bytes[1] = b1;
        return os.toByteArray();
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
        int newCapacity = bytes.length;
        if((min < 0) || (newCapacity == (Integer.MAX_VALUE-1))){
            throw new SerializationException(
                    "Array length exceeds maximum capacity");
        }
        while(newCapacity <= min){
            newCapacity *= 2;
            if(newCapacity < 1){
                //overflow
                newCapacity = Integer.MAX_VALUE-1;
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

        private File file;
        private DataFrame df;
        private CompletableFuture<Void> future;

        /**
         * Constructs a new <code>ConcurrentDFWriter</code>
         * 
         * @param file The file to write
         * @param df The DataFrame to write
         */
        ConcurrentDFWriter(final File file, final DataFrame df){
            this.file = file;
            this.df = df;
            this.future = new CompletableFuture<Void>();
        }

        /**
         * Starts this Runnable in its own thread
         */
        public CompletableFuture<Void> execute(){
            new Thread(this).start();
            return this.future;
        }

        @Override
        public void run(){
            try{
                writeFile(file, df);
                this.future.complete(null);
            }catch(Throwable ex){
                this.future.completeExceptionally(ex);
            }
        }
    }

    /**
     * Background thread for concurrent read operations of DataFrames files.
     *
     */
    private static class ConcurrentDFReader implements Runnable {

        private File file;
        private CompletableFuture<DataFrame> future;

        /**
         * Constructs a new <code>ConcurrentDFReader</code>
         * 
         * @param file The file to read
         */
        ConcurrentDFReader(final File file){
            this.file = file;
            this.future = new CompletableFuture<DataFrame>();
        }

        /**
         * Starts this Runnable in its own thread
         */
        public CompletableFuture<DataFrame> execute(){
            new Thread(this).start();
            return this.future;
        }

        @Override
        public void run(){
            try{
                this.future.complete(readFile(file));
            }catch(Throwable ex){
                this.future.completeExceptionally(ex);
            }
        }
    }
}
