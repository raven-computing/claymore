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
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Pattern;

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
 * Convenience class for reading CSV-files. A CSVReader uses the <i>UTF-8</i>
 * character encoding and a comma as a separator by default.<br>
 * To specify these values yourself, call <code>useCharset()</code> and
 * <code>useSeparator()</code> respectively.
 * 
 * <p>If not further specified, a CSVReader assumes that the CSV-file to read has
 * a header. Therefore, the first line of a CSV-file will be used to construct the
 * column names of the DataFrame returned by the <code>read()</code> method. The
 * handling of the first line can be controlled by
 * the <code>withHeader()</code> method.
 * 
 * <p>The {@link DataFrame} returned by the <code>read()</code> method uses the
 * <i>String</i> type for all columns by default. However, a user may specify 
 * each type individually. Passing the types directly to the
 * <code>useColumnTypes()</code> method will ensure that the columns of the returned 
 * DataFrame are of the corresponding type. The order of the types of the argument 
 * will specify to which column that type is assigned to.
 * 
 * <p>There is only one restriction regarding the separator and the content of
 * the CSV-file to read.<br>
 * Any character except <i>double quotes</i> can be used as a separator. If a data value
 * contains one or more instances of the used separator character, then that data value
 * must be enclosed with double quotes. For that reason <i>double quotes</i> cannot
 * be used as a separator character or occur inside data values.
 * 
 * <p>If a data value in the CSV-file is nonexistent or empty, then it should have a
 * text representation of <i>"null"</i> inside the file to read. Simply omitting
 * nonexistent or empty values, causing separators to be pasted together, is considered
 * to be a malformed line, which this implementation handles gracefully.
 * If a CSV-file contains at least one data value which is represented by the <i>"null"</i>
 * string, then the DataFrame produced by the underlying CSVReader instance will
 * be a <code>NullableDataFrame</code> and the respective values will be null.
 * 
 * <p>A CSVReader may also be constructed to read any <code>InputStream</code> passed to
 * the constructor. Any input stream will be automatically wrapped and buffered
 * by a <code>BufferedReader</code> instance. All closable resources will be
 * automatically closed by a CSVReader after a read operation.
 * 
 * @author Phil Gaiser
 * @see CSVWriter
 * @since 1.0.0
 *
 */
public class CSVReader {

    private InputStream is;
    private File file;
    private String separator = ",";
    private Charset charset = StandardCharsets.UTF_8;
    private Column[] types;
    private boolean hasHeader = true;

    /** Used for concurrent read operations **/
    private ConcurrentCSVReader async;

    /**
     * Constructs a new <code>CSVReader</code> for the specified file.<br>
     * It's assumed that the CSV-file has a header.<br>
     * All columns of the returned DataFrame will be of type <i>String</i>.
     * 
     * <p>The individual types may be adjusted.<br>
     * <p><i>Example:</i><br>
     * <pre><code> 
     * new CSVReader("file.csv")
     *          .withHeadaer(true)
     *          .useSeparator(',')
     *          .useCharset("UTF-8")
     *          .useColumnTypes(Integer.class, String.class, Float.class)
     *          .read();
     * </code></pre>
     * 
     * The above code will construct a <code>CSVReader</code> which can read a
     * file called "file.csv" which has a header. The first column consists of integers,
     * the second column consists of strings and the third column consists of floats
     * 
     * @param file The file to read. May be a path to a file. Must not be null
     */
    public CSVReader(final String file){
        if((file == null) || (file.isEmpty())){
            throw new IllegalArgumentException("File argument must not be null or empty");
        }
        this.file = new File(file);
    }

    /**
     * Constructs a new <code>CSVReader</code> for the specified file object.<br>
     * It's assumed that the CSV-file has a header.<br>
     * All columns of the returned DataFrame will be of type <i>String</i>.
     * 
     * <p>The individual types may be adjusted.<br>
     * <p><i>Example:</i><br>
     * <pre><code> 
     * new CSVReader(csvFile)
     *          .withHeadaer(true)
     *          .useSeparator(',')
     *          .useCharset("UTF-8")
     *          .useColumnTypes(Integer.class, String.class, Float.class)
     *          .read();
     * </code></pre>
     * 
     * The above code will construct a <code>CSVReader</code> which can read the
     * specified <code>File</code> object. The CSV-file has a header. The first column
     * consists of integers, the second column consists of strings and the
     * third column consists of floats
     * 
     * @param file The {@link File} to read. Must not be null
     */
    public CSVReader(final File file){
        if(file == null){
            throw new IllegalArgumentException("File argument must not be null");
        }
        this.file = file;
    }

    /**
     * Constructs a new <code>CSVReader</code> for reading the specified
     * <code>InputStream</code>.<br>
     * It's assumed that the CSV-file has a header.<br>
     * All columns of the returned DataFrame will be of type <i>String</i>.
     * 
     * <p>The individual types may be adjusted.<br>
     * <p><i>Example:</i><br>
     * <pre><code> 
     * new CSVReader(inputStream)
     *          .withHeadaer(true)
     *          .useSeparator(',')
     *          .useCharset("UTF-8")
     *          .useColumnTypes(Integer.class, String.class, Float.class)
     *          .read();
     * </code></pre>
     * 
     * The above code will construct a <code>CSVReader</code> which can read the
     * specified <code>InputStream</code>. The CSV-file has a header. The first column
     * consists of integers, the second column consists of strings and the
     * third column consists of floats
     * 
     * @param is The <code>InputStream</code> to read from. Must not be null
     */
    public CSVReader(final InputStream is){
        if(is == null){
            throw new IllegalArgumentException("InputStream argument must not be null");
        }
        this.is = is;
    }

    /**
     * Reads the CSV-file and returns a DataFrame representing its content.<br>
     * This method can only be called once. Subsequent calls will result in an
     * <code>IllegalStateException</code>.<br>
     * If this <code>CSVReader</code> was constructed to use headers, then
     * the first line of the file is used to create the column names of the
     * returned DataFrame.
     * <p>Resources will be closed automatically before this method returns
     * 
     * @return A DataFrame holding the content of the CSV-file read
     * @throws IOException If the file cannot be opened or read, or if the file 
     *                     content is improperly formatted
     * @throws IllegalStateException If this method has already been called
     * @see #readAsync()
     */
    public DataFrame read() throws IOException{
        if(file != null){
            ensureExists();
        }
        return read0();
    }

    /**
     * Creates a background thread which will read the CSV-file asynchronously and
     * supply a DataFrame representing its content to the CompletableFuture returned
     * by this method.<br>
     * This method can only be called once. Subsequent calls will result in an
     * <code>IllegalStateException</code>.
     * 
     * <p>This method is meant to be used for large CSV-files.
     * 
     * <p>When called, this method will return immediately. The result of the 
     * background operation will be passed to the {@link CompletableFuture} instance.
     * 
     * <p>Please note that any IOExceptions encountered by the launched background 
     * thread will result in the CompletableFuture being completed exceptionally.
     * 
     * @return A <code>CompletableFuture</code> for the asynchronous read operation
     * @throws IllegalStateException If this method has already been called
     * @see #read()
     */
    public CompletableFuture<DataFrame> readAsync() throws IllegalStateException{
        if(async != null){
            throw new IllegalStateException("readAsync() already called");
        }
        this.async = new ConcurrentCSVReader();
        return this.async.execute();
    }

    /**
     * Instructs this <code>CSVReader</code> whether to treat the first line
     * of the CSV data as a header
     * 
     * @param readHeader A boolean value specifiying whether to treat
     *                   the first line as a header
     * @return This CSVReader instance
     */
    public CSVReader withHeader(final boolean readHeader){
        this.hasHeader = readHeader;
        return this;
    }

    /**
     * Instructs this <code>CSVReader</code> to use the specified separator when
     * reading the CSV data.<br>
     * The default separator is a comma (','). 
     * 
     * @param separator The character to be used as a separator
     * @return This CSVReader instance
     */
    public CSVReader useSeparator(final char separator){
        if(separator == '"'){
            throw new IllegalArgumentException(
                    "Cannot use double quotes as separator character");
        }
        this.separator = normalizeSeparator(separator);
        return this;
    }

    /**
     * Instructs this <code>CSVReader</code> to use the specified Charset when
     * reading CSV data.<br>
     * For example: <code>"UTF-8"</code>, which is unicode encoded as UTF-8
     * 
     * @param charset The charset to be used when reading CSV data
     * @return This CSVReader instance
     * @throws IllegalCharsetNameException If the given charset name is illegal
     * @throws UnsupportedCharsetException If the named charset is not
     *                                     supported in the underlying JVM
     * @see #useCharset(Charset)
     */
    public CSVReader useCharset(final String charset)
            throws UnsupportedCharsetException, IllegalCharsetNameException{

        this.charset = ((charset != null)
                ? Charset.forName(charset)
                        : StandardCharsets.UTF_8);

        return this;
    }

    /**
     * Instructs this <code>CSVReader</code> to use the specified Charset when
     * reading CSV data.<br>
     * For example: <code>StandardCharsets.UTF_8</code>, which is
     * unicode encoded as UTF-8
     * 
     * @param charset The charset to be used when reading CSV data
     * @return This CSVReader instance
     * @see #useCharset(String)
     */
    public CSVReader useCharset(final Charset charset){
        this.charset = ((charset != null)
                ? charset
                        : StandardCharsets.UTF_8);

        return this;
    }

    /**
     * Instructs this <code>CSVReader</code> to use the specified types when
     * reading CSV data.<br>
     * These types will correspond to the types used by columns of the DataFrame
     * returned by <code>read()</code>.
     * 
     * <p><i>Example:</i><br>
     * <pre><code> 
     * myReader.useColumnTypes(Integer.class, String.class, Float.class);
     * </code></pre>
     * 
     * The above code will instruct the <code>CSVReader</code> to treat all values
     * in the first column of the CSV-file as integers, the second column as strings 
     * and the third column as floats
     * 
     * @param types The types corresponding to each column
     * @return This CSVReader instance
     */
    public CSVReader useColumnTypes(final Class<?>... types){
        if((types != null) && !isNullType(types[0])){
            inferColumnTypes(types);
        }else{
            this.types = null;
        }
        return this;
    }

    /**
     * Reads the CSV data and returns a DataFrame representing its content.<br>
     * This method can only be called once. Subsequent calls will result in an
     * <code>IllegalStateException</code>.<br>
     * If this <code>CSVReader</code> was constructed to use headers, then
     * the first line of the file is used to create the column names of the
     * returned DataFrame.
     * <p>Resources will be closed automatically before this method returns
     * 
     * @return A DataFrame holding the content of the CSV-file read
     * @throws IOException If the file cannot be opened or read, or if the file 
     *                     content is improperly formatted
     * @throws IllegalStateException If this method has already been called
     */
    private DataFrame read0() throws IOException{
        final BufferedReader reader = createReader();
        this.is = null;
        this.file = null;

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
                    final String[] blocks = pattern.split(process(line), 0);
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
                    df.addRow((Object[])first);
                }
                ++lineIndex;
                while((line = reader.readLine()) != null){
                    ++lineIndex;
                    if(line.isEmpty()){//skip empty lines
                        continue;
                    }
                    final String[] blocks = pattern.split(process(line), 0);
                    for(int i=0; i<blocks.length; ++i){
                        if(blocks[i].equals("null")){
                            blocks[i] = null;
                        }else{
                            blocks[i] = normalize(blocks[i]);
                        }
                    }
                    try{
                        df.addRow((Object[])blocks);
                    }catch(DataFrameException ex){//null value in row
                        df = DataFrame.convert(df, NullableDataFrame.class);
                        df.addRow((Object[])blocks);
                    }
                }
            }
        }catch(RuntimeException ex){
            throw new IOException(String.format(
                    "Improperly formatted CSV file at line: %s", lineIndex), ex);

        }finally{
            reader.close();
        }
        return df;
    }

    /**
     * Creates a BufferedReader for this CSVReader instance
     * 
     * @return A <code>BufferedReader</code> for reading characters
     *         from an input stream or file
     * @throws FileNotFoundException If this CSVReader was constructed to
     *                               use a File object and the corresponding
     *                               file was not found
     * @throws IllegalStateException If this method has already been called
     */
    private BufferedReader createReader() throws FileNotFoundException{
        if(file != null){
            return new BufferedReader(
                    new InputStreamReader(new FileInputStream(this.file), charset));

        }else if(is != null){
            return new BufferedReader(
                    new InputStreamReader(this.is, charset));
        }else{
            throw new IllegalStateException("read() already called");
        }
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
                throw new IllegalArgumentException("Unrecognized type: " + type);	
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
     * Processes a text line and handles malformed formats
     * 
     * @param line The line to process
     * @return A processed line
     */
    private String process(String line){
        if(line.startsWith(separator)){
            line = "null" + line;
        }
        final String s = separator + separator;
        while(line.contains(s)){
            line = line.replaceAll(
                    s,
                    separator + "null" + separator);
        }
        if(line.endsWith(separator)){
            line = line + "null";
        }
        return line;
    }

    /**
     * Normalizes the separator character by escaping special regex characters
     * 
     * @param separator The separator character to normalize
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
     * Ensures that the file specified by this CSVReader exists
     * and is not a directory
     * 
     * @throws FileNotFoundException If the specified file does
     *                               not exist or is a directory
     */
    private void ensureExists() throws FileNotFoundException{
        if((!this.file.exists()) || (this.file.isDirectory())){
            throw new FileNotFoundException(String.format(
                    "File '%s' does not exist or is a directory", file));
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

        private CompletableFuture<DataFrame> future;

        /**
         * Constructs a new <code>ConcurrentCSVReader</code>
         * 
         */
        ConcurrentCSVReader(){
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
                this.future.complete(read());
            }catch(Throwable ex){
                this.future.completeExceptionally(ex);
            }
        }
    }
}
