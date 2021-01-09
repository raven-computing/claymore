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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.util.concurrent.CompletableFuture;

import com.raven.common.struct.DataFrame;

/**
 * Convenience class for writing CSV-files. A CSVWriter uses the <i>UTF-8</i>
 * character encoding and a comma as a separator by default.<br>
 * To specify these values yourself, call <code>useCharset()</code> and
 * <code>useSeparator()</code> respectively.
 * 
 * <p>If not further specified, a CSVWriter will write a header to the CSV-file
 * if the corresponding DataFrame has any column names set. This behaviour can be
 * controlled with the <code>withHeader()</code> method.
 * 
 * <p>There is only one restriction regarding the separator as specified
 * by the <code>useSeparator()</code> method. Any character except <i>double quotes</i>
 * can be used as a separator. If a data value contains one or more instances of the
 * used separator character, then that data value will be enclosed with double quotes.
 * For that reason <i>double quotes</i> cannot be used as a separator character or
 * occur inside data values.
 * 
 * <p>If the DataFrame to write contains nullable values, then any null values will be
 * represented as a <i>"null"</i> string.
 * 
 * <p>A CSVWriter may also be constructed to write to any <code>OutputStream</code>
 * passed to the constructor. Any output stream will be automatically wrapped and buffered
 * by a <code>BufferedWriter</code> instance. All closable resources will be automatically
 * closed by a CSVWriter after a write operation.
 * 
 * @author Phil Gaiser
 * @see CSVReader
 * @see DataFrameSerializer
 * @since 1.0.0
 *
 */
public class CSVWriter {

    private OutputStream os;
    private File file;
    private String separator = ",";
    private Charset charset = StandardCharsets.UTF_8;
    private boolean writeHeader = true;

    /** Used for concurrent write operations **/
    private ConcurrentCSVWriter async;

    /**
     * Constructs a new <code>CSVWriter</code> for the specified file
     * 
     * @param file The file to write. May be a path to a file. Must not be null
     */
    public CSVWriter(final String file){
        if((file == null) || (file.isEmpty())){
            throw new IllegalArgumentException(
                    "File argument must not be null or empty");
        }
        this.file = new File(file);
    }

    /**
     * Constructs a new <code>CSVWriter</code> for the specified file object
     * 
     * @param file The {@link File} to write. Must not be null
     */
    public CSVWriter(final File file){
        if(file == null){
            throw new IllegalArgumentException(
                    "File argument must not be null");
        }
        this.file = file;
    }

    /**
     * Constructs a new <code>CSVWriter</code> for writing to the specified
     * <code>OutputStream</code>.<br>
     * The stream will be automatically closed after the write operation
     * 
     * @param os The <code>OutputStream</code> to write to. Must not be null
     */
    public CSVWriter(final OutputStream os){
        if(os == null){
            throw new IllegalArgumentException(
                    "OutputStream argument must not be null");
        }
        this.os = os;
    }

    /**
     * Writes the content of the specified DataFrame to the file or output
     * stream in a CSV format.<br>
     * The entries of each column are separated by the standard separator,
     * unless directly specified by <code>useSeparator()</code>
     * 
     * <p>This method can only be called once. Subsequent calls will result in an
     * <code>IllegalStateException</code>.
     * 
     * <p>Resources will be closed automatically before this method returns
     * 
     * @param df The DataFrame to write to a CSV-file or output stream.
     *           Must not be null
     * @throws IOException If the file cannot be opened or written
     * @throws IllegalStateException If this method has already been called
     * @see #writeAsync(DataFrame)
     */
    public void write(final DataFrame df) throws IOException{
        if(df == null){
            throw new IllegalArgumentException("DataFrame must not be null");
        }
        final BufferedWriter writer = createWriter();
        this.os = null;
        this.file = null;

        try{
            final String nl = System.lineSeparator();
            final int cols = df.columns();//cache

            //Add header if available and requested
            if(df.hasColumnNames() && writeHeader){
                final String[] names = df.getColumnNames();
                for(int i=0; i<names.length; ++i){
                    writer.write(escape(names[i]));
                    if(i<names.length-1){
                        writer.write(separator);
                    }
                }
                writer.write(nl);
            }
            //Add rows
            for(int i=0; i<df.rows(); ++i){
                final Object[] row = df.getRow(i);
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
     * Creates a background thread which will asynchronously write the specified
     * DataFrame to the file or output stream in a CSV format.<br>
     * The CompletableFuture returned by this method does not return
     * anything upon completion.<br>
     * 
     * This method can only be called once. Subsequent calls will result in an
     * <code>IllegalStateException</code>.<br>
     * 
     * <p>This method is meant to be used for large DataFrames/CSV-files.<br>
     * 
     * <p>When called, this method will return immediately.
     * 
     * <p>Please note that any IOExceptions encountered by the launched background 
     * thread will result in the CompletableFuture being completed exceptionally.
     * 
     * @param df The DataFrame to write to a CSV-file or output stream.
     *           Must not be null
     * @return A <code>CompletableFuture</code> for the asynchronous write operation
     * @throws IllegalStateException If this method has already been called
     * @see #write(DataFrame)
     */
    public CompletableFuture<Void> writeAsync(DataFrame df) throws IllegalStateException{
        if(async != null){
            throw new IllegalStateException("writeAsync() already called");
        }
        this.async = new ConcurrentCSVWriter(df);
        return this.async.execute();
    }

    /**
     * Instructs this <code>CSVWriter</code> to use the specified separator when
     * writing to the CSV-file or output stream.<br>
     * The default separator is a comma (','). 
     * 
     * @param separator The character to be used as a separator
     * @return This CSVWriter instance
     */
    public CSVWriter useSeparator(final char separator){
        if(separator == '"'){
            throw new IllegalArgumentException(
                    "Cannot use double quotes as separator character");
        }
        this.separator = String.valueOf(separator);
        return this;
    }

    /**
     * Instructs this <code>CSVWriter</code> whether to write the column names to
     * the first line of the CSV data as a header
     * 
     * @param writeHeader A boolean value specifiying whether to write
     *                   a header to the first line
     * @return This CSVWriter instance
     */
    public CSVWriter withHeader(final boolean writeHeader){
        this.writeHeader = writeHeader;
        return this;
    }

    /**
     * Instructs this <code>CSVWriter</code> to use the specified Charset when
     * writing CSV data.<br>
     * For example: <code>"UTF-8"</code>, which is unicode encoded as UTF-8
     * 
     * @param charset The charset to be used when writing CSV data
     * @return This CSVWriter instance
     * @throws IllegalCharsetNameException If the given charset name is illegal
     * @throws UnsupportedCharsetException If the named charset is not
     *                                     supported in the underlying JVM
     * @see #useCharset(Charset)
     */
    public CSVWriter useCharset(final String charset)
            throws UnsupportedCharsetException, IllegalCharsetNameException{

        this.charset = ((charset != null)
                ? Charset.forName(charset)
                        : StandardCharsets.UTF_8);

        return this;
    }

    /**
     * Instructs this <code>CSVWriter</code> to use the specified Charset when
     * writing CSV data.<br>
     * For example: <code>StandardCharsets.UTF_8</code>, which is
     * unicode encoded as UTF-8
     * 
     * @param charset The charset to be used when writing CSV data
     * @return This CSVWriter instance
     * @see #useCharset(String)
     */
    public CSVWriter useCharset(final Charset charset){
        this.charset = ((charset != null)
                ? charset
                        : StandardCharsets.UTF_8);

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
     * Creates a BufferedWriter for this CSVWriter instance
     * 
     * @return A <code>BufferedWriter</code> for writing characters
     *         to an output stream or file
     * @throws FileNotFoundException If the file exists but is a directory rather
     *                               than a regular file, does not exist but cannot
     *                               be created, or cannot be opened for any
     *                               other reason
     * @throws IllegalStateException If this method has already been called
     */
    private BufferedWriter createWriter() throws FileNotFoundException{
        if(file != null){
            return new BufferedWriter(
                    new OutputStreamWriter(new FileOutputStream(this.file), charset));

        }else if(os != null){
            return new BufferedWriter(
                    new OutputStreamWriter(this.os, charset));

        }else{
            throw new IllegalStateException("read() already called");
        }
    }

    /**
     * Background thread for concurrent write operations of CSV-files.
     *
     */
    private class ConcurrentCSVWriter implements Runnable {

        private DataFrame df;
        private CompletableFuture<Void> future;

        /**
         * Constructs a new <code>ConcurrentCSVWriter</code>
         * 
         * @param df The DataFrame to write
         */
        ConcurrentCSVWriter(final DataFrame df){
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
                write(df);
                this.future.complete(null);
            }catch(Throwable ex){
                this.future.completeExceptionally(ex);
            }
        }
    }
}
