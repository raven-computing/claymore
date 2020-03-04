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

package com.raven.common.util;

/**
 * Exception thrown when the command line arguments passed to an 
 * <code>ArgumentParser</code> do not match the arguments as they were 
 * constructed through its <code>ArgumentParser.Builder</code> instance.<br>
 * Besides the standard methods provided by <code>Exception</code>, this 
 * class also provides methods to get the erroneous argument that caused
 * the exception to be thrown, and a  hint message to be displayed 
 * to the user.
 * 
 * @since 1.0.0
 *
 */
public class ArgumentParseException extends Exception {

    private static final long serialVersionUID = 1L;

    private String cause;
    private String hint;

    /**
     * Constructs a new <code>ArgumentParseException</code> with null 
     * as its detail message. The cause is not initialized, and may
     * subsequently be initialized by a call to {@link #initCause}
     */
    protected ArgumentParseException(){
        super();
    }

    /**
     * Constructs a new <code>ArgumentParseException</code> with the 
     * specified detail message. The cause is not initialized, and may
     * subsequently be initialized by a call to {@link #initCause}
     *
     * @param message The detail message. The detail message is saved for
     * 				  later retrieval by the {@link #getMessage()} method
     */
    protected ArgumentParseException(String message){
        super(message);
    }

    /**
     * Constructs a new <code>ArgumentParseException</code> with the 
     * specified cause and a detail message of 
     * <tt>(cause==null ? null : cause.toString())</tt> (which
     * typically contains the class and detail message of <tt>cause</tt>)
     *
     * @param cause The cause (which is saved for later retrieval by the
     *         		{@link #getCause()} method). A null value is permitted,
     *              and indicates that the cause is nonexistent or unknown)
     */
    protected ArgumentParseException(Throwable cause){
        super(cause);
    }

    /**
     * Constructs a new <code>ArgumentParseException</code> with the
     * specified detail message and cause.
     * <p>Note that the detail message associated with {@code cause} is
     * <i>not</i> automatically incorporated in this exception's 
     * detail message
     *
     * @param message The detail message
     * @param cause The cause (which is saved for later retrieval by the
     *         		{@link #getCause()} method). A null value is permitted, 
     *         		and indicates that the cause is nonexistent or unknown
     */
    protected ArgumentParseException(String message, Throwable cause){
        super(message, cause);
    }

    /**
     * Constructs a new <code>ArgumentParseException</code> with the 
     * specified detail message, cause, suppression enabled or disabled,
     * and writable stack trace enabled or disabled
     *
     * @param message The detail message
     * @param cause The cause. A null value is permitted, and indicates 
     * 				that the cause is nonexistent or unknown
     * @param enableSuppression Whether or not suppression is enabled
     *                          or disabled
     * @param writableStackTrace Whether or not the stack trace should
     *                           be writable
     */
    protected ArgumentParseException(String message, Throwable cause,
            boolean enableSuppression, boolean writableStackTrace){

        super(message, cause, enableSuppression, writableStackTrace);
    }

    /**
     * Returns a detailed hint about all the arguments the underlying 
     * <code>ArgumentParser</code> instance can handle.<br> The returned
     * string is formatted and suitable to be displayed to a user
     * 
     * @return A hint showing all arguments the underlying argument parser was
     * 		   set up to handle
     */
    public String hint(){
        return this.hint;
    }

    /**
     * Returns the argument that caused this <code>ArgumentParseException</code>
     * to be thrown
     * 
     * @return The erroneous argument that caused this exception, as a string
     */
    public String cause(){
        return this.cause;
    }

    /**
     * Sets a detailed hint about all the arguments the underlying 
     * <code>ArgumentParser</code> instance can handle
     * 
     * @param hint The hint to show to the user
     */
    protected void setHint(final String hint){
        this.hint = hint;
    }

    /**
     * Sets the argument that caused this <code>ArgumentParseException</code>
     * to be thrown
     * 
     * @param arg The erroneous argument that caused this exception, as a string
     */
    protected void setCause(final String arg){
        this.cause = arg;
    }
}
