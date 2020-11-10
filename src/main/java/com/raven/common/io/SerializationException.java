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

/**
 * A runtime exception to be thrown when an object cannot
 * be serialized or deserialized.
 * 
 * @author Phil Gaiser
 * @since 3.0.0
 *
 */
public class SerializationException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * Constructs a <code>SerializationException</code> with no
     * detail message
     */
    public SerializationException(){
        super();
    }

    /**
     * Constructs a new <code>SerializationException</code> with the
     * specified detail message
     * 
     * @param message The detail message
     */
    public SerializationException(String message){
        super(message);
    }

    /**
     * Constructs a new <code>SerializationException</code> with the specified 
     * cause and a detail message of 
     * <tt>(cause==null ? null : cause.toString())</tt> (which typically contains
     * the class and detail message of <tt>cause</tt>).
     * This constructor is useful for exceptions that are little more than
     * wrappers for other throwables
     * 
     * @param cause The cause (which is saved for later retrieval by the
     *        {@link Throwable#getCause()} method).  (A <tt>null</tt> value
     *        is permitted, and indicates that the cause is nonexistent or
     *        unknown.)
     */
    public SerializationException(Throwable cause){
        super(cause);
    }

    /**
     * Constructs a new <code>SerializationException</code> with the specified 
     * detail message and cause.
     * <p>Note that the detail message associated with <code>cause</code> is
     * <i>not</i> automatically incorporated in this exception's detail
     * message.
     *
     * @param message The detail message (which is saved for later retrieval
     *        by the {@link Throwable#getMessage()} method)
     * @param cause The cause (which is saved for later retrieval by the
     *        {@link Throwable#getCause()} method).  (A <tt>null</tt> value
     *        is permitted, and indicates that the cause is nonexistent or
     *        unknown.)
     */
    public SerializationException(String message, Throwable cause){
        super(message, cause);
    }

    /**
     * Constructs a new <code>SerializationException</code> with the specified detail
     * message, cause, suppression enabled or disabled, and writable stack trace 
     * enabled or disabled
     *
     * @param message The detail message
     * @param cause The cause. (a null value is permitted, and indicates that the
     *        cause is nonexistent or unknown)
     * @param enableSuppression Whether or not suppression is enabled or disabled
     * @param writableStackTrace Whether or not the stack trace should be writable
     */
    public SerializationException(String message, Throwable cause, 
            boolean enableSuppression, boolean writableStackTrace) {

        super(message, cause, enableSuppression, writableStackTrace);
    }
}
