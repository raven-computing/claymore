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
 * An object which can run on a given {@link Action}. Concrete classes
 * implementing this interface must implement the <code>run()</code> method. That
 * method may contain arbitrary code to execute and fulfill a specific intention.
 * An <code>Actable</code> is conceptually similar to a <code>java.lang.Runnable</code>
 * in that both are used to execute actions on a potentially different thread.
 * The <code>run()</code> method of an Actable has an <code>Action</code> as
 * a parameter. This can be used for passing arbitrary arguments
 * to an Actable at runtime.
 * 
 * @author Phil Gaiser
 * @see Action
 * @since 3.0.0
 */
@FunctionalInterface
public interface Actable {
    
    /**
     * Runs instructions on the specified Action. Implementations of this method
     * should return normally after execution. Any occurred exceptions should be
     * handled gracefully by the implementation or, if applicable, any encompassed
     * wrapper objects.
     * The general contract of the <code>run()</code> method is that it may execute
     * on any user defined action
     * 
     * @param action The <code>Action</code> to run on
     */
    public void run(Action action);
    
}
