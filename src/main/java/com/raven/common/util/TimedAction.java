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

import java.util.concurrent.TimeUnit;

/**
 * An {@link Action} used in timed executions. A TimedAction does not
 * implement the execution code itself but rather holds additional information
 * about such which can be accessed and used by concrete implementation code.
 * In addition to the methods specified by the <code>Action</code> interface, 
 * a TimedAction provides access to a reference of the underlying
 * {@link Chronometer} instance which is used to carry out timed executions.
 * A caller might wait for the termination of a TimedAction via
 * the <code>awaitTermination()</code> method. It should block until
 * the TimedAction in question has terminated entirely.
 * 
 * @author Phil Gaiser
 * @see Action
 * @see Actable
 * @see FutureAction
 * @since 3.0.0
 *
 */
public interface TimedAction extends Action {
    
    /**
     * Returns a reference to the Chronometer used to manage a timed
     * execution for this Action
     * 
     * @return A <code>Chronometer</code> responsible for managing
     *         the timing of this Action
     */
    public Chronometer getChronometer();
    
    /**
     * Suspends the calling thread and waits until the timed action
     * terminates either through successful completion or cancellation. If
     * the timed action has already terminated then this method should return
     * immediately
     */
    public void awaitTermination();
    
    /**
     * Suspends the calling thread and waits until the timed action
     * terminates either through successful completion or cancellation or the
     * specified amount of real time has elapsed. If the timed action has already
     * terminated then this method should return immediately
     * 
     * @param time The maximum amount of time to wait for termination
     * @param unit The <code>TimeUnit</code> for the specified maximum amount
     *             of time to wait
     */
    public void awaitTermination(long time, TimeUnit unit);
    
}
