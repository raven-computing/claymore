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

package com.raven.common.util;

/**
 * An action to be executed on by an {@link Actable}. An Action does not
 * implement the execution code itself but rather holds additional information
 * about such which can be accessed and used by concrete implementation code.
 * An Action can take an arbitrary Object as its argument. Classes implementing
 * the <code>Actable</code> interface can then query that argument, retrieve
 * additional data and adjust the implementation logic based on that.
 * Furthermore, actables may query the operation status at any time and therefore
 * may decide to interrupt or abort execution if an Action indicates that it
 * has been cancelled. Cancellation can be initiated with the <code>cancel()</code>
 * method and queried with the <code>isCancelled()</code> method. Once an action
 * has been cancelled it cannot be restarted or resumed. Additionally, the current
 * execution status can be obtained with the <code>isRunning()</code> method.<br>
 * An Action may indicate that some task should be carried out more than once.
 * In that case it is considered to be recurrent. A recurrent Action is to be
 * executed more than once by an Actable, where the Actable is responsible to
 * decide when and how to execute on the given Action. The number of iterations
 * an action should be carried out can be queried with the
 * <code>getCount()</code> method. The successful completion of all iterations
 * may be queried with the <code>isCompleted()</code> method. A cancelled action
 * should never be considered completed.
 * 
 * @author Phil Gaiser
 * @see Actable
 * @since 3.0.0
 */
public interface Action {

    /**
     * Returns the argument of this action as a type denoted by the the specified
     * Class object
     * 
     * @param <T> The type of the argument to be returned
     * @param classOfArg The Class object of the type <code>T</code> to be returned
     * @return The argument of this Action as an object of type <code>T</code> 
     * @throws ClassCastException If the argument of this Action cannot be
     *                            cast to the specified type
     */
    public <T> T getArgument(Class<T> classOfArg) throws ClassCastException;

    /**
     * Indicates whether this action has been cancelled. Once an action
     * is cancelled, it cannot be resumed. A completed action should never
     * be marked as cancelled
     * 
     * @return True if this Action has been cancelled, false if this Action
     *         has not been cancelled
     */
    public boolean isCancelled();

    /**
     * Cancels this action. Concrete actions may interrupt an already
     * running execution or concede completion. If this action is recurrent,
     * then all future runs are guaranteed to be cancelled after
     * this method returns
     */
    public void cancel();

    /**
     * Indicates whether this action is recurrent. A recurrent action is
     * repetitive in that it will be run mutliple times, possibly with an
     * intermittence between consecutive runs
     * 
     * @return True if this Action is recurrent. Returns false if this
     *         Action is not recurrent and therefore only run once
     * @see #getCount()
     */
    public boolean isRecurrent();

    /**
     * Returns the number of remaining iterations this action is run for.
     * For actions which are only run once this method returns 1 if the
     * underlying action has not yet started execution. For recurrent actions
     * this method indicates the remaining number of runs which
     * have not yet been started
     * 
     * @return The number of remaining runs of this Action
     * @see #isRecurrent()
     */
    public long getCount();

    /**
     * Indicates whether this action is currently running
     * 
     * @return True if this action is running at the time this method is called.
     *         False if this action is not running
     */
    public boolean isRunning();

    /**
     * Indicates whether this action has completed all its executions
     * 
     * @return True if this action has completed all its executions at the time
     *         this method is called. False if this action has not yet completed
     *         and has pending executions at the time this method is called
     */
    public boolean isCompleted();

    /**
     * Indicates whether this action has terminated all its executions. This method
     * should return true if the action is either cancelled or has
     * completed successfully
     * 
     * @return True if this action has terminated either through cancellation or
     *         successful completion of all its executions at the time this method
     *         is called. False if this action has not yet terminated and has
     *         pending executions at the time this method is called
     */
    public boolean isTerminated();

}
