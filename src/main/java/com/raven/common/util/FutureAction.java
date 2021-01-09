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

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * An {@link Action} to be executed by a {@link Chronometer} at some specified
 * point in the future. A FutureAction can be configured in a number of ways
 * to specify execution behaviour. It can be singular so that it is only carried
 * out once at a specific time or it may be recurrent which causes a FutureAction
 * to be repetitively executed multiple times in the future. The exact number of
 * times a FutureAction should be executed can be specified by calling
 * the {@link #setCount(long)} method. Setting a count greater than one causes a
 * FutureAction to be recurrent.<br>
 * A concrete <code>Executor</code> can be specified by calling
 * the {@link #executedBy(Executor)} method. Doing so will cause the FutureAction
 * to be submitted to that Executor by the time the action should be carried out.
 * <br>An argument to be used when executing a FutureAction can be sepcified by
 * calling the {@link #with(Object)} method. The argument can be any arbitrary
 * Object and may later be recieved by the executing thread
 * with the {@link #getArgument(Class)} method.
 * 
 * <p>A FutureAction can be conveniently created by using one of the
 * provided static utility methods. For example, the follwing code creates a
 * FutureAction which executes the specified <code>Runnable</code> in 5 seconds
 * in the future when given to a Chronometer instance:
 * <p><pre><code>
 * FutureAction action = FutureAction.in(5, TimeUnit.SECONDS, runnable);
 * </code></pre>
 * 
 * Equivalently, the following code createa a <b>recurring</b> FutureAction which
 * executes the specified <code>Runnable</code> after every 5 seconds:
 * <p><pre><code>
 * FutureAction action = FutureAction.every(5, TimeUnit.SECONDS, runnable);
 * </code></pre>
 * 
 * <p>More static utility methods are provided which can create FutureActions that
 * run at a specific time in the future, once or recurrently. All static methods
 * for creating FutureActions have an equivalent method which takes an
 * {@link Actable} as an argument instead of a <code>Runnable</code>. The executed
 * FutureAction is then passed as an <code>Action</code> argument to the
 * <code>run()</code> method of that Actable.
 * 
 * <p>A FutureAction can be cancelled prior its execution by invoking
 * the {@link #cancel()} method. If an action is cancelled while it is already
 * executing, the running execution will not be interrupted in any way but future
 * repetitions of recurring actions will be aborted. The cancellation status can
 * be queried with the {@link #isCancelled()} method. Once a FutureAction has been
 * cancelled it cannot be resumed or restarted.
 * 
 * <p>Time scheduling of a FutureAction heavily depends on the system time as
 * provided by <code>System.currentTimeMillis()</code>. If the clock of the
 * underlying system experiences an unpredictable change then a FutureAction may
 * or may not run in time and the timed results of a FutureAction are undefined.
 * 
 * <p>The monitor of a FutureAction should not be used for synchronization.
 * The termination of a FutureAction, either through successful completion or
 * cancellation, can be waited for with the {@link #awaitTermination()} method.
 * 
 * <p>This implementation is thread-safe.
 * 
 * @author Phil Gaiser
 * @see Action
 * @see TimedAction
 * @see Actable
 * @since 3.0.0
 *
 */
public abstract class FutureAction implements Runnable, Actable, TimedAction {

    /**
     * Specifies that a <code>FutureAction</code> should be executed
     * only once, that is one time
     */
    public static final int ONCE = 1;

    /**
     * Specifies that a <code>FutureAction</code> should be executed indefinitely
     * until either its <code>cancel()</code> method is invoked or the
     * underlying Chronometer instance is stopped
     */
    public static final int INDEFINITE = -100;

    private Executor executor;
    private Chronometer chron;
    private Object argument;
    private AtomicLong destiny;
    private AtomicLong interval;
    private AtomicLong count;
    private AtomicLong isRunning;
    private AtomicBoolean isCancelled;
    private ScheduledFuture<?> future;

    /**
     * Constructs a new <code>FutureAction</code> to be executed in the
     * specified amount of milliseconds in the future for the specified
     * number of times
     * 
     * @param destiny The time in milliseconds that should elapse before the
     *                constructed FutureAction gets executed. Must not be negative
     * @param count The number of times the constructed FutureAction should
     *              be executed. May be {@link FutureAction#INDEFINITE} to
     *              specify that the constructed action should be recurrent and
     *              executed indefinitely
     */
    public FutureAction(final long destiny, final long count){
        if(destiny < 0){
            throw new IllegalArgumentException("FutureAction cannot start in the past: "
                                               + destiny);
        }
        this.destiny = new AtomicLong(destiny);
        this.interval = new AtomicLong(destiny);
        this.count = new AtomicLong(1L);
        this.isRunning = new AtomicLong();
        this.isCancelled = new AtomicBoolean();
        this.setCount(count);
    }

    /**
     * Returns the argument of this FutureAction as a type denoted by 
     * the specified Class object
     * 
     * @param <T> The type of the argument to be returned
     * @param classOfArg The Class object of the type <code>T</code> to be returned
     * @return The argument of this FutureAction as an object of type <code>T</code>
     * @throws ClassCastException If the argument of this FutureAction cannot be
     *                            cast to the specified type
     */
    @Override
    public <T> T getArgument(final Class<T> classOfArg) throws ClassCastException{
        return classOfArg.cast(this.argument);
    }

    /**
     * Indicates whether this FutureAction has been cancelled. Once an action
     * is cancelled it cannot be resumed. A completed action is never marked
     * as cancelled
     * 
     * @return True if this FutureAction has been cancelled, false if
     *         this FutureAction has not been cancelled
     * @see #isCompleted()
     * @see #isTerminated()
     */
    @Override
    public boolean isCancelled(){
        return (isCancelled.get() && !isCompleted());
    }

    /**
     * Cancels this FutureAction. Already started runs are not interrupted.
     * If this action is not recurring and is already running then this call
     * has no effect. If this action is recurrent, then all future runs
     * are guaranteed to be cancelled after this method returns
     */
    @Override
    public void cancel(){
        this.isCancelled.set(true);
        if(!isRunning()){
            finish();
        }
    }

    /**
     * Indicates whether this FutureAction is recurrent. A recurrent action is
     * repetitive in that it will be run mutliple times after the specified
     * time of this FutureAction
     * 
     * @return True if this FutureAction is recurrent. Returns false if this
     *         FutureAction is not recurrent and therefore only run once at the
     *         specified time in the future
     * @see #getCount()
     */
    @Override
    public boolean isRecurrent(){
        return this.count.get() != 1;
    }

    /**
     * Returns the number of remaining iterations this FutureAction is run for.
     * For actions which are only run once this method returns 1 if the underlying
     * FutureAction has not yet started execution. For recurrent actions this method
     * indicates the remaining number of runs which have not yet been started
     * 
     * @return The number of remaining runs of this FutureAction. Returns
     *         {@link FutureAction#INDEFINITE} if this FutureAction is configured
     *         to run indefinitely
     * @see #isRecurrent()
     * @see #isCompleted()
     */
    @Override
    public long getCount(){
        return this.count.get();
    }

    /**
     * Returns a reference to the Chronometer used to manage a timed
     * execution for this FutureAction
     * 
     * @return A <code>Chronometer</code> responsible for managing
     *         the timing of this FutureAction or null if this FutureAction
     *         is not yet associated with a Chronometer 
     */
    @Override
    public Chronometer getChronometer(){
        return this.chron;
    }

    /**
     * Suspends the calling thread and waits until the FutureAction terminates
     * either through successful completion or cancellation. This method
     * essentially blocks until {@link #isTerminated()} returns true. If
     * the FutureAction has already terminated then this method returns
     * immediately
     * 
     * @see #awaitTermination(long, TimeUnit)
     */
    @Override
    public synchronized void awaitTermination(){
        awaitTermination0();
    }

    /**
     * Suspends the calling thread and waits until the FutureAction
     * terminates either through successful completion or cancellation or the
     * specified amount of real time has elapsed. This method essentially blocks
     * until either {@link #isTerminated()} returns true or the specified time
     * has elapsed. If the FutureAction has already terminated then this
     * method returns immediately
     * 
     * @param time The maximum amount of time to wait for termination
     * @param unit The <code>TimeUnit</code> for the specified maximum amount
     *             of time to wait
     */
    @Override
    public synchronized void awaitTermination(long time, TimeUnit unit){
        awaitTermination0(unit.toMillis(time));
    }

    /**
     * Indicates whether this FutureAction is currently running
     * 
     * @return True if this FutureAction is running at the time this method
     *         is called. False if this FutureAction is not running
     */
    @Override
    public boolean isRunning(){
        return this.isRunning.get() > 0;
    }

    /**
     * Indicates whether this FutureAction has completed all its executions.
     * A cancelled FutureAction which had pending executions at the time it was
     * cancelled is not considered completed. A FutureAction which runs
     * indefinitely is never considered completed
     * 
     * @return True if this FutureAction has completed all its executions at
     *         the time this method is called. False if this FutureAction has
     *         not yet completed and has pending executions at the time this
     *         method is called
     * @see #getCount()
     * @see #isCancelled()
     * @see #isTerminated()
     */
    @Override
    public boolean isCompleted(){
        return (!isRunning() && count.get() == 0);
    }

    /**
     * Indicates whether this FutureAction has terminated all its executions.
     * This method returns true if the FutureAction is either cancelled or has
     * completed successfully
     * 
     * @return True if this FutureAction has terminated either through
     *         cancellation or successful completion of all its executions at the
     *         time this method is called. False if this FutureAction has not yet
     *         terminated and has pending executions at the time this
     *         method is called
     * @see #isCancelled()
     * @see #isCompleted()
     */
    @Override
    public boolean isTerminated(){
        return (!isRunning() && ((count.get() == 0) || isCancelled.get()));
    }

    /**
     * Sets the number of times this FutureAction should be run in the future.<br>
     * If this FutureAction is already completed or has been cancelled, then setting
     * a new count number has no effect  
     * 
     * @param count The number of runs of this FutureAction.<br>
     *              Must be positive or {@link FutureAction#INDEFINITE}
     * @return This <code>FutureAction</code> instance
     */
    public FutureAction setCount(final long count){
        if((count <= 0) && (count != INDEFINITE)){
            throw new IllegalArgumentException("Invalid count argument: " + count);
        }
        if(!isCancelled() && !isCompleted()){
            this.count.set(count);
        }
        return this;
    }

    /**
     * Specifies the Executor to be used for running this FutureAction with.
     * If this method is passed null, then this FutureAction will be executed on
     * the watcher thread which is used for managing execution timing. In that case
     * the FutureAction should not perform computationally expensive work or
     * blocking calls as that might cause future actions to not be run in time.
     * 
     * <p>If an Executor is specified, then this FutureAction will be submitted
     * to it at the configured time. Please note that this does not guarantee the
     * precise execution at the configured time as the specified Executor might not
     * have a spare thread at its immediate disposal
     * 
     * @param executor The <code>Executor</code> to be used by this FutureAction.
     *                 May be null
     * @return This <code>FutureAction</code> instance
     */
    public FutureAction executedBy(final Executor executor){
        this.executor = executor;
        return this;
    }

    /**
     * Specifies the argument of this FutureAction. During execution, the object
     * passed to this method can be retrieved
     * via the {@link #getArgument(Class)} method
     * 
     * @param argument The argument of this FutureAction. May be any Object
     * @return This <code>FutureAction</code> instance
     */
    public FutureAction with(final Object argument){
        this.argument = argument;
        return this;
    }

    /**
     * Sets the initial delay of this FutureAction to the specified
     * amount of time. This value can only be set as long as this FutureAction
     * has not been passed to a Chronometer for timed execution. If this
     * FutureAction has already been submitted to a Chronometer, then this
     * method has no effect.
     * 
     * @param time The amount of time units to pass before
     *             the FutureAction is run for the first time
     * @param unit The time unit to use for the FutureAction
     * @return This <code>FutureAction</code> instance
     */
    public FutureAction after(final long time, final TimeUnit unit){
        this.destiny.set(unit.toMillis(time));
        return this;
    }

    /**
     * Runs this FutureAction now. This method should usually not be called
     * directly by API users. This method gets automatically called by the
     * underlying watcher thread which causes the configured Runnable
     * or Actable of this FutureAction to be either directly executed on the
     * watcher thread or submitted to an Executor if set.
     */
    @Override
    public final void run(){
        if(!isCancelled()){
            if(count.get() == INDEFINITE){
                run1();
            }else{
                if(getCountAndDecrement() > 0){
                    run1();
                }else if(count.get() == 0){
                    finish();
                }
            }
        }
    }

    /**
     * Sets the Chronometer to be referenced by this FutureAction
     * 
     * @param chron The <code>Chronometer</code> to be referenced
     *              by this FutureAction
     */
    protected void setChronometer(final Chronometer chron){
        this.chron = chron;
    }

    /**
     * Sets the ScheduledFuture object of this FutureAction
     * 
     * @param futureTask The <code>ScheduledFuture</code> to be used by
     *                   this FutureAction
     */
    protected void setScheduledFuture(final ScheduledFuture<?> futureTask){
        this.future = futureTask;
    }

    /**
     * Sets the cancelled status of this FutureAction to the specified value
     * 
     * @param value The boolean value to set the cancelled status of
     *              this FutureAction to
     */
    protected void setCancelled(final boolean value){
        this.isCancelled.set(value);
    }

    /**
     * Gets the Executor of this FutureAction
     * 
     * @return The <code>Executor</code> of this FutureAction. May be null
     */
    protected Executor getExecutor(){
        return this.executor;
    }

    /**
     * Gets the initial delay of this FutureAction in milliseconds
     * 
     * @return The amount of time in milliseconds that has to elapse before
     *         this FutureAction runs for the first time
     */
    protected long destiny(){
        return this.destiny.get();
    }

    /**
     * Gets the interval between recurrent executions in milliseconds
     * 
     * @return The amount of time in milliseconds that has to elapse between
     *         recurrent executions of this FutureAction
     */
    protected long interval(){
        return this.interval.get();
    }

    /**
     * Sets the interval of this FutureAction to the specified value
     * 
     * @param interval The value to set the interval to. In milliseconds
     * @return This <code>FutureAction</code> instance
     */
    protected FutureAction setInterval(final long interval){
        this.interval.set(interval);
        return this;
    }

    private void run1(){
        if(executor != null){
            this.executor.execute(() -> run0());
        }else{
            run0();
        }
    }

    private void run0(){
        this.isRunning.incrementAndGet();
        try{
            run(FutureAction.this);
        }finally{
            this.isRunning.decrementAndGet();
            if((count.get() == 0) || isCancelled.get()){
                finish();
            }
        }
    }

    private void finish(){
        if(future != null){
            if(!future.cancel(false)){
                if(chron != null){
                    this.chron.watcher.remove(this);
                }
            }
        }
        markTerminated();
    }

    private long decrement(final long input){
        return ((input > 0) ? (input - 1) : input);
    }

    private long getCountAndDecrement(){
        long prev, next;
        do{
            prev = count.get();
            next = decrement(prev);
        }while(!count.compareAndSet(prev, next));
        return prev;
    }

    private void awaitTermination0(){
        while(!isTerminated()){
            try{
                this.wait(0);
            }catch(InterruptedException ex){ }
        }
    }

    private void awaitTermination0(long timeout){
        final long limit = now() + timeout;
        while(!isTerminated() && (timeout > 0)){
            try{
                this.wait(timeout);
            }catch(InterruptedException ex){
                //interrupted
            }finally{
                timeout = limit - now();
            }
        }
    }

    private synchronized void markTerminated(){
        this.notifyAll();
    }

    /**
     * Creates a <code>FutureAction</code> which is scheduled to run in
     * the specified amount of time units in the future. The specified
     * <code>Actable</code> will be executed once
     * 
     * @param time The amount of time units to pass before
     *             the FutureAction is run
     * @param unit The time unit to use for the FutureAction
     * @param actable The action to be executed in the future
     * @return A <code>FutureAction</code> which runs the specified
     *         <code>Actable</code> once at the specified point
     *         in the future
     */
    public static FutureAction in(final long time, final TimeUnit unit,
            final Actable actable){
        
        return new FutureAction(unit.toMillis(time), ONCE){
            @Override
            public void run(Action action){
                actable.run(this);
            }
        };
    }

    /**
     * Creates a <code>FutureAction</code> which is scheduled to run in
     * the specified amount of time units in the future. The specified
     * <code>Runnable</code> will be executed once
     * 
     * @param time The amount of time units to pass before
     *             the FutureAction is run
     * @param unit The time unit to use for the FutureAction
     * @param runnable The action to be executed in the future
     * @return A <code>FutureAction</code> which runs the specified
     *         <code>Runnable</code> once at the specified point
     *         in the future
     */
    public static FutureAction in(final long time, final TimeUnit unit,
            final Runnable runnable){
        
        return new FutureAction(unit.toMillis(time), ONCE){
            @Override
            public void run(Action action){
                runnable.run();
            }
        };
    }

    /**
     * Creates a recurrent <code>FutureAction</code> which is scheduled to
     * run indefinitely in the specified amount of time units in the future.
     * The specified <code>Actable</code> will be first executed after the
     * specified amount of time has elapsed and then indefinitely each time
     * after the specified amount of time has elapsed. The exact execution
     * count can be adjusted with the {@link #setCount(long)} method. The
     * initial delay can be adjusted with the {@link #after(long, TimeUnit)}
     * method. The execution of the FutureAction can be stopped by calling
     * its {@link #cancel()} method
     * 
     * @param time The amount of time units to pass before
     *             the FutureAction is run
     * @param unit The time unit to use for the FutureAction
     * @param actable The action to be executed in the future
     * @return A <code>FutureAction</code> which runs the specified
     *         <code>Actable</code> at the specified point in the future
     *         for an unlimited number of times
     */
    public static FutureAction every(final long time, final TimeUnit unit,
            final Actable actable){
        
        return new FutureAction(unit.toMillis(time), INDEFINITE){
            @Override
            public void run(Action action){
                actable.run(this);
            }
        };
    }

    /**
     * Creates a recurrent <code>FutureAction</code> which is scheduled to
     * run indefinitely in the specified amount of time units in the future.
     * The specified <code>Runnable</code> will be first executed after the
     * specified amount of time has elapsed and then indefinitely each time
     * after the specified amount of time has elapsed. The exact execution
     * count can be adjusted with the {@link #setCount(long)} method. The
     * initial delay can be adjusted with the {@link #after(long, TimeUnit)}
     * method. The execution of the FutureAction can be stopped by calling
     * its {@link #cancel()} method
     * 
     * @param time The amount of time units to pass before
     *             the FutureAction is run
     * @param unit The time unit to use for the FutureAction
     * @param runnable The action to be executed in the future
     * @return A <code>FutureAction</code> which runs the specified
     *         <code>Runnable</code> at the specified point in the future
     *         for an unlimited number of times
     */
    public static FutureAction every(final long time, final TimeUnit unit,
            final Runnable runnable){
        
        return new FutureAction(unit.toMillis(time), INDEFINITE){
            @Override
            public void run(Action action){
                runnable.run();
            }
        };
    }

    /**
     * Creates a <code>FutureAction</code> which is scheduled to run once
     * at the specified Instant of time in the future.
     * The specified <code>Actable</code> will be executed once at the
     * specified point in the future.
     * 
     * <p>The returned FutureAction should not be manually made recurrent by
     * adjusting its count number. Changing the count with the
     * {@link #setCount(long)} method will have no effect. The execution of
     * the FutureAction can be stopped by calling its {@link #cancel()} method
     * 
     * @param instant The <code>Instant</code> of time at which the specified
     *                action is to be executed
     * @param actable The action to be executed in the future
     * @return A <code>FutureAction</code> which runs the specified
     *         <code>Actable</code> once at the specified point in the future
     * @see #alwaysAt(LocalTime, Actable)
     */
    public static FutureAction at(final Instant instant,
                                  final Actable actable){

        final long destiny = instant.toEpochMilli() - now();
        return new FutureAction(destiny, ONCE){
            @Override
            public void run(Action action){
                actable.run(this);
            }
        };
    }

    /**
     * Creates a <code>FutureAction</code> which is scheduled to run once
     * at the specified Instant of time in the future.
     * The specified <code>Runnable</code> will be executed once at the
     * specified point in the future.
     * 
     * <p>The returned FutureAction should not be manually made recurrent by
     * adjusting its count number. Changing the count with the
     * {@link #setCount(long)} method will have no effect. The execution of
     * the FutureAction can be stopped by calling its {@link #cancel()} method
     * 
     * @param instant The <code>Instant</code> of time at which the specified
     *                action is to be executed
     * @param runnable The action to be executed in the future
     * @return A <code>FutureAction</code> which runs the specified
     *         <code>Runnable</code> once at the specified point in the future
     * @see #alwaysAt(LocalTime, Runnable)
     */
    public static FutureAction at(final Instant instant,
                                  final Runnable runnable){

        final long destiny = instant.toEpochMilli() - now();
        return new FutureAction(destiny, ONCE){
            @Override
            public void run(Action action){
                runnable.run();
            }
        };
    }

    /**
     * Creates a <code>FutureAction</code> which is scheduled to run once
     * at the point of time in the future specified by the ZonedDateTime.
     * The specified <code>Actable</code> will be executed once at the
     * specified point in the future.
     * 
     * <p>The returned FutureAction should not be manually made recurrent by
     * adjusting its count number. Changing the count with the
     * {@link #setCount(long)} method will have no effect. The execution of
     * the FutureAction can be stopped by calling its {@link #cancel()} method
     * 
     * @param time The <code>ZonedDateTime</code> at which the specified
     *             action is to be executed
     * @param actable The action to be executed in the future
     * @return A <code>FutureAction</code> which runs the specified
     *         <code>Actable</code> once at the specified point in the future
     * @see #alwaysAt(LocalTime, Actable)
     */
    public static FutureAction at(final ZonedDateTime time,
                                  final Actable actable){

        return FutureAction.at(time.toInstant(), actable);
    }

    /**
     * Creates a <code>FutureAction</code> which is scheduled to run once
     * at the point of time in the future specified by the ZonedDateTime.
     * The specified <code>Runnable</code> will be executed once at the
     * specified point in the future.
     * 
     * <p>The returned FutureAction should not be manually made recurrent by
     * adjusting its count number. Changing the count with the
     * {@link #setCount(long)} method will have no effect. The execution of
     * the FutureAction can be stopped by calling its {@link #cancel()} method
     * 
     * @param time The <code>ZonedDateTime</code> at which the specified
     *             action is to be executed
     * @param runnable The action to be executed in the future
     * @return A <code>FutureAction</code> which runs the specified
     *         <code>Runnable</code> once at the specified point in the future
     * @see #alwaysAt(LocalTime, Runnable)
     */
    public static FutureAction at(final ZonedDateTime time,
                                  final Runnable runnable){
        
        return FutureAction.at(time.toInstant(), runnable);
    }

    /**
     * Creates a <code>FutureAction</code> which is scheduled to run once
     * at the point of time in the future specified by the LocalTime.
     * The specified <code>Actable</code> will be executed once at the
     * specified time in the future. If the local time of day has already
     * passed by the time this method is called, then the returned FutureAction
     * is scheduled to run once at the specified time on the next day. The
     * returned FutureAction is therefore guaranteed to be run within 24 hours
     * 
     * <p>The returned FutureAction should not be manually made recurrent by
     * adjusting its count number. Changing the count with the
     * {@link #setCount(long)} method will have no effect. The execution of
     * the FutureAction can be stopped by calling its {@link #cancel()} method
     * 
     * @param time The <code>LocalTime</code> at which the specified
     *             action is to be executed
     * @param actable The action to be executed in the future
     * @return A <code>FutureAction</code> which runs the specified
     *         <code>Actable</code> once at the specified point in the future
     * @see #alwaysAt(LocalTime, Actable)
     */
    public static FutureAction at(final LocalTime time,
                                  final Actable actable){
        
        ZonedDateTime date = ZonedDateTime.of(LocalDate.now(), time,
                                              ZoneId.systemDefault());
        
        //Make sure destiny is always in the future
        if(LocalTime.now().isAfter(time)){
            date = date.plus(Duration.ofDays(1));
        }
        return FutureAction.at(date, actable);
    }

    /**
     * Creates a <code>FutureAction</code> which is scheduled to run once
     * at the point of time in the future specified by the LocalTime.
     * The specified <code>Runnable</code> will be executed once at the
     * specified time in the future. If the local time of day has already
     * passed by the time this method is called, then the returned FutureAction
     * is scheduled to run once at the specified time on the next day. The
     * returned FutureAction is therefore guaranteed to be run within 24 hours
     * 
     * <p>The returned FutureAction should not be manually made recurrent by
     * adjusting its count number. Changing the count with the
     * {@link #setCount(long)} method will have no effect. The execution of
     * the FutureAction can be stopped by calling its {@link #cancel()} method
     * 
     * @param time The <code>LocalTime</code> at which the specified
     *             action is to be executed
     * @param runnable The action to be executed in the future
     * @return A <code>FutureAction</code> which runs the specified
     *         <code>Runnable</code> once at the specified point in the future
     * @see #alwaysAt(LocalTime, Runnable)
     */
    public static FutureAction at(final LocalTime time,
                                  final Runnable runnable){
        
        ZonedDateTime date = ZonedDateTime.of(LocalDate.now(), time,
                                              ZoneId.systemDefault());
        
        //Make sure destiny is always in the future
        if(LocalTime.now().isAfter(time)){
            date = date.plus(Duration.ofDays(1));
        }
        return FutureAction.at(date, runnable);
    }

    /**
     * Creates a <code>FutureAction</code> which is scheduled to always run
     * at the time in the future specified by the LocalTime.
     * The specified <code>Actable</code> will be executed always at the
     * specified time of day in the future. If the local time of day has already
     * passed by the time this method is called, then the returned FutureAction
     * is scheduled to first run at the specified time on the next day. The
     * returned FutureAction is therefore guaranteed to be first run
     * within 24 hours.
     * 
     * <p>The exact execution count can be adjusted with the
     * {@link #setCount(long)} method. The execution of the FutureAction can
     * be stopped by calling its {@link #cancel()} method
     * 
     * @param time The <code>LocalTime</code> at which the specified
     *             action is to be executed
     * @param actable The action to be executed in the future
     * @return A <code>FutureAction</code> which runs the specified
     *         <code>Actable</code> always at the specified time in the future
     * @see #at(LocalTime, Actable)
     */
    public static FutureAction alwaysAt(final LocalTime time,
                                        final Actable actable){

        ZonedDateTime date = ZonedDateTime.of(LocalDate.now(), time,
                ZoneId.systemDefault());

        final Duration oneDay = Duration.ofDays(1);
        //Make sure destiny is always in the future
        if(LocalTime.now().isAfter(time)){
            date = date.plus(oneDay);
        }
        final long destiny = date.toInstant().toEpochMilli() - now();
        return new FutureAction(destiny, INDEFINITE){
            @Override
            public void run(Action action){
                actable.run(this);
            }
        }.setInterval(oneDay.toMillis());
    }

    /**
     * Creates a <code>FutureAction</code> which is scheduled to always run
     * at the time in the future specified by the LocalTime.
     * The specified <code>Runnable</code> will be executed always at the
     * specified time of day in the future. If the local time of day has already
     * passed by the time this method is called, then the returned FutureAction
     * is scheduled to first run at the specified time on the next day. The
     * returned FutureAction is therefore guaranteed to be first run
     * within 24 hours.
     * 
     * <p>The exact execution count can be adjusted with the
     * {@link #setCount(long)} method. The execution of the FutureAction can
     * be stopped by calling its {@link #cancel()} method
     * 
     * @param time The <code>LocalTime</code> at which the specified
     *             action is to be executed
     * @param runnable The action to be executed in the future
     * @return A <code>FutureAction</code> which runs the specified
     *         <code>Runnable</code> always at the specified time in the future
     * @see #at(LocalTime, Runnable)
     */
    public static FutureAction alwaysAt(final LocalTime time,
                                        final Runnable runnable){

        ZonedDateTime date = ZonedDateTime.of(LocalDate.now(), time,
                ZoneId.systemDefault());

        final Duration oneDay = Duration.ofDays(1);
        //Make sure destiny is always in the future
        if(LocalTime.now().isAfter(time)){
            date = date.plus(oneDay);
        }
        final long destiny = date.toInstant().toEpochMilli() - now();
        return new FutureAction(destiny, INDEFINITE){
            @Override
            public void run(Action action){
                runnable.run();
            }
        }.setInterval(oneDay.toMillis());
    }

    /**
     * Returns the current time in milliseconds
     * 
     * @return The current time, in epoch milliseconds
     */
    private static long now(){
        return System.currentTimeMillis();
    }
}
