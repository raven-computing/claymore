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

import java.time.Duration;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * A chronometer that can measure time, notify you of time-based events and
 * asynchronously execute actions in the future.
 * 
 * <p>Time can be measured by constructing a <code>Chronometer</code> through
 * its default constructor and then calling {@link #start()} at the point in 
 * time you wish to begin the measurement. Similarly, the {@link #stop()}
 * method stops time measurement. 
 * 
 * <p>You can then call one of the <code>elapsed*()</code> methods to get the 
 * absolute time measured. Doing this while the chronometer is still running
 * will simply return the time from the start, up to that point.
 * 
 * <p>A Chronometer can be used to execute a {@link FutureAction}. In order
 * to do that, a FutureAction must be created and given to
 * the {@link #execute(FutureAction)} method of a Chronometer instance. Once
 * a FutureAction has been submitted, the configured initial and subsequent
 * delay cannot be changed. A FutureAction can always be cancelled by invoking
 * its <code>cancel()</code> method directly or stopping the underlying Chronometer
 * instance altogether.
 * 
 * <p>When submitting a FutureAction to a Chronometer, the timed execution of
 * the action is scheduled with a background daemon thread, a so called
 * watcher thread. There can only be one watcher thread for each
 * Chronometer instance. A watcher thread is only created for a Chronometer when
 * a FutureAction is submitted for timed execution or if a Chronometer is created
 * explicitly with a permanent watcher thread. A permanent watcher thread can only
 * be terminated by stopping the underlying Chronometer instance. Therefore
 * clearing all references to a Chronometer with a permanent watcher thread creates
 * a memory leak as the background thread will never stop. It is generally the
 * responsibility of the caller to properly stop a Chronometer. As a default
 * behaviour, a Chronometer will only create a temporary watcher thread if required.
 * A temporary watcher thread will terminate itself and release its underlying
 * resources after 60 seconds of inactivity. Subsequent submissions of FutureAction
 * objects will then start a new temporary watcher thread on demand.
 * 
 * <p>The mode of a background watcher thread can be configured in the
 * Chronometer constructor. The watcher mode is immutable, which means that once
 * a Chronometer is constructed, the mode of the underlying watcher thread
 * cannot be changed. By default, when no mode is specified a temporary watcher
 * thread is only created on demand.
 * 
 * <p>Time measurement of a Chronometer heavily depends on the system time as
 * provided by <code>System.currentTimeMillis()</code>. If the clock of the
 * underlying system experiences an unpredictable change while a Chronometer
 * is running, then the measured results of that Chronometer are undefined.
 * 
 * @author Phil Gaiser
 * @see FutureAction
 * @since 1.0.0
 *
 */
public class Chronometer {

    /**
     * Keeps a watcher thread alive even when idle. When creating a Chronometer
     * with this mode a watcher thread is started and kept running until
     * the <code>stop()</code> method of the Chronometer is called
     */
    public static final boolean WATCHER_MODE_PERMANENT = true;

    /**
     * Causes idle watcher threads to be automatically terminated
     * after 60 seconds of inactivity. Calling the <code>stop()</code> method
     * of the Chronometer always directly terminates a watcher thread 
     */
    public static final boolean WATCHER_MODE_TEMPORARY = false;

    private static final ThreadFactory WATCHER_FACTORY = new WatcherFactory();

    protected Watcher watcher;

    private long t0;
    private long t1;
    private boolean isRunning;

    /**
     * Constructs a new <code>Chronometer</code>.<br>
     * In order to start time measurement, you must call {@link #start()} or
     * execute a {@link FutureAction}.<br>
     * If at some point a background watcher thread is required, it is
     * startd in <i>temporary</i> mode
     * 
     * @see #WATCHER_MODE_TEMPORARY
     */
    public Chronometer(){ }

    /**
     * Constructs a new <code>Chronometer</code> using the specified mode for
     * the created watcher thread. If the mode is <i>permanent</i> then the
     * watcher thread is started immediately and runs until <code>stop()</code>
     * is called. If the <i>temporary</i> mode is specified then a watcher thread
     * is started as soon as a <code>FutureAction</code> is submitted for timed
     * execution
     * 
     * @param watcherMode The mode to apply to the watcher thread. Must be
     *                    either <i>temporary</i> or <i>permanent</i>
     * @see #WATCHER_MODE_TEMPORARY
     * @see #WATCHER_MODE_PERMANENT
     */
    public Chronometer(final boolean watcherMode){
        this.watcher = new Watcher(watcherMode);
    }

    /**
     * Starts time measurement of this <code>Chronometer</code>.<br>
     * Subsequent calls will have no effect
     * 
     * @return This Chronometer instance
     */
    public Chronometer start(){
        if(!isRunning){
            this.isRunning = true;
            t0 = System.currentTimeMillis();
        }
        return this;
    }

    /**
     * Stops time measurement of this <code>Chronometer</code>.<br>
     * Subsequent calls will have no effect
     * 
     * <p>Any FutureActions handled by this Chonrometer will be cancelled and
     * a running watcher thread will be terminated
     * 
     * @return This Chronometer instance
     */
    public Chronometer stop(){
        final long tmp = System.currentTimeMillis();
        if(isRunning){
            t1 = tmp;
            this.isRunning = false;
        }
        if(watcher != null){
            this.watcher.stop();
        }
        return this;
    }

    /**
     * Returns the total amount of <i>milliseconds</i> elapsed
     * 
     * @return The number of milliseconds elapsed
     */
    public long elapsedMillis(){
        if(isRunning){
            return (System.currentTimeMillis() - t0);
        }else{
            return (t1 - t0);
        }
    }

    /**
     * Returns the total amount of <i>seconds</i> elapsed
     * 
     * @return The number of seconds elapsed
     */
    public long elapsedSeconds(){
        if(isRunning){
            return ((System.currentTimeMillis() - t0) / 1000L);
        }else{
            return ((t1 - t0) / 1000L);
        }
    }

    /**
     * Returns the total amount of <i>minutes</i> elapsed
     * 
     * @return The number of minutes elapsed
     */
    public long elapsedMinutes(){
        if(isRunning){
            return ((System.currentTimeMillis() - t0) / 60000L);
        }else{
            return ((t1 - t0) / 60000L);
        }
    }

    /**
     * Returns the total amount of <i>hours</i> elapsed
     * 
     * @return The number of hours elapsed
     */
    public long elapsedHours(){
        if(isRunning){
            return ((System.currentTimeMillis() - t0) / 3600000L);
        }else{
            return ((t1 - t0) / 3600000L);
        }
    }

    /**
     * Returns the total amount of <i>days</i> elapsed
     * 
     * @return The number of days elapsed
     */
    public long elapsedDays(){
        if(isRunning){
            return ((System.currentTimeMillis() - t0) / 86400000L);
        }else{
            return ((t1 - t0) / 86400000L);
        }
    }

    /**
     * Returns the total amount of time elapsed as
     * a <i>java.time.Duration</i> object
     * 
     * @return The time elapsed as a <code>Duration</code> object
     */
    public Duration elapsedDuration(){
        return Duration.ofMillis(elapsedMillis());
    }

    /**
     * Executes the specified FutureAction at its specified time in the future
     * 
     * @param action The <code>FutureAction</code> to execute at its
     *               specified point in the future
     * @return The submitted <code>FutureAction</code> instance
     * @since 3.0.0
     */
    public FutureAction execute(final FutureAction action){
        if(action != null){
            if(watcher == null){
                this.watcher = new Watcher(WATCHER_MODE_TEMPORARY);
            }
            action.setChronometer(this);
            start().watcher.watch(action);
        }
        return action;
    }

    /**
     * Returns a string representation of this Chronometer.<br>
     * If this Chronometer is running then the returned string will
     * represent the point in time this method is called
     *
     * @return A string representation of this Chronometer.
     */
    @Override
    public String toString(){
        final StringBuilder sb = new StringBuilder();
        long elapsed = 0;
        long days = 0;
        long hours = 0;
        long minutes = 0;
        long seconds = 0;
        if(isRunning){
            elapsed = System.currentTimeMillis()-t0;
        }else{
            elapsed = t1-t0;
        }
        while(elapsed>=86400000L){
            days += 1;
            elapsed-=86400000L;
        }
        if(days>0){
            sb.append(days);
            sb.append(" days ");
        }
        while(elapsed>=3600000L){
            hours+=1;
            elapsed-=3600000L;
        }
        if(hours>0){
            sb.append(hours);
            sb.append("h ");
        }
        while(elapsed>=60000L){
            minutes+=1;
            elapsed-=60000L;
        }
        if(minutes>0){
            sb.append(minutes);
            sb.append("min ");
        }
        while(elapsed>=1000L){
            seconds+=1;
            elapsed-=1000L;
        }
        if(seconds>0){
            sb.append(seconds);
            sb.append("s ");
        }
        if(elapsed>0){
            sb.append(elapsed);
            sb.append("ms");
        }
        final String res = sb.toString();
        return (res.isEmpty() ? "0" : res);
    }

    /**
     * Represents a watcher thread keeping track of when to execute
     * submitted FutureActions. 
     *
     */
    protected class Watcher {

        private boolean mode;
        private ScheduledThreadPoolExecutor thread;
        private BlockingDeque<FutureAction> actions;

        /**
         * Constructs a new <code>Watcher</code> with the specified mode
         * 
         * @param mode The mode of the costructed Watcher
         */
        protected Watcher(final boolean mode){
            this.mode = mode;
            createWatcher();
            this.actions = new LinkedBlockingDeque<FutureAction>();
        }

        /**
         * Submits the specified FutureAction for scheduling.
         * 
         * @param action The <code>FutureAction</code> to watch for timed execution
         */
        protected void watch(final FutureAction action){
            this.actions.offer(action);
            if(thread == null){
                createWatcher();
            }
            action.setScheduledFuture(thread.scheduleWithFixedDelay(
                    action,
                    action.destiny(),
                    action.interval(),
                    TimeUnit.MILLISECONDS));

        }

        /**
         * Stops this Watcher and shuts down the underlying daemon thread. Marks all
         * actively running and pending FutureActions as cancelled 
         */
        protected void stop(){
            this.thread.shutdownNow();
            this.thread = null;
            for(final FutureAction action : actions){
                action.setCancelled(true);
            }
            this.actions.clear();
        }

        /**
         * Tries to removes the specified FutureAction from the internal
         * queue of both the watcher and the watcher daemon thread
         * 
         * @param action The <code>FutureAction</code> for which to attempt removal
         */
        protected void remove(final FutureAction action){
            this.actions.remove(action);
            this.thread.remove(action);
        }

        /**
         * Creates a new watcher thread in the configured mode
         */
        private void createWatcher(){
            this.thread = (ScheduledThreadPoolExecutor)
                    Executors.newScheduledThreadPool(1, WATCHER_FACTORY);

            this.thread.setMaximumPoolSize(1);
            if(mode == WATCHER_MODE_PERMANENT){
                //Disable any timeouts for core threads as a watcher
                //in permanent mode should keep running until explicitly stopped
                this.thread.allowCoreThreadTimeOut(false);
                //Start a permanent watcher thread right away
                this.thread.prestartCoreThread();
            }else{// == WATCHER_MODE_TEMPORARY
                //Let an idle watcher thread terminate
                //itself after 60 seconds of inactivity
                this.thread.setKeepAliveTime(60, TimeUnit.SECONDS);
                //A watcher thread is always a core thread
                this.thread.allowCoreThreadTimeOut(true);
            }
            this.thread.setRemoveOnCancelPolicy(true);
        }
    }

    /**
     * A <code>ThreadFactory</code> for creating watcher threads.<br>
     * All watcher threads are daemon threads by default.
     *
     */
    private static final class WatcherFactory implements ThreadFactory {

        @Override
        public Thread newThread(Runnable r){
            final Thread thread = new Thread(r, "Chronometer.Watcher-" + postfix());

            thread.setDaemon(true);
            if(thread.getPriority() != Thread.NORM_PRIORITY){
                thread.setPriority(Thread.NORM_PRIORITY);
            }
            return thread;
        }

        private synchronized String postfix(){
            return String.valueOf(System.currentTimeMillis());
        }
    }
    
}
