/* 
 * Copyright (C) 2019 Raven Computing
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

/**
 * A chronometer that can measure time and notify you of time-based events.
 * 
 * <p>Time can be measured by constructing a <code>Chronometer</code> through
 * its default constructor and then calling {@link #start()} at the point in 
 * time you wish to begin the measurement. Similarly, call {@link #stop()} right
 * after everything you wanted to measure is done.
 * 
 * <p>You can then call one of the <code>elapsed*()</code> methods to get the 
 * absolute time measured. Doing this while the chronometer is still running
 * will simply return the time from the start, up to that point.
 * 
 * <p>In order to get notified after a predefined amount of time has elapsed 
 * you must call {@link #setTimer(Duration, Timer)} and pass a 
 * {@link Chronometer.Timer} to it. You can specify the notification interval as
 * a {@link Duration} object.
 * 
 * @author Phil Gaiser
 * @since 1.0.0
 *
 */
public class Chronometer {
	
	/**
	 * Functional interface defining a callback for receiving events when the
	 * specified amount of time has elapsed.
	 *
	 */
	public interface Timer {
		
		/**
		 * Called whenever the predefined amount of time has elapsed passing the
		 * total amount of milliseconds since <code>start()</code> was called as
		 * an argument to this method. This callback will be triggered cyclically until
		 * <code>stop()</code> of the underlying Chronometer is called
		 * 
		 * @param elapsed The total amount of milliseconds since <code>start()</code>
		 */
		void onTick(long elapsed);
	}
	
	private long t0;
	private long t1;
	private boolean isRunning;
	
	private Watcher watcher;

	/**
	 * Constructs a new <code>Chronometer</code>.<br>
	 * In order to start time measurement, you must call {@link #start()}
	 */
	public Chronometer(){ }
	
	/**
	 * Starts time measurement of this <code>Chronometer</code>.<br>
	 * Subsequent calls will have no effect
	 * 
	 * @return This Chronometer instance
	 */
	public Chronometer start(){
		if(!isRunning){
			this.isRunning = true;
			if(this.watcher != null){
				new Thread(watcher).start();
			}
			t0 = System.currentTimeMillis();
		}
		return this;
	}
	
	/**
	 * Stops time measurement of this <code>Chronometer</code>.<br>
	 * Subsequent calls will have no effect
	 * <p>If a <code>Chronometer.Timer</code> has been set, then the
	 * callback will be disabled after this method returns
	 * 
	 * @return This Chronometer instance
	 */
	public Chronometer stop(){
		final long tmp = System.currentTimeMillis();
		if(isRunning){
			t1 = tmp;
			this.isRunning = false;
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
			return ((System.currentTimeMillis() - t0) / 1000l);
		}else{
			return ((t1 - t0) / 1000l);
		}
	}
	
	/**
	 * Returns the total amount of <i>minutes</i> elapsed
	 * 
	 * @return The number of minutes elapsed
	 */
	public long elapsedMinutes(){
		if(isRunning){
			return ((System.currentTimeMillis() - t0) / 60000l);
		}else{
			return ((t1 - t0) / 60000l);
		}
	}
	
	/**
	 * Returns the total amount of <i>hours</i> elapsed
	 * 
	 * @return The number of hours elapsed
	 */
	public long elapsedHours(){
		if(isRunning){
			return ((System.currentTimeMillis() - t0) / 36000000l);
		}else{
			return ((t1 - t0) / 36000000l);
		}
	}
	
	/**
	 * Returns the total amount of <i>days</i> elapsed
	 * 
	 * @return The number of days elapsed
	 */
	public long elapsedDays(){
		if(isRunning){
			return ((System.currentTimeMillis() - t0) / 8640000000l);
		}else{
			return ((t1 - t0) / 8640000000l);
		}
	}
	
	/**
	 * Sets a callback for this Chronometer which is being called every time
	 * the specified amount of time (duration) has elapsed, until {@link #stop()}
	 * is called.<br>
	 * The time interval must be specified as a {@link Duration} object.
	 * <p><i>Example:</i><br>
	 * <pre><code> 
	 * Chronomter chrono = new Chronometer();
	 * chrono.setTimer(Duration.ofSeconds(15), (elapsed) -> { doMagic(); });
	 * chrono.start();
	 * </code></pre>
	 * The above code will call <i>doMagic()</i> every <i>15 seconds</i>
	 * 
	 * @param duration The amount of time between each call
	 * @param timer The <code>Chronometer.Timer</code> for the callback
	 * @return This Chronometer instance
	 */
	public Chronometer setTimer(final Duration duration, final Timer timer){
		if((duration == null) || (timer == null)){
			throw new IllegalArgumentException("Arguments must not be null");
		}
		final long ms = duration.toMillis();
		if(ms <= 0){
			throw new IllegalArgumentException("Duration must be greater than 0ms");
		}
		this.watcher = new Watcher(timer, ms);
		return this;
	}
	
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
		while(elapsed>=8640000000l){
			days += 1;
			elapsed-=8640000000l;
		}
		if(days>0){
			sb.append(days+" days ");
		}
		while(elapsed>=36000000l){
			hours+=1;
			elapsed-=36000000l;
		}
		if(hours>0){
			sb.append(hours+"h ");
		}
		while(elapsed>=60000l){
			minutes+=1;
			elapsed-=60000l;
		}
		if(minutes>0){
			sb.append(minutes+"min ");
		}
		while(elapsed>=1000l){
			seconds+=1;
			elapsed-=1000l;
		}
		if(seconds>0){
			sb.append(seconds+"s ");
		}
		if(elapsed>0){
			sb.append(elapsed+"ms");
		}
		return sb.toString();
	}
	
	/**
	 * Watcher thread keeping track of when to trigger the Timer callback.
	 *
	 */
	private class Watcher implements Runnable {
		
		private Timer delegate;
		private long interval;
		
		/**
		 * Constructs a new <code>Watcher</code>
		 * 
		 * @param delegate The <code>Chronometer.Timer</code> for the callback
		 * @param interval The time interval between each call in milliseconds 
		 */
		Watcher(final Timer delegate, final long interval){
			this.delegate = delegate;
			this.interval = interval;
		}

		@Override
		public void run(){
			try{
				while(isRunning){
					final long now = System.currentTimeMillis();
					if((interval-10) > 0){
						Thread.sleep(interval-10);
					}
					while(System.currentTimeMillis()<(now+interval));
					if(isRunning){
						delegate.onTick(System.currentTimeMillis()-t0);
					}
				}
			}catch(InterruptedException ex){ }
		}
	}

}
