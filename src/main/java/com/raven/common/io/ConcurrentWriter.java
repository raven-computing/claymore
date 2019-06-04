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

package com.raven.common.io;

import java.io.File;

/**
 * Functional interface defining a callback for concurrently 
 * writing files.
 * 
 * @author Phil Gaiser
 *
 */
@FunctionalInterface
public interface ConcurrentWriter {
	
	/**
	 * Called when a concurrent writing operation has finished,
	 * passing the result in form of a File object as an argument 
	 * to this method
	 * 
	 * @param file The File object representing the written file
	 */
	void onWritten(File file);

}
