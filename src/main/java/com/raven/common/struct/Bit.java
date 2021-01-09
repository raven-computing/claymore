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

package com.raven.common.struct;

/**
 * A bit is the smallest unit of information. It can only be in one of
 * two possible states: {@link Bit#_1} (true) or {@link Bit#_0} (false).<br>
 * This class exposes two public static constants which are intended to be
 * used in certain situations when dealing with bit values
 * as primitive booleans. In such cases readability may be increased by
 * using the constants of this class.
 * 
 * <p>This class cannot be instantiated.
 * 
 * @author Phil Gaiser
 * @see BitVector
 * @since 3.0.0
 *
 */
public final class Bit {

    private Bit(){ }

    /**
     * Represents the <b>zero</b> bit. Its value is <code>false</code>
     */
    public static final boolean _0 = false;

    /**
     * Represents the <b>one</b> bit. Its value is <code>true</code>
     */
    public static final boolean _1 = true;

}
