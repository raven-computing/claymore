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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A parser for command line arguments. This class cannot be instantiated directly.
 * <br>Use an {@link ArgumentParser.Builder} instead.
 * <p>All arguments constructed by that builder are treated case sensitive, with 
 * the exception of the already defined standard helper arguments. Supported helper
 * arguments are:<br>
 * <pre>   <i>-help</i>  <i>-?</i>  <i>-version</i></pre> <br>
 * A helper argument must always be the first argument. You can query if such was
 * passed to the application by calling {@link #helpTriggered()} and
 * {@link #versionTriggered()} respectively. You should then take appropriate actions.
 * <br> Although not obligatory, all arguments should be prefixed with a single 
 * dash ('-') when passing them to the command line. An argument is made of a 
 * key-value-pair, separated by an equal sign ('='). Using an equal sign either
 * within the key or the value is not permitted and will result in a 
 * malformed argument.<br> 
 * After building an argument parser the {@link #parse} method should be called, 
 * passing it the string array from the main method.
 * 
 * <p><i>Example:</i><br>
 * <pre><code> ArgumentParser ap = new ArgumentParser.Builder()
 *				.integerArg("myint")
 *				.optionalStringArg("mystring")
 *				.optionalBooleanArg("mybool")
 *				.build();
 * </code></pre>
 * The above code will let you pass a mandatory integer argument, and two optional
 * arguments, a string and a boolean, to the application at startup.<br>
 * Then pass the string array holding the arguments from the command line to the 
 * parse method and catch an <code>ArgumentParseException</code> like this:
 *
 * <pre><code>	
 *  try{
 *  	ap.parse(args);
 *  }catch(ArgumentParseException ex){ 
 *  	//handle exception 
 *  }
 * </code></pre>
 * Finally, right after the catch clause, check whether any helper arguments got
 * passed to the application, which would require a separate action:
 * 
 * <pre><code>	
 * if(ap.helpTriggered()){
 *  	//handle -help arg
 * }
 * if(ap.versionTriggered()){
 *  	//handle -version arg
 * }
 * </code></pre>
 * You may then get any argument passed to the main method's String array like this:
 * <pre><code>
 * int i = ap.getIntegerArg("myint", 42);
 * String s = ap.getStringArg("mystring", "bla");
 * boolean b = ap.getBooleanArg("mybool", true);
 * </code></pre>
 * The second argument in above methods specifies a default value if the corresponding
 * command line argument is optional and was not provided.<br>
 *
 * 
 * @author Phil Gaiser
 * @since 1.0.0
 *
 */
public class ArgumentParser {

    private Container container;

    private boolean helpTriggered;
    private boolean versionTriggered;

    private ArgumentParser(){ }

    private ArgumentParser(final Container container){
        this.container = container;
    }

    /**
     * Parses all command line arguments provided by the main method's string array.
     * <br>All arguments are parsed in the order within the array. Any helper arguments
     * must be located at the first position.<br>Arguments should be prefixed with a 
     * single dash ('-') when passing them to the command line. An argument is made of 
     * a key-value-pair, separated by an equal sign ('='). Using an equal sign either
     * within the key or the value is not permitted and will result in a 
     * malformed argument.
     * 
     * <p>If any errors occur, such as an invalid, malformed or missing argument, then 
     * this method will throw an {@link ArgumentParseException}. That exception holds 
     * data about the cause of the error and delivers formatted messages which can be
     * displayed to the user
     * 
     * <p>If no exception is thrown, then all command line arguments are valid and can
     * be accessed through their corresponding getters
     * 
     * @param args The string array from the main method
     * @throws ArgumentParseException Indicates that a command line argument is either
     * 		   						  missing, malformed, unrecognized, restricted,
     * 								  forbidden or invalid
     */
    public void parse(final String[] args) throws ArgumentParseException{
        if(args.length != 0){
            final String arg = trim(args[0]);
            if(arg.equalsIgnoreCase("help") || arg.equalsIgnoreCase("?")){
                this.helpTriggered = true;
                return;
            }else if(arg.equalsIgnoreCase("version")){
                this.versionTriggered = true;
                return;
            }
        }
        for(int i=0; i<args.length; ++i){
            final String[] s = args[i].split("=");
            if(s.length != 2){
                final ArgumentParseException ex = constructException(String.format(
                        "Malformed argument '%s'", args[i]));

                ex.setCause(args[i]);
                throw ex;
            }
            final String key = s[0];
            final String value = s[1];
            final Argument a = this.container.args.get(trim(key));
            if(a == null){
                final ArgumentParseException ex = constructException(String.format(
                        "Invalid argument '%s'", key));

                ex.setCause(args[i]);
                throw ex;
            }
            if(!a.parse(value)){
                final ArgumentParseException ex = constructException(String.format(
                        "Invalid argument '%s'. %s", args[i], a.hint));

                ex.setCause(args[i]);
                throw ex;
            }
            a.value = value;
            if(!a.isOptional){
                this.container.mandatory.remove(trim(key));
            }
        }
        if(!container.mandatory.isEmpty()){
            final String missing = this.container.mandatory.iterator().next();
            final ArgumentParseException ex = constructException(String.format(
                    "Missing argument '%s'", missing));

            ex.setCause(missing);
            throw ex;
        }
    }

    /**
     * Gets the integer value of the specified command line argument.<br>
     * Please note that {@link #parse} must be called prior to this method
     * 
     * @param arg The key of the command line argument ot get
     * @return The integer value of the specified argument, or null if the
     * 		   argument was optional and not provided
     */
    public Integer getIntegerArg(final String arg){
        final Argument a = this.container.args.get(trim(arg));
        if(a != null){
            return (a.value != null ? Integer.valueOf(a.value) : null);
        }
        return null;
    }

    /**
     * Gets the integer value of the specified command line argument 
     * or returns the specified default value.<br>
     * Please note that {@link #parse} must be called prior to this method
     * 
     * @param arg The key of the command line argument to get
     * @param defaultValue The default value in the case the argument was unspecified
     * @return The integer value of the specified argument, or the default value
     *         if the argument was optional and not provided
     */
    public int getIntegerArg(final String arg, final int defaultValue){
        final Integer i = getIntegerArg(arg);
        return ((i != null) ? i : defaultValue);
    }

    /**
     * Gets the string value of the specified command line argument.<br>
     * Please note that {@link #parse} must be called prior to this method
     * 
     * @param arg The key of the command line argument ot get
     * @return The string value of the specified argument, or null if the
     * 		   argument was optional and not provided
     */
    public String getStringArg(final String arg){
        final Argument a = this.container.args.get(trim(arg));
        return (a != null ? a.value : null);
    }

    /**
     * Gets the string value of the specified command line argument 
     * or returns the specified default value.<br>
     * Please note that {@link #parse} must be called prior to this method
     * 
     * @param arg The key of the command line argument to get
     * @param defaultValue The default value in the case the argument was unspecified
     * @return The string value of the specified argument, or the default value
     *         if the argument was optional and not provided
     */
    public String getStringArg(final String arg, final String defaultValue){
        final String s = getStringArg(arg);
        return ((s != null) ? s : defaultValue);
    }

    /**
     * Gets the boolean value of the specified command line argument.<br>
     * Please note that {@link #parse} must be called prior to this method
     * 
     * @param arg The key of the command line argument ot get
     * @return The boolean value of the specified argument, or null if the
     * 		   argument was optional and not provided
     */
    public Boolean getBooleanArg(final String arg){
        final Argument a = this.container.args.get(trim(arg));
        if(a != null){
            return (a.value != null ? Boolean.valueOf(a.value) : null);
        }
        return null;
    }

    /**
     * Gets the boolean value of the specified command line argument 
     * or returns the specified default value.<br>
     * Please note that {@link #parse} must be called prior to this method
     * 
     * @param arg The key of the command line argument to get
     * @param defaultValue The default value in the case the argument was unspecified
     * @return The boolean value of the specified argument, or the default value
     *         if the argument was optional and not provided
     */
    public boolean getBooleanArg(final String arg, final boolean defaultValue){
        final Boolean b = getBooleanArg(arg);
        return ((b != null) ? b : defaultValue);
    }

    /**
     * Gets a descriptive text which indicates what arguments to use with this
     * <code>ArgumentParser</code> instance
     * 
     * @return A hint which can be displayed to the user
     */
    public String hint(){
        return this.container.usage;
    }

    /**
     * Indicates whether the <code>-help</code> argument was passed to this 
     * argument parser, which should cause the application to show some kind 
     * of help text
     * 
     * @return True if some variation of the <i>-help</i> argument was passed
     * 		   to this argument parser, false otherwise
     */
    public boolean helpTriggered(){
        return this.helpTriggered;
    }

    /**
     * Indicates whether the <code>-version</code> argument was passed to this 
     * argument parser, which should cause the application to show version
     * information about itself
     * 
     * @return True if some variation of the <i>-version</i> argument was passed
     * 		   to this argument parser, false otherwise
     */
    public boolean versionTriggered(){
        return this.versionTriggered;
    }

    /**
     * Constructs and returns a new <code>ArgumentException</code> with the 
     * specified detail message
     * 
     * @param message The message of the new exception
     * @return A new <code>ArgumentException</code>
     */
    private ArgumentParseException constructException(final String message){
        final ArgumentParseException ex = new ArgumentParseException(message);
        ex.setHint(this.container.usage);
        return ex;
    }

    /**
     * Removes any dispensable characters from an argument key to ensure 
     * flexibility when handling different input
     * 
     * @param key The argument key to trim
     * @return The trimmed argument key
     */
    private static String trim(final String key){
        return (key.startsWith("-") ? key.substring(1) : key);
    }

    /**
     * Builder class to create an <code>ArgumentParser</code>.<br>
     * Simply call the no-args constructor followed by all needed builder methods.
     * Finally, the <code>build()</code> method will return an argument parser
     * which can then be used to perform the actual parsing.
     *
     */
    public static class Builder {

        private Container container;

        /**
         * Constructs a new <code>ArgumentParser.Builder</code>.<br>
         * To build an <code>ArgumentParser</code>, subsequently call
         * all needed builder methods, followed by {@link #build} to get hold of
         * the argument parser
         */
        public Builder(){
            this.container = new Container();
        }

        /**
         * Adds a <b>mandatory</b> integer argument to this builder
         * 
         * @param key The key of the integer argument to add
         * @return This builder instance
         */
        public Builder integerArg(final String key){
            return integerArg(key, false); 
        }

        /**
         * Adds an <b>optional</b> integer argument to this builder
         * 
         * @param key The key of the integer argument to add
         * @return This builder instance
         */
        public Builder optionalIntegerArg(final String key){
            return integerArg(key, true); 
        }

        /**
         * Adds a <b>mandatory</b> integer argument to this builder. The argument 
         * is restricted by the specified range. Passing an out-of-range integer 
         * argument to the <code>parse()</code> method will 
         * throw a <code>ArgumentParseException</code>
         * 
         * @param key The key of the integer argument to add
         * @param from The beginning of the accepted range of the integer 
         * 			   argument (inclusive)
         * @param to The end of the accepted range of the integer 
         * 			 argument (inclusive)
         * @return This builder instance
         */
        public Builder integerArg(final String key, final int from, final int to){
            return integerArg(key, from, to, false);
        }

        /**
         * Adds an <b>optional</b> integer argument to this builder. The argument 
         * is restricted by the specified range. Passing an out-of-range integer 
         * argument to the <code>parse()</code> method will 
         * throw a <code>ArgumentParseException</code>
         * 
         * @param key The key of the integer argument to add
         * @param from The beginning of the accepted range of the integer 
         *             argument (inclusive)
         * @param to The end of the accepted range of the integer 
         *           argument (inclusive)
         * @return This builder instance
         */
        public Builder optionalIntegerArg(final String key, final int from, final int to){
            return integerArg(key, from, to, true);
        }

        /**
         * Adds a <b>mandatory</b> string argument to this builder
         * 
         * @param key The key of the string argument to add
         * @return This builder instance
         */
        public Builder stringArg(final String key){
            return stringArg(key, false);
        }

        /**
         * Adds an <b>optional</b> string argument to this builder
         * 
         * @param key The key of the string argument to add
         * @return This builder instance
         */
        public Builder optionalStringArg(final String key){
            return stringArg(key, true);
        }

        /**
         * Adds a <b>mandatory</b> string argument to this builder. The argument 
         * is restricted by the specified {@link Set}. Passing a string argument which
         * does not exist in the specified set to the <code>parse()</code> method will
         * throw a <code>ArgumentParseException</code>.
         * 
         * @param key The key of the string argument to add
         * @param set The set containing all strings allowed for the specified argument
         * @return This builder instance
         */
        public Builder stringArg(final String key, final Set<String> set){
            return stringArg(key, set, false);
        }

        /**
         * Adds an <b>optional</b> string argument to this builder. The argument 
         * is restricted by the specified {@link Set}. Passing a string argument which
         * does not exist in the specified set to the <code>parse()</code> method will
         * throw a <code>ArgumentParseException</code>.
         * 
         * @param key The key of the string argument to add
         * @param set The set containing all strings allowed for the specified argument
         * @return This builder instance
         */
        public Builder optionalStringArg(final String key, final Set<String> set){
            return stringArg(key, set, true);
        }

        /**
         * Adds a <b>mandatory</b> boolean argument to this builder
         * 
         * @param key The key of the boolean argument to add
         * @return This builder instance
         */
        public Builder booleanArg(final String key){
            return booleanArg(key, false);
        }

        /**
         * Adds an <b>optional</b> boolean argument to this builder
         * 
         * @param key The key of the boolean argument to add
         * @return This builder instance
         */
        public Builder optionalBooleanArg(final String key){
            return booleanArg(key, true);
        }

        /**
         * Builds and returns an <code>ArgumentParser</code> from this builder
         * 
         * @return A new <code>ArgumentParser</code> instance
         */
        public ArgumentParser build(){
            StringBuilder sb = new StringBuilder();
            Set<Map.Entry<String, Argument>> entries = this.container.args.entrySet();
            sb.append("Usage: ");
            for(final Map.Entry<String, Argument> e : entries){
                if(!e.getValue().isOptional){
                    sb.append("-"+e.getKey()
                    +"="+(e.getValue().isAlphanumeric ? "<>" : "true|false")+" ");
                }
            }
            for(final Map.Entry<String, Argument> e : entries){
                if(e.getValue().isOptional){
                    sb.append("[-"+e.getKey()
                    +"="+(e.getValue().isAlphanumeric ? "<>" : "true|false")+"] ");
                }
            }
            this.container.usage = sb.toString();
            return new ArgumentParser(this.container);
        }

        /**
         * Adds an integer argument to this builder. It must be specified
         * whether the argument is optional
         * 
         * @param key The key of the integer argument to add
         * @param isOptional True if the added argument is optional, false if it
         *                   is mandatory
         * @return This builder instance
         */
        private Builder integerArg(final String key, final boolean isOptional){
            Argument a = new Argument(isOptional, "Must be an integer"){
                @Override
                boolean parse(String value){
                    try{
                        Integer.valueOf(value);
                    }catch(NumberFormatException ex){
                        return false;
                    }
                    return true;
                }
            };
            this.container.args.put(trim(key), a);
            if(!isOptional){
                this.container.mandatory.add(trim(key));
            }
            return this;
        }

        /**
         * Adds an integer argument to this builder. The argument is restricted by the 
         * specified range. Passing an out-of-range integer argument to the 
         * <code>parse()</code> method will throw a <code>ArgumentParseException</code>.
         * <br>It must be specified whether the argument is optional
         * 
         * @param key The key of the integer argument to add
         * @param from The beginning of the accepted range of the integer 
         *             argument (inclusive)
         * @param to The end of the accepted range of the integer 
         *           argument (inclusive)
         * @param isOptional True if the added argument is optional, false if it
         *                   is mandatory
         * @return This builder instance
         */
        private Builder integerArg(final String key, final int from, final int to,
                final boolean isOptional){

            Argument a = new Argument(isOptional, String.format(
                    "Must be an integer between %s and %s", from, to)){

                @Override
                boolean parse(String value){
                    try{
                        final int i = Integer.valueOf(value);
                        return ((i>=from) && (i<=to));
                    }catch(NumberFormatException ex){
                        return false;
                    }
                }
            };
            this.container.args.put(trim(key), a);
            if(!isOptional){
                this.container.mandatory.add(trim(key));
            }
            return this;
        }

        /**
         * Adds a string argument to this builder. It must be specified
         * whether the argument is optional
         * 
         * @param key The key of the string argument to add
         * @param isOptional True if the added argument is optional, false if it
         *                   is mandatory
         * @return This builder instance
         */
        private Builder stringArg(final String key, final boolean isOptional){
            Argument a = new Argument(isOptional, "Invalid string argument"){
                @Override
                boolean parse(String value){
                    return true;
                }
            };
            this.container.args.put(trim(key), a);
            if(!isOptional){
                this.container.mandatory.add(trim(key));
            }
            return this;
        }

        /**
         * Adds a string argument to this builder. The argument is restricted by the 
         * specified {@link Set}. Passing a string argument which does not exist in the
         * specified set to the <code>parse()</code> method will
         * throw an <code>ArgumentParseException</code>.<br>
         * It must be specified whether the argument is optional
         * 
         * @param key The key of the string argument to add
         * @param set The set containing all strings allowed for the specified argument
         * @param isOptional True if the added argument is optional, false if it
         *                   is mandatory
         * @return This builder instance
         */
        private Builder stringArg(final String key, final Set<String> set,
                final boolean isOptional){

            Argument a = new Argument(isOptional, ""){
                @Override
                boolean parse(String value){
                    this.hint = String.format("Unknown argument '%s'", value);
                    return set.contains(value);
                }
            };
            this.container.args.put(trim(key), a);
            if(!isOptional){
                this.container.mandatory.add(trim(key));
            }
            return this;
        }

        /**
         * Adds a boolean argument to this builder. It must be specified
         * whether the argument is optional
         * 
         * @param key The key of the boolean argument to add
         * @param isOptional True if the added argument is optional, false if it
         *                   is mandatory
         * @return This builder instance
         */
        private Builder booleanArg(final String key, final boolean isOptional){
            Argument a = new Argument(isOptional, "Must be a boolean"){
                @Override
                boolean parse(String value){
                    return ((value.equalsIgnoreCase("true")) 
                            || (value.equalsIgnoreCase("false")) 
                               ? true 
                               : false);
                }
            };
            a.isAlphanumeric = false;
            this.container.args.put(trim(key), a);
            if(!isOptional){
                this.container.mandatory.add(trim(key));
            }
            return this;
        }
    }

    /**
     * Container holding arguments and info messages.
     *
     */
    private static class Container {

        Map<String, Argument> args;
        Set<String> mandatory;
        String usage;

        Container(){
            this.args = new HashMap<String, Argument>();
            this.mandatory = new HashSet<String>();
        }
    }

    /**
     * Models an argument and holds data about such.
     *
     */
    private abstract static class Argument {

        String value;
        String hint;
        boolean isOptional;
        boolean isAlphanumeric = true;

        public Argument(final boolean isOptional, final String hint){
            this.isOptional = isOptional;
            this.hint = hint;
        }

        abstract boolean parse(String value);
    }
}
