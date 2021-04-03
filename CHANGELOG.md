#### 4.0.3
* Changed info string to standard representation for uninitialized DataFrames in info() method.
* Fixed some errors in Column conversions.

#### 4.0.2
* Changed NullableDataFrame.addColumn() methods to cache row objects
* Changed AbstractDataFrame.toArray() to avoid unnecessary clone operation
* Changed exception error messages in DataFrame implementations

#### 4.0.1
* Added checks for unsigned ints for row and column count in DataFrame serialization
* Added additional catch clauses in DataFrameSerializer.serialize() and DataFrameSerializer.deserialize() to catch and re-throw all SerializationExceptions
* Added null checks in DataFrameUtils.merge() function for every argument array element
* Changed DataFrameSerializer.serializeImplv2() to use for-loop instead of for-each-loop over DataFrame columns to avoid additional copy operations
* Changed DataFrameSerializer.copyBytes() to use native System.arraycopy() for improved performance
* Fixed a benign bug in DataFrame serialization which in some cases added a superfluous zero byte to the end of the array
* Fixed a bug in NullableDataFrame minimum() and maximum() methods which returned NaN when there is only one non-null max/min value
* Fixed column handling when adding, inserting and setting empty columns
* Fixed return value of NullableDataFrame.castToNumericType() to null for integer cases when double value is NaN
* Fixed incorrect NaN presorting in float and double columns

#### 4.0.0
* Changed DataFrame API and implementations to strictly adhere to official specification
* Fixed a bug in reading of large DataFrame files
* Improved reading of CSV files with malformed formats
* Removed DataFrame serialization support for the legacy text-based version 1 format

#### 3.0.1
* Added BitVector implementation
* Added ProbabilisticSet interface and implementing BloomFilter classes 
* Added Item, WritableItem, FinalItem and ObservableItem classes
* Added Bit class with static constants
* Added Serializer interface
* Added StringSerializer class
* Added DataFrameSerializerInstance class
* Added SerializationException class
* Added ArgumentParser optional methods to remove boolean trap
* Added Actable, Action and TimedAction interfaces
* Added FutureAction class
* Added Chronometer support for FutureActions
* Added ByteFunction interface
* Added getType() method in abstract Column class
* Added BinaryColumn and NullableBinaryColumn classes to DataFrame API
* Added withHeader() method to CSVReader and CSVWriter class and remove boolean trap from constructor
* Added DataFrameSerializer and CSVReader CSVWriter read and write support from input/outputStreams
* Added ConfigurationFile complete review
* Added ConfigurationFile and PropertiesFile read and write support from input/outputStreams
* Added Hash class with static utility methods
* Added HashFunction enum
* Changed DataFrame addRow() and such method argument to varargs
* Changed indexOfAll() method in DataFrame API to return an empty array if nothing found
* Changed clone() return type to Column in abstract Column class
* Changed ConfigurationFile.Section class to be iterable
* Changed return type to CompletableFuture for async methods in CSVReader, CSVWriter and DataFrameSerializer
* Fixed Chronometer formatting bug for elapsed hours
* Fixed bug in equals() method in DefaultDataFrame and NullableDataFrame implementation
* Refactored static DateFrame utility methods to DataFrameUtil class
* Removed ConcurrentReader and ConcurrentWriter interfaces

#### 2.5.0
* Added serialization support for the DataFrame version 2 binary format (v2)
* Added unique type codes to all classes extending *Column*. This is now the preferred way for differentiating columns instead of using the *instanceof* operator
* Added some JUnit tests for the made changes
* Changed namespace from *com.kilo52.common* to *com.raven.common*
* Changed *DataFrameSerializer* so that all operations are now provided as public static methods. The class cannot be instantiated anymore
* Changed the DataFrame API so that references of column labels are now located directly in each Column instance. Appropriate constructors and methods were added
* Changed *CSVFileReader* and *CSVFileWriter* so that headers and data values are properly escaped when they contain instances of the used separator
* Removed the *Serializable* interface extension from the *DataFrame* interface and *Column* abstract class. Serialization is now officially only supported via the *DataFrameSerializer* class

#### 2.0.2
* Added code to properly close resources in *CSVFileReader*, *CSVFileWriter*, *DataFrameSerializer* and all FileHandlers
* Fixed crash for *ConcurrentCSVReader* threads when encountering an uncaught exception
* Improved behaviour of *CSVFileReader*, causing it to skip empty lines in CSV files instead of throwing an exception
* Improved error messages for IOExceptions caused by improperly formatted CSV files
* Minor refactoring of code for starting background threads in *CSVFileReader*, *CSVFileWriter* and *DataFrameSerializer*

#### 2.0.0
* Added *Row* interface and *RowItem* annotation
* Added methods to the DataFrame API to allow row operations with custom classes implementing the *Row* marker interface and annotating fields with *@RowItem*
* Added constructors to both *DefaultDataFrame* and *NullableDataFrame* implementation to construct an empty DataFrame based on a *Row* class. Column structure is inferred by annotated fields
* Added *get_Argument()* methods to *ArgumentParser* which let the user specify a default value that gets returned if the requested argument was not provided
* Added JUnit tests
* Fixed Bug when deserializing DataFrames with CharColumns/NullableCharColumns that have escaped characters
* Improved performance for *indexOf()*, *indexOfAll()* and *filter()* methods in all DataFrame implementations by caching Pattern instance which holds precompiled regex for faster match operation
* Removed deprecated *findAll()* methods from the DataFrame API

#### 1.3.0
* Added remove capabilities to the *ConfigurationFile* class. Both Sections as well as individual entries can now be removed if desired
* Added package-info documentation to all packages
* Fixed DataFrame deserialization error when trying to read a *.df* file that represents an uninitialized DataFrame, i.e. no columns defined
* Fixed minor code formatting issues

#### 1.1.0
* Changed all *filter()* methods to return an empty DataFrame instead of null when there are no matches
* Renamed *findAll()* methods to *filter()* in the DataFrame API. All *findAll()* methods are now deprecated and will be removed in a future release
* Return type of public methods in CSVFileReader and CSVFileWriter changed to allow method chaining

#### 1.0.0
* Open source release

