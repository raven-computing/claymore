#### 2.5.0
* Namespace change from *com.kilo52.common* to *com.raven.common*
* Added serialization support for the DataFrame version 2 binary format (v2)
* Removed the *Serializable* interface extension from the *DataFrame* interface and *Column* abstract class. Serialization is now officially only supported via the *DataFrameSerializer* class
* Changed *DataFrameSerializer* so that all operations are now provided as public static methods. The class cannot be instantiated anymore
* Changed the DataFrame API so that references of column labels are now located directly in each Column instance. Appropriate constructors and methods were added
* Added unique type codes to all classes extending *Column*. This is now the preferred way for differentiating columns instead of using the *instanceof* operator
* Changed *CSVFileReader* and *CSVFileWriter* so that headers and data values are properly escaped when they contain instances of the used separator
* Added some JUnit tests for the made changes

#### 2.0.2
* Added code to properly close resources in *CSVFileReader*, *CSVFileWriter*, *DataFrameSerializer* and all FileHandlers
* Improved behaviour of *CSVFileReader*, causing it to skip empty lines in CSV files instead of throwing an exception
* Fixed crash for *ConcurrentCSVReader* threads when encountering an uncaught exception
* Minor refactoring of code for starting background threads in *CSVFileReader*, *CSVFileWriter* and *DataFrameSerializer*
* Improved error messages for IOExceptions caused by improperly formatted CSV files

#### 2.0.0
* Added *Row* interface and *RowItem* annotation
* Added methods to the DataFrame API to allow row operations with custom classes implementing the *Row* marker interface and annotating fields with *@RowItem*
* Added constructors to both *DefaultDataFrame* and *NullableDataFrame* implementation to construct an empty DataFrame based on a *Row* class. Column structure is inferred by annotated fields
* Improved performance for *indexOf()*, *indexOfAll()* and *filter()* methods in all DataFrame implementations by caching Pattern instance which holds precompiled regex for faster match operation
* Removed deprecated *findAll()* methods from the DataFrame API
* Fixed Bug when deserializing DataFrames with CharColumns/NullableCharColumns that have escaped characters
* Added *get_Argument()* methods to *ArgumentParser* which let the user specify a default value that gets returned if the requested argument was not provided
* Added JUnit tests

#### 1.3.0
* Added remove capabilities to the *ConfigurationFile* class. Both Sections as well as individual entries can now be removed if desired
* Fixed DataFrame deserialization error when trying to read a *.df* file that represents an uninitialized DataFrame, i.e. no columns defined
* Added package-info documentation to all packages
* Fixed minor code formatting issues

#### 1.1.0
* Renamed *findAll()* methods to *filter()* in the DataFrame API. All *findAll()* methods are now deprecated and will be removed in a future release
* All *filter()* methods now return an empty DataFrame instead of null when there are no matches
* Return type of public methods in CSVFileReader and CSVFileWriter changed to allow method chaining

#### 1.0.0 
* Final version for open source release

