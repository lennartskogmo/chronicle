About Chronicle

Chronicle is a pyspark based framework for loading data from common data sources directly into delta tables in a databricks environment.  
By using spark which is already an integral part of Databricks, Chronicle aims remain 100% code based and to eliminate both the need for additional tools and additonal intermediate steps when loading data.
The framework is called Chronicle because the delta tables it writes out are so-called slowly changing dimensions type 2 tables, which captures changes over time.

Although Chronicle was developed on databricks it should work in any spark environment with minimal changes.

The major components of Chronicle are:
- Readers which are responsible for reading data and producing data frames
- Writer(s) which are responsible for taking data frames and writing to delta tables
- Metadata components which are responsible reading configuration data and providing a object oriented interface to the configuration
- The object loader which handles parallel execution of data loading
