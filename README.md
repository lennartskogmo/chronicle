## About Chronicle

Chronicle is a pyspark based framework for loading data from common data sources directly into delta tables in a databricks environment.  
By using spark which is already an integral part of Databricks, Chronicle aims remain 100% code based and to eliminate both the need for additional tools and additonal intermediate steps when loading data.  
The framework is called Chronicle because the delta tables it writes out are so-called slowly changing dimensions type 2 tables, which captures changes over time.  

Although Chronicle was developed on databricks it should work in any spark environment with minimal changes.  

The major components of Chronicle are:
- Readers which are responsible for reading data and producing data frames
- Writer(s) which are responsible for taking data frames and writing to delta tables
- Metadata components which are responsible for handling configuration data and providing a object oriented interface to the configuration
- The object loader which handles parallel execution of data loading

Chronicle uses the concepts data connections and data objects when dealing with metadata and configuration:
- Data objects represents the tables that results from loading data. These tables or objects are also expected to be optimized, vaccumed, anonymized and just managed over time in general
- Data connections represent connection details needed to load in data


## Adding Chronicle to your spark environment

Chronicle can be added to your spark environment either as a python wheel or a databricks notebook.

Running the [build script](build/build.sh) should produce a wheel version of the framework that can be copied to your databricks workspace but this also requires a local installation of docker.  

Using the notebook version of the framework only requires copying the [chronicle notebook](library/chronicle.py) to your databricks workspace.  

Deployments are generally easier when using the notebook verison as deploying a new wheel to a databricks workspace seems to require starting any affected clusters.
For testing purposes using the nootebook version is recommended.  


In addition to the framework itself there are a couple of [support notebooks](library/) for loading configuration from yml, creating schemas from configuration, and running the object loader.  
In order to load objects on a schedule from metadata these notebooks can be run in sequence from a standard scheduled databricks job, but more about that later.  

When using the wheel version components can imported in typical python fashion.  
When using the notebook version the framework notebook must be run before any other code.  
See support notebooks for examples.


## Using Chronicle without configuration metadata.

If you are just testing Chronicle or have a very small project you can use Chronicle components directly from any notebook.  

Even if you do this, you should still use the resolve_secret function or dbtools to lookup your secrets and avoid storing them direclty in the nootebook.  

First run the chronicle.py notebook by putting the following in a cell:    

```
%run ./chronicle.py
```


