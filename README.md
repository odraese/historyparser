# Hive ProtoBuf History File Parser.
Hive write history events (like task start/finish) to protobuf files under dag_data and a manifest for this in query_data. There is lots of
useful information for performance and task analysis in there and while
there are official tools to process these files, I created this helper
program for automated processing of these events. This current 
implementation support the reading of task finish evenets to create a
report about the min/max/average/standard deviation of tasks execution
times, grouped by application ID. But the tool is designed in a way that
allows easy enhancing it for other scenarios.

### Configuration
The tool takes three arguments. The first one is the name of a event processor class (within the 
``processors`` package. The second one is the path to the directory,holding the query_data and 
dag_data directories. This can either be a local directory or also a HDFS path. The third 
parameter takes the filename for the report to generate. All three parameters are also specified in
the ``pom.xml`` file to have standard parameters for Maven based program execution. You need 
to adjust the arguments in the POM if you want to run the program via Maven.

###  Starting the tool
```
mvn compile exec:java -Dexec.args="TaskTimeEventProcessor -i /sourcePath -o targetFile.txt"
```

### ATS Support
ATS source support was added to this tool and can be used by specifying the `-ats` command line
option. The content of the source directory is then supposed to be ATS data (JSON format of
the events). Internally, these will be converted into HistoryEventProto instances and then
passed on the the existing @c EventProcessor instances. 
Consuming ATS input is less efficient than the HistoryProtoEvent instances stored in a 
sequence file within a dag_data directory.

## Current event processors
**Both processors extract and work on LLAP related tasks only!**

* ``TaskFeatureExtractor`` is parsing the dag_data for HistoryEventProto and writes each
  task finish event as a single line with its counter values to the report. It is good
  to produce a text file that can be mined for specific task features.
* ``TaskTimeEventProcessor`` is aggregating task execution times (min/max/count/avg/stdDev) 
  and write these aggregates to the report. So, it produces basic LLAP application
  statistics.

## Extending
New scenarios can be added by
* implementing a ``EventProcessor``, which handled the event stream
* specifying the ``EventProcessor`` in ``App.setupFor`` method
* configuring event maximum, filters, ... in ``App``

### Dependencies
* The project defines multiple Tez libraries as dependencies, which are
  downloaded by Maven upon first compile.
* Based on amount of source data and ``EventProcessor`` implementation,
  you might need to increase the heap size of your JVM like ``-Xmx8G -XX:+UseG1GC 
  -XX:+UseStringDeduplication``
