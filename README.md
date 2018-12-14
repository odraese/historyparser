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
The tool takes two arguments. The first one is the path to the directory,holding the query_data and dag_data directories. This can either be a 
local directory or also a HDFS path. The second parameter takes the
filename for the report to generate. Both paths are also specified in
the ```pom.xml``` file to have standard parameters for Maven based
program execution. You need to adjust the arguments in the POM if you
want to run the program via Maven.

###  Starting the tool
```
mvn compile exec:java -Dexec.args="/sourcePath targetFile.txt"
```

### Extending
New scenarios can be added by
* implementing a ```EventProcessor```, which handled the event stream
* specifying the ```EventProcessor``` in ```App```
* configuring event maximum, filters, ... in ```App```

### Dependencies
* The project defines multiple Tez libraries as dependencies, which are
  downloaded by Maven upon first compile.
* Based on amount of source data and ```EventProcessor``` implementation,
  you might need to increase the heap size of your JVM like ```-Xmx8G -XX:+UseG1GC 
  -XX:+UseStringDeduplication```
