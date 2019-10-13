# Kafka Streaming Project For DDOS Attack Analysis
The Objective of this project was to read a log file from a location and write to a message system using Kafka stream. A consumer will read data from the message system and detects whether the attacker is part of the DDOS attack. Once an attacker is found, the ip-address should be written to a results directory which could be used for further processing.

### Prerequisit
* [Kafka_2.12-2.3.0][kdownload]
* [Java 1.8][jdownload]
* IDE - IntelliJ IDEA or Eclipse

### Installation
* **Download & Extract Kafka, Edit below properites in config folder:**
    * server.properties ----------> log.dirs=C:\kafka_2.11-1.0.0\kafka-logs
    * zookeeper.properties ----------> dataDir=C:\kafka_2.11-1.0.0\data
*  **Open Kafka-Streaming in IntelliJ**

### Apache Log File Format
```155.157.157.83 - - [25/May/2015:23:11:39 +0000] "GET / HTTP/1.0" 200 3557 "-" "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; en) Opera 8.51"```

**LogFormat "%h %l %u %t "%r" %>s %b "%{Referer}i" "%{User-Agent}i"**

|Symbol|Description|
|------:|-----------:|
|%h|IP Address of client (remote host)|
|%l|Identd of client (normally unavailable)|
|%u|User id of user requesting object|
|%t|Time of request|
|%r|Full request string|
|%>s|Status code|
|%b|Size of request (excluding headers)|
|%{Referer}i|The previous webpage|
|%{User-agent}i|The Client’s browser|

### Steps
* Start Zookeeper with below command in $KAFKA_HOME
```ssh
bin\zookeeper-server-start.bat config\zookeeper.properties
```

* Start kafka Server with below command in $KAFKA_HOME
```ssh
bin\kafka-server-start.bat config\server.properties
```

* Run kafkaJavaConsumer.java by passing topic name as argument
* Run KafkaJavaProducer.java by passing topic namd and File Location for data streaming

### Refrences
1. [Apache Access Log Description][logDesc]
2. [DDOS Attack Analysis][ddosLink]

[kdownload]: <https://kafka.apache.org/downloads>
[jdownload]: <https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html>
[logDesc]: <https://www.keycdn.com/support/apache-access-log>
[ddosLink]: <https://www.loggly.com/blog/how-to-detect-and-analyze-ddos-attacks-using-log-analysis/>
