# Kafka REST API #

Version: 0.1-SNAPSHOT

#### REST service for Apache Kafka ####


### Version Compatability ###
This code is built with the following assumptions.  You may get mixed results if you deviate from these versions.

* [Kafka](http://incubator.apache.org/kafka) 0.8.1+
* [Protocol Buffers](https://developers.google.com/protocol-buffers) 2.6.1+

### Prerequisites ###
* Protocol Buffers
* Zookeeper (for Kafka)
* Kafka

### Building ###
To make a jar you can do:  

`mvn package`

The jar file is then located under `target`.

### Running an instance ###
**Make sure your Kafka and Zookeeper servers are running first (see Kafka documentation)**

In order to run kafka-rest-api on another machine you will probably want to use the _dist_ assembly like so:

`mvn assembly:assembly`

The zip file now under the `target` directory should be deployed to `KAFKA_REST_API_HOME` on the remote server.


### REST Request Format ###
#####URI _/submit/namespace/id_ | _/1.0/submit/namespace/id_#####
POST/PUT 

* The _namespace_ is required and is only accepted if it is in the configured white-list.
* The _id_ is optional although if you provide it currently it needs to be a valid UUID unless id validation is disabled on the _namespace_. 
* The payload content length must be less than the configured maximum.

DELETE

* The _namespace_ is required and is only accepted if it is in the configured white-list.
* The _id_ is required although if you provide it currently it needs to be a valid UUID unless id validation is disabled on the _namespace_.

Here's the list of HTTP response codes that Kafka REST API could send back:

* 201 Created - Returns the id submitted/generated. (default)
* 403 Forbidden - Violated access restrictions. Most likely because of the method used.
* 413 Request Too Large - Request payload was larger than the configured maximum.
* 400 Bad Request - Returned if the POST/PUT failed validation in some manner.
* 404 Not Found - Returned if the URI path doesn't exist or if the URI was not in the proper format.
* 500 Server Error - General server error. Someone with access should look at the logs for more details.

### Example kafka-rest-api Configuration (conf/kafka-rest-api.properties) ###
    # valid namespaces (whitelist only, comma separated)
    valid.namespaces=mynamespace,othernamespace
    max.content.length=1048576

### Example Kafka Producer Configuration (conf/kafka.producer.properties) ###
    # comma delimited list of ZK servers
    zk.connect=127.0.0.1:2181
    # use message encoder
    serializer.class=org.apache.kafka.rest.serializer.KafkaRestApiEncoder
    # asynchronous producer
    producer.type=async
    # compression.code (0=uncompressed,1=gzip,2=snappy)
    compression.codec=2
    # batch size (one of many knobs to turn in kafka depending on expected data size and request rate)
    batch.size=100

