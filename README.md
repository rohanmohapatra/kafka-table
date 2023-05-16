# Kafka Distributed Hash Table
A distributed incremental hash table built on Kafka. 

Clients can use this service as an incrementable hashtable. It is a hashtable whose key is a string and value is an integer. 
We cannot directly set the integer for a given key, but we can increment it. (We decrement by incrementing with a negative number.) 
If a key does not exist in the table, we act as if the key has a value of zero. the Two operations we can do on the table are:
  - get(key) - returns the current integer for the key, or zero if the key does not exist.
  - increment(key, value) - increments the key by the given value. this operation will fail if the value would take the key below 0. there can be no negative values.

Periodically a replica snapshots the state of the table into the log to enable fast recovery of replicas or new replicas to come up to speed.

## Benefits of Incrementable Hash Table 
It is commonly used in various applications where efficient lookup and insertion of key-value pairs are required, and the number of elements in the hashtable can change dynamically over time.
 
## How can you use it?
Clients will use gRPC to make INC and GETget requests to a replica, you can use any replica.

## Building
```
mvn package
```

## Running the replica
```
java -jar .\target\kafka-table-1.0-SNAPSHOT-spring-boot.jar replica cs-reed-07.class.homeofcode.com:9092 replicaId grpcPort snaphotFrequency topicPrefix
```
