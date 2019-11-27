#!/bin/bash

kafka-topics kafka-topics --zookeeper localhost:2181 --create --topic wordcount-source-topic --if-not-exists  --partitions 1 --replication-factor 1

kafka-topics  --zookeeper localhost:2181 --create --topic wordcount-target-topic --if-not-exists  --partitions 1 --replication-factor 1

kafka-topics --bootstrap-server localhost:9092 --list
