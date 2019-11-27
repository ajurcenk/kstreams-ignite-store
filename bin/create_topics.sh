#!/bin/bash

kafka-topics --bootstrap-server localhost:9092 --create --topic wordcount-source-topic  --partitions 1 --replication-factor 1

kafka-topics --bootstrap-server localhost:9092 --create --topic wordcount-target-topic  --partitions 1 --replication-factor 1

kafka-topics --bootstrap-server localhost:9092 --list
