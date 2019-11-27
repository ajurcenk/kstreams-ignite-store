#!/bin/bash
kafka-producer-perf-test --topic wordcount-source-topic --num-records 100 --throughput 1 --payload-file ./../cfg/test_data.txt --producer-props bootstrap.servers=localhost:9092