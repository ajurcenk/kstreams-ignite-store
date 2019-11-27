#!/bin/bash
mvn exec:java -f ./../pom.xml -Dexec.mainClass=org.ajur.demo.kstreams.ignite.app.IgniteContinuousQueryApp