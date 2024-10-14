#!/bin/bash

if [ -f ../jars/worker.jar ]; then
    mvn clean
    rm ../jars/worker.jar
fi

mvn package

mv worker-jar-with-dependencies.jar ../jars/worker.jar
