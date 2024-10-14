#!/bin/bash

if [ -f ../jars/local.jar ]; then
    mvn clean
    rm ../jars/local.jar
fi

mvn package

mv local-jar-with-dependencies.jar ../jars/local.jar
