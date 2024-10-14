#!/bin/bash

if [ -f ../jars/manager.jar ]; then
    mvn clean
    rm ../jars/manager.jar
fi

mvn package

mv manager-jar-with-dependencies.jar ../jars/manager.jar
