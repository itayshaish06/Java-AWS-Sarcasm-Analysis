#!/bin/bash

cd Worker
mvn clean
cd ..

cd Manager
mvn clean
cd ..

cd Local
mvn clean
cd ..

cd jars
rm -f *.jar
