cd Worker
call mvn clean
cd ..

cd Manager
call mvn clean
cd ..

cd Local
call mvn clean
cd ..

cd jars
del *.jar
cd ..