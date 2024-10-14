if exist ..\jars\worker.jar (
    call mvn clean
    del ..\jars\worker.jar
)

call mvn package

move worker-jar-with-dependencies.jar ..\jars\worker.jar
