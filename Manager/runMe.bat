if exist  ..\jars\manager.jar (
	call mvn clean
	del  ..\jars\manager.jar
)
	
call mvn package

move manager-jar-with-dependencies.jar  ..\jars\manager.jar