if exist  ..\jars\local.jar (
	call mvn clean
	del ..\jars\local.jar
)
	
call mvn package

move local-jar-with-dependencies.jar  ..\jars\local.jar