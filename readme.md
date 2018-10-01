test task:
in archive test_task.zip, password is "test_task" (to avoid indexing)

settings:
set mysql connection settings here
\src\main\resources\application.properties
OR here application.properties and copy  this file to /target

install:
mvn clean install

usage:
cd target
java -cp "parser.jar" com.ef.Parser --startDate=2017-01-01.13:00:00 --duration=hourly --threshold=100
