cd ../common
call mvn package -DskipTests=true
cd ../preprocessing
call mvn package -DskipTests=true
cd ../segment
call mvn package -DskipTests=true
cd ../filecombine
call mvn package -DskipTests=true
cd ../warehouse
call mvn package -DskipTests=true
cd ../clustering
call mvn package -DskipTests=true
pause