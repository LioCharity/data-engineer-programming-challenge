# Description
### FolderDescription
- jars: contains all the jar needed to build and run this project
- src/main: main code
- scr/test: unit tests

### How to run the unit test
````shell
# clone
>git clone <>

>cd data-engineer-programming-challenge

>sbt test
````
### Package and run the whole project (local )
````shell
# The jar is generated in src/target/scala
>sbt package

>spark2-submit  --class com.amadeus.acu.challenge.ProgrammingChallengeEntry --master local[10]  --num-executors 15 --driver-memory 5G --executor-memory 4G --jars /projects/jars/hadoop-hdfs-2.4.0.jar,/projects/jars/log4j-1.2.17.jar,/projects/jars/scopt_2.11-3.7.1.jar,/projects/jars/spark-sql_2.11-2.3.0.jar,/projects/jars/spark-core_2.11-2.3.0.jar <PATH TO JAR>/data-engineer-programming-challenge_2.11-0.1.jar -b <PATH TO CSV BOOKINGS>/sample-bookings.csv -s <PATH TO SEARCHES>/sample-searches.csv -o <OUTPUT DIR> -r <PATH TO>/ref_airport_popularity.csv

e.g
>spark2-submit  --class com.amadeus.acu.challenge.ProgrammingChallengeEntry --master local[10]  --num-executors 15 --driver-memory 5G --executor-memory 4G --jars /projects/jars/hadoop-hdfs-2.4.0.jar,/projects/jars/log4j-1.2.17.jar,/projects/jars/scopt_2.11-3.7.1.jar,/projects/jars/spark-sql_2.11-2.3.0.jar,/projects/jars/spark-core_2.11-2.3.0.jar /projects/data-engineer-programming-challenge/target/scala-2.11/data-engineer-programming-challenge_2.11-0.1.jar -b file:///projects/data-engineer-programming-challenge/src/test/resources/test-data/sample-bookings.csv -s file:///projects/data-engineer-programming-challenge/src/test/resources/test-data/sample-searches.csv -o file:///projects/challengeOutput -r file:///projects/data-engineer-programming-challenge/src/main/resources/ref_airport_popularity.csv

````



* Number of bookings: 10000010
* Number of searches: 20390198
* Total number of passengers: 4908809

Top 10 airports

|airport_code|numberOfPassengers|            city|
|------------|------------------|----------------|
|         lhr|             88809|          london|
|         mco|             70930|      orlando fl|
|         lax|             70530|  los angeles ca|
|         las|             69630|    las vegas nv|
|         jfk|             66270|     new york ny|
|         cdg|             64490|           paris|
|         bkk|             59460|         bangkok|
|         mia|             58150|        miami fl|
|         sfo|             58000|san francisco ca|
|         dxb|             55590|           dubai|

NB: Not all the cities are available in the file used to convert from iata
airport code to city, which is normal for some
small airports. We can consider using this https://ourairports.com/data/
which is updated daily and seems more complete.

## Bonus: Web service for the question2

### Requirements
* Python 2.X or 3.X
* nodejs: https://nodejs.org/en/download/
* npm install payload-validator (payload/param validator)

### Usage
* check node version: 
```shell
>node -v 
v12.14.1
or 
>node --version
v12.14.1
````
* Update the config file to put the right path to the JSON file
containing all the data.
* launch node server: node app.js
```shell
>node app.js
Server running at http://127.0.0.1:3000/
````
* target the server(with postman or ARC ...):
    * URL:http://localhost:3000
    * header name: Content-Type
    * header value: application/json
    * body: {"top_N_airport": 2}

