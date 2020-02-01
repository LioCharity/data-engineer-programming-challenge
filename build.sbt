name := "data-engineer-programming-challenge"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "2.3.0", 
  "org.apache.spark" %% "spark-sql" % "2.3.0",
  "org.apache.hadoop" % "hadoop-hdfs" % "2.4.0",
  "log4j" % "log4j" % "1.2.17",
  "com.holdenkarau" %% "spark-testing-base" % "2.3.0_0.12.0" % "test",
  "com.github.scopt" %% "scopt" % "3.7.1",
  "org.scalatest" %% "scalatest" % "3.1.0" % "test")

fork in Test := true
parallelExecution in Test := false
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")



