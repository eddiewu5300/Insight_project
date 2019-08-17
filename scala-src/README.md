### Run the commands
`./scala/sbt assembly`  

`cp scala/target/scala-2.11/project-assembly-1.0.jar $SPARK_HOME/jars`  

`spark-submit --class spark.text_processing --master spark://<master_ip>:7077  --executor-memory 5G --driver-memory 5G project-assembly-1.0.jar`