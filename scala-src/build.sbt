name := "project"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.2.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.2.1" % "provided",
  "org.apache.opennlp" % "opennlp-tools" % "1.6.0",
  "org.apache.opennlp" % "opennlp-uima" % "1.6.0",
  "edu.stanford.nlp" % "stanford-corenlp" % "3.6.0",
  "com.google.protobuf" % "protobuf-java" % "2.6.1"
)

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.3.2"
libraryDependencies += "joda-time" % "joda-time" % "2.8.1"

// assemblyMergeStrategy in assembly := {
//   case PathList("META-INF", xs @ _*) => MergeStrategy.discard
//   case x                             => MergeStrategy.first
// }
