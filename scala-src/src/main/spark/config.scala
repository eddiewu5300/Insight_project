package spark

object Config {

  val cassandraHost = "10.0.0.7,10.0.0.13,10.0.0.5" // this can be a comma separated list in prod env
  val cassandraKeySpace = "project"
  val sparkMaster = "10.0.0.11"
}
