package org.sustain


// Main entrypoint for the Spark Model that gets submitted to the cluster.
object SparkModel {

  var configuration: Map[String,String] = Map(
    "modelType" -> "",
    "databaseName" -> "",
    "databaseHost" -> "",
    "collection" -> "",
    "query" -> "",
    "sparkMaster" -> ""
  )

  // Main entrypoint for the SparkModel JAR.
  def main(args: Array[String]): Unit = {

    try {
      processArgs(args)
      validateConfiguration()
    } catch {
      case ex: IllegalArgumentException => {
        println("Caught IllegalArgumentException:\n" + ex.getMessage)
      }
    }

    println("Exiting!")

  }

  // Processes arguments passed into the main function of the JAR
  // at runtime, and fills out the configuration map.
  // Arguments should come in the form "key=value", and
  // the key must exist in the configuration map.
  def processArgs(args: Array[String]): Unit = {

    args.foreach(arg => {
      val kvPair: Array[String] = arg.split('=').map(_.trim)
      if (kvPair.length != 2) {
        throw new IllegalArgumentException("Arguments must be in the form \"key=value\"!")
      }

      if (!configuration.contains(kvPair(0))) {
        throw new IllegalArgumentException("Invalid key \"%s\", must be one of:\n%s".format(
          kvPair(0), "[modelType, databaseName, databaseHost, collection, query, sparkMaster]"
        ))
      }

      // Update value in configuration map
      configuration += (kvPair(0) -> kvPair(1))
      println("Updated key \"%s\" to have value \"%s\"".format(kvPair(0), configuration(kvPair(0))))
    })

  }

  // Validates the arguments, making sure that all the necessary args
  // have been provided.
  def validateConfiguration(): Unit = {
    for (key <- configuration.keys) {
      key match {
        case "collection" | "query" =>
          if (configuration("collection").trim.isEmpty && configuration("query").trim.isEmpty) {
            throw new IllegalArgumentException("Omitted arguments for both \"query\" and \"collection\"! Must specify one.")
          }
        case _ =>
          if (configuration(key).trim.isEmpty) {
            throw new IllegalArgumentException("Omitted arguments for both \"%s\". Must specify.".format(key))
          }
      }
    }
  }

}
