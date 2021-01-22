/* -----------------------------------------------
 * SparkModel.scala -
 *
 * Description:
 *    Provides a demonstration of the Spark MongoDB Connector, by
 *    implementing a linear regression model on one of the SUSTAIN
 *    datasets in the MongoDB sharded cluster.
 *    Guide for this project taken directly from MongoDB docs:
 *    https://docs.mongodb.com/spark-connector/master/scala-api
 *
 *  Author:
 *    Caleb Carlson
 *
 * ----------------------------------------------- */

package org.sustain

import org.sparkproject.dmg.pmml.support_vector_machine.Coefficient

// Main entrypoint for the Spark Model that gets submitted to the cluster.
object SparkModel {

  var configuration: Map[String,String] = Map(
    "modelType" -> "",
    "databaseName" -> "",
    "databaseHost" -> "",
    "collection" -> "",
    "gisJoins" -> "",
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

    /* Minimum Imports */
    import com.mongodb.spark.config.ReadConfig
    import com.mongodb.spark.sql.DefaultSource
    import com.mongodb.spark.MongoSpark
    import org.apache.spark.sql.SparkSession
    import org.apache.spark.sql.DataFrame
    import org.apache.spark.sql.Dataset
    import org.apache.spark.sql.Row
    import org.apache.spark.sql.functions.col
    import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer}
    import org.apache.spark.ml.regression.LinearRegression
    import org.apache.spark.ml.regression.LinearRegressionModel
    import org.apache.spark.ml.evaluation.RegressionEvaluator


    // Create the SparkSession and ReadConfig
    val sparkConnector: SparkSession = SparkSession.builder()
      .master(configuration("sparkMaster"))
      .appName("SustainConnector")
      .config("spark.mongodb.input.uri", configuration("databaseHost"))
      .config("spark.mongodb.input.database", configuration("databaseName"))
      .config("spark.mongodb.input.collection", configuration("collection"))
      .getOrCreate()

    import sparkConnector.implicits._ // For the $()-referenced columns

    // Read collection into a DataFrame
    val df: DataFrame = MongoSpark.load(sparkConnector)

    // Only select relevant fields
    val df1: DataFrame = df.select("GISJOIN", "_id", "temp", "year")


    println(s">>> Columns: GISJOIN,coefficients,intercept,rmse,predictedMax2021")
    val gisJoins: Array[String] = configuration("gisJoins").split(',')
    for (gisJoin: String <- gisJoins) {

      val t0 = System.nanoTime()

      // Discard all rows not for this GISJOIN, group by the year, selecting the maximum temp among the temps for that year.
      // Finally, sort by the years, so it's in chronological order.
      val gisDf: DataFrame = df.filter($"GISJOIN" === gisJoin)
        .groupBy("year")
        .max("temp")
        .sort("year")
        .withColumnRenamed("max(temp)", "label")

      // Make a linear model for the max temps
      // Create a feature transformer that merges multiple columns into a vector column
      val assembler: VectorAssembler = new VectorAssembler()
        .setInputCols(Array("year"))
        .setOutputCol("features")

      // Merge multiple feature columns into a single vector column
      val mergedDf: DataFrame = assembler.transform(gisDf)

      // Create a linear regression model object and fit it to the training set
      val linearRegression: LinearRegression = new LinearRegression()
        .setMaxIter(10)
        .setRegParam(0.3)
        .setElasticNetParam(0.8)

      val lrModel: LinearRegressionModel = linearRegression.fit(mergedDf)

      // Print the coefficients and intercept for linear regression


      //println(s"\tCoefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

      val trainingSummary = lrModel.summary
      //println(s"\tTotal Iterations: ${trainingSummary.totalIterations}")
      //println(s"\tObjective History: [${trainingSummary.objectiveHistory.mkString(",")}]")
      //trainingSummary.residuals.show()
      //println(s"\tRMSE: ${trainingSummary.rootMeanSquaredError}")
      //println(s"\tR2: ${trainingSummary.r2}")

      println(">>> Results: {" +
        s"${gisJoin}," +
        s"${lrModel.coefficients}," +
        s"${lrModel.intercept}," +
        s"${trainingSummary.rootMeanSquaredError}," +
        s"${predictMaxTemperature(2021, lrModel.coefficients(0), lrModel.intercept)})}}")

      // Use the model on the testing set, and evaluate results
      //val lrPredictions: DataFrame = lrModel.transform(test)
      //val evaluator: RegressionEvaluator = new RegressionEvaluator().setMetricName("rmse")
      //println(s"\tTest RMSE: ${evaluator.evaluate(lrPredictions)}")

      val t1 = System.nanoTime()
      println("Elapsed time: " + (t1 - t0)/1000000000 + "seconds")
    }

    // Group by GISJOIN and year, selecting the max of year
    //val ds2: Dataset[Row] = df1.groupBy("GISJOIN", "year").max("temp")

    //val collected: Array[Row] = ds2.collect()
    //for ()

    //val df3: DataFrame = ds2.where(ds2("GISJOIN") === "G1200170").sort("year")

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

  // Predict a year given a linear model (y = mx + b)
  def predictMaxTemperature(year: Int, coefficient: Double, intercept: Double): Double = {
    return (year * coefficient) + intercept
  }

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")
    result
  }
}
