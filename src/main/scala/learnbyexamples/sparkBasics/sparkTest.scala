package learnbyexamples.sparkBasics

import org.apache.spark.sql.SparkSession


object sparkTest extends App {

  // Spark Session is the Entry point into the Spark Application.
  val sparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("Spark Test App")
    .getOrCreate()

  // Display info for the app we created
  println(" This is a Spark Test App.")
  println(" App Name:" + sparkSession.sparkContext.appName)
  println("Deploy Mode:" + sparkSession.sparkContext.deployMode)
  println("Master: "+ sparkSession.sparkContext.master)
}
