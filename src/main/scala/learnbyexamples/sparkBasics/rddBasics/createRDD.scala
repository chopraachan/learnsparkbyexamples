package learnbyexamples.sparkBasics.rddBasics

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession


object createRDD extends App {
  // Spark Session is the Entry point into the Spark Application. This would be used in all the
  val sparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("create rdd")
    .getOrCreate()

  val sparkContext: SparkContext = sparkSession.sparkContext
  // Use parallelize option to create an rdd.
  // sc.parallelize() can be used to create a RDD from existing collection

  // This setting will be used to Turn on Info, Debug messages on the console.
  sparkContext.setLogLevel("ERROR")

  val intCollection: Array[Int] = Array(1,2,3,4,5,6,7,8,9,10)
  val strCollection: Array[String] = Array("This","is","a","collection", "of", "strings")
  val mixCollection: Seq[Any] = List("This","is", "mixed",1,2,3,4,5)

  val intRdd = sparkContext.parallelize(intCollection)
  val strRdd = sparkContext.parallelize(strCollection)
  val mixRdd = sparkContext.parallelize(mixCollection)

  //rdd does not not have a show method. So we will collect the contents and display using println

  intRdd.collect().foreach(println)
  println("==========================")
  strRdd.collect().foreach(println)
  println("==========================")
  mixRdd.collect().foreach(println)

  // Other ways of exploring rdd
  println("First element of intRdd:"+intRdd.first())
  println("==========================")
  println("First element of strRdd:"+strRdd.first())
  println("==========================")
  println("First element of mixRdd:"+mixRdd.first())
  println("==========================")

  // Creating an empty rdd
  // parallelize can we used to create empty rdd. We can create an empty sequence of respective type and pass it to parallelize.
  val strEmptyRDD = sparkContext.parallelize(Seq.empty[String])
  val genericEmptyRDD = sparkContext.parallelize(Seq.empty[Any])
  val intEmptyRDD = sparkContext.parallelize(Seq.empty[Int])

  println("Count of string empty rdd:"+ strEmptyRDD.count())






}
