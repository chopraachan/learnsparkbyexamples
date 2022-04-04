package learnbyexamples.sparkBasics.rddBasics

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object rddReadTextFiles extends App {

  // Spark Session is the Entry point into the Spark Application. This would be used in all the
  val sparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("create rdd")
    .getOrCreate()

  val sparkContext: SparkContext = sparkSession.sparkContext
  sparkContext.setLogLevel("ERROR")

  // read text files into a RDD
  val rddFromTextFile: RDD[String] = sparkContext.textFile("src/main/resources/textFiles/brands/brands.txt")

  // to get data from this file we will use the collect method
  rddFromTextFile.collect().foreach(f => {
      println(f)
    }
  )

  println("="*30)

  // read multiple text files
  val rddFromMultipleTextFiles: RDD[String] = sparkContext.textFile("src/main/resources/textFiles/brands/brands.txt,"
    + "src/main/resources/textFiles/brands/brands1.txt,"
    + "src/main/resources/textFiles/brands/brands2.txt,"
    + "src/main/resources/textFiles/brands/brands3.txt"
  )

  rddFromMultipleTextFiles.collect().foreach(f => {
      println(f)
    }
  )

  println("="*30)
  // read text files from a directory
  val rddTextFilesFromDirectory: RDD[String] = sparkContext.textFile("src/main/resources/textFiles/brands/")

  rddTextFilesFromDirectory.collect().foreach(f => {
      println(f)
    }
  )

  println("="*30)
  // read text files using a wildcard character
  val rddTextFilesWildCard: RDD[String] = sparkContext.textFile("src/main/resources/textFiles/brands/bran*.txt")

  println(rddTextFilesWildCard.count())


  println("="*30)
  // read whole text file. This will return a tuple comprising of
    // _1 --- File Name
    // _2 === Content of the file
  val rddWholeTextFile: RDD[(String, String)] = sparkContext.wholeTextFiles("src/main/resources/textFiles/brands")

  rddWholeTextFile.foreach( f => {
      println(f._1+" => "+f._2 )
    }
  )


}
