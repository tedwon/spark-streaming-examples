package org.jbugkorea.spark.batch

import org.apache.spark.{SparkConf, SparkContext}

/**
 * <pre>
 * ./bin/spark-submit --class org.apache.spark.examples.com.realtimecep.spark.scala.HelloWorldScala --master spark://teds-MacBook-Pro.local:7077 /Users/ted/Dropbox/Development/spark/sample-spark-maven-project/target/spark-maven-0.1.0-SNAPSHOT.jar /Users/ted/Development/spark/dist/mydata.txt
 *
 * ./bin/spark-submit --class org.apache.spark.examples.com.realtimecep.spark.scala.HelloWorldScala --master spark://teds-MacBook-Pro.local:7077 /Users/ted/Dropbox/Development/spark/sample-spark-maven-project/target/spark-maven-0.1.0-SNAPSHOT.jar hdfs://localhost:9000/test/mydata.txt
 *
 * ./bin/spark-submit --class org.apache.spark.examples.com.realtimecep.spark.scala.HelloWorldScala --master spark://teds-MacBook-Pro.local:7077 /Users/ted/Dropbox/Development/spark/sample-spark-maven-project/target/spark-maven-0.1.0-SNAPSHOT.jar /Users/ted/Downloads/muffin.analytics.28987.muffin-04.A.json.20140820152050
 * </pre>
 */
object HelloWorldScala {
  def main(args: Array[String]): Unit = {
    println("Hello Spark!!")

    val conf = new SparkConf().setAppName("Hello Spark!!")
      .setMaster("local[*]")
      .set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)
    println(args(0))
    val file = sc.textFile(args(0)).cache()

    println(file.count)
    println(file.first)

  }
}
