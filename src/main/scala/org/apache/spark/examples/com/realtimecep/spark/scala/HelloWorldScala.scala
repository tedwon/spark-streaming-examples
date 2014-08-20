package org.apache.spark.examples.com.realtimecep.spark.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created on 8/20/14.
 *
 * <pre>
 * ./bin/spark-submit --class org.apache.spark.examples.com.realtimecep.spark.scala.HelloWorldScala --master spark://teds-MacBook-Pro.local:7077 /Users/ted/Dropbox/Development/spark/sample-spark-maven-project/target/spark-maven-0.1.0-SNAPSHOT.jar /Users/ted/Development/spark/dist/mydata.txt
 *
 * ./bin/spark-submit --class org.apache.spark.examples.com.realtimecep.spark.scala.HelloWorldScala --master spark://teds-MacBook-Pro.local:7077 /Users/ted/Dropbox/Development/spark/sample-spark-maven-project/target/spark-maven-0.1.0-SNAPSHOT.jar hdfs://localhost:9000/test/mydata.txt
 *
 * ./bin/spark-submit --class org.apache.spark.examples.com.realtimecep.spark.scala.HelloWorldScala --master spark://teds-MacBook-Pro.local:7077 /Users/ted/Dropbox/Development/spark/sample-spark-maven-project/target/spark-maven-0.1.0-SNAPSHOT.jar /Users/ted/Downloads/muffin.analytics.28987.muffin-04.A.json.20140820152050
 * </pre>
 *
 * @author <a href="iamtedwon@gmail.com">Ted Won</a>
 * @version 1.0
 */
object HelloWorldScala {
  def main(args: Array[String]): Unit = {
    println("Hello Spark!!")

    val conf = new SparkConf().setAppName("Hello Spark!!")
      //    .setMaster("spark://127.0.0.1:7077")
      .set("spark.executor.memory", "8g")
    val sc = new SparkContext(conf)
    println(args(0))
    val file = sc.textFile(args(0)).cache()

    println(file.count)
    println(file.first)

  }
}
