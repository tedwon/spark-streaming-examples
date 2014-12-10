package org.apache.spark.examples.com.realtimecep.spark.scala.batch

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
object SaveAsTextFileScala {
  def main(args: Array[String]): Unit = {
    println("Hello Spark!!")

    val conf = new SparkConf().setAppName("Hello Spark!!")
      .setMaster("local[1]")
      .set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)

    val data = Array(1, 2, 3, 4, 5)
    val distData = sc.parallelize(data)
    val first: Int = distData.first()
    println(first)
//    distData.saveAsTextFile("/tmp/aaa")
    distData.saveAsTextFile("hdfs://namenode/user/ted/test")


  }
}
