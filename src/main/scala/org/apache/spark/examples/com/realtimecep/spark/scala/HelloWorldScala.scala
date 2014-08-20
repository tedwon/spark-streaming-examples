package org.apache.spark.examples.com.realtimecep.spark.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created on 8/20/14.
 *
 * <pre>
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
