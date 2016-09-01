package org.jbugkorea.spark.jdg

import java.util.Properties

import org.infinispan.spark.stream._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.infinispan.client.hotrod.event.ClientEvent.Type
import org.infinispan.spark.domain.Person


object FilteringRDDByQueryNew {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)

    val infinispanHost = "127.0.0.1:11222;127.0.0.1:11372"

    val conf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .setAppName("spark-infinispan-example-filter-RDD-scala")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val configuration = new Properties
    configuration.put("infinispan.client.hotrod.server_list", infinispanHost)
    configuration.put("infinispan.rdd.cacheName", "default")

    val ssc = new StreamingContext(sc, Seconds(1))

    val stream = new InfinispanInputDStream[String, Person](ssc, StorageLevel.MEMORY_ONLY, configuration)

    // Filter only created entries
    val createdBooksRDD = stream.filter { case (_, _, t) => t == Type.CLIENT_CACHE_ENTRY_CREATED }

    // Reduce last 30 seconds of data, every 10 seconds
    val windowedRDD: DStream[Long] = createdBooksRDD.count().reduceByWindow(_ + _, Seconds(30), Seconds(10))

    // Prints the results, couting the number of occurences in each individual RDD
    windowedRDD.foreachRDD { rdd => println(rdd.reduce(_ + _)) }

    // Start the processing
    ssc.start()
    ssc.awaitTermination()
  }
}
