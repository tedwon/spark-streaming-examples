package org.jbugkorea.spark.jdg

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.infinispan.client.hotrod.{RemoteCache, RemoteCacheManager}
import org.infinispan.spark._
import org.jbugkorea.Book

object WriteRDDToJDGScala {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)

    val infinispanHost = "127.0.0.1:11222;127.0.0.1:11372"

    val conf = new SparkConf()
      .setAppName("spark-infinispan-write-example-RDD-scala")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    // Create an RDD of Books
    val bookOne = new Book("Linux Bible", "Negus, Chris", "2015")
    val bookTwo = new Book("Java 8 in Action", "Urma, Raoul-Gabriel", "2014")

    val sampleBookRDD = sc.parallelize(Seq(bookOne, bookTwo))
    val pairsRDD = sampleBookRDD.zipWithIndex().map(_.swap)

    val infinispanProperties = new Properties
    infinispanProperties.put("infinispan.client.hotrod.server_list", infinispanHost)
    infinispanProperties.put("infinispan.rdd.cacheName", "default")

    // Write the Key/Value RDD to the Data Grid
    pairsRDD.writeToInfinispan(infinispanProperties)


    // Obtain the remote cache
    val cacheManager = new RemoteCacheManager
    val cache: RemoteCache[Long, Book] = cacheManager.getCache()

    val book: Book = cache.get(0L)
    println(book)

  }
}
