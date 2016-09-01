package org.jbugkorea.spark.jdg

import java.util.Properties

import com.google.common.collect.Sets
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.infinispan.client.hotrod.{RemoteCache, RemoteCacheManager}
import org.infinispan.spark._
import org.jbugkorea.{Author, Book}

object WriteRDDToJDGScala {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)

    val infinispanHost = "127.0.0.1:11222;127.0.0.1:11372"

    val conf = new SparkConf()
      .setAppName("spark-infinispan-write-example-RDD-scala")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    // Create an RDD of Books
    val authors = Sets.newHashSet[Author]
    authors.add(new Author("Negus, Chris", ""))
    authors.add(new Author("Urma, Raoul-Gabriel", ""))
    val bookOne = new Book("Linux Bible", "desc", 2015, authors)
    val bookTwo = new Book("Java 8 in Action", "desc", 2014, authors)

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
