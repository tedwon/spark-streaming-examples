package org.jbugkorea.spark.jdg

import java.util
import java.util.Properties

import com.google.common.collect.Sets
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.infinispan.client.hotrod.{RemoteCache, RemoteCacheManager}
import org.infinispan.spark.rdd.InfinispanRDD
import org.jbugkorea.{Author, Book}

object CreatRDDFromJDGScala {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)

    // Obtain the remote cache
    val cacheManager = new RemoteCacheManager
    val cache: RemoteCache[Integer, Book] = cacheManager.getCache()

    // Put some sample data to the remote cache
    val authors = Sets.newHashSet[Author]
    authors.add(new Author("Negus, Chris", ""))
    authors.add(new Author("Urma, Raoul-Gabriel", ""))
    val bookOne = new Book("Linux Bible", "desc", 2015, authors)
    val bookTwo = new Book("Java 8 in Action", "desc", 2014, authors)
    cache.put(1, bookOne)
    cache.put(2, bookTwo)

    val infinispanHost = "127.0.0.1:11222;127.0.0.1:11372"

    val conf = new SparkConf()
      .setAppName("spark-infinispan-example-RDD-scala")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val infinispanProperties = new Properties
    infinispanProperties.put("infinispan.client.hotrod.server_list", infinispanHost)
    infinispanProperties.put("infinispan.rdd.cacheName", "default")

    // Create RDD from cache
    val infinispanRDD = new InfinispanRDD[Integer, Book](sc, configuration = infinispanProperties)

    infinispanRDD.foreach(println)

    val booksRDD: RDD[Book] = infinispanRDD.values

    val count = booksRDD.count()

    println(count)
  }
}
