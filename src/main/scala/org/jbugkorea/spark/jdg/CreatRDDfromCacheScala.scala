package org.jbugkorea.spark.jdg

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.infinispan.client.hotrod.{RemoteCache, RemoteCacheManager}
import org.infinispan.spark.rdd.InfinispanRDD
import org.jbugkorea.User

object CreatRDDfromCacheScala {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)

    // Obtain the remote cache
    val cacheManager = new RemoteCacheManager
    val cache: RemoteCache[Integer, User] = cacheManager.getCache()

    // Put some sample data to the remote cache
    cache.put(1, new User("ted", 1))
    cache.put(2, new User("bob", 2))


    val infinispanHost = "127.0.0.1:11222;127.0.0.1:11372"

    val conf = new SparkConf()
      .setAppName("spark-infinispan-wordcount-scala")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val infinispanProperties = new Properties
    infinispanProperties.put("infinispan.client.hotrod.server_list", infinispanHost)
    infinispanProperties.put("infinispan.rdd.cacheName", "default")

    // Create RDD from cache
    val infinispanRDD = new InfinispanRDD[Integer, User](sc, configuration = infinispanProperties)
    infinispanRDD.foreach(println)
    val usersRDD = infinispanRDD.values.count()

    println(usersRDD)
  }
}
