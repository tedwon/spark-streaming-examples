package org.jbugkorea.spark.jdg

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.infinispan.client.hotrod.{RemoteCache, RemoteCacheManager}
import org.infinispan.spark._
import org.jbugkorea.User

/**
  * Created by tedwon on 8/8/16.
  */
object WriteRDDtoCacheScala {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)

    // Obtain the remote cache
    val cacheManager = new RemoteCacheManager
    val cache: RemoteCache[Long, User] = cacheManager.getCache()



    val infinispanHost = "127.0.0.1:11222;127.0.0.1:11372"

    val conf = new SparkConf().setAppName("write-example-RDD-scala")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val userC = new User("c", 3)
    val userD = new User("d", 4)

    val sampleUserRDD: RDD[User] = sc.parallelize(Seq(userC,userD))
    val pairsRDD: RDD[(Long, User)] = sampleUserRDD.zipWithIndex().map(_.swap)

    val infinispanProperties = new Properties
    infinispanProperties.put("infinispan.client.hotrod.server_list", infinispanHost)
    //    infinispanProperties.put("infinispan.rdd.cacheName", "exampleCache")

    // Write the Key/Value RDD to the Data Grid
    pairsRDD.writeToInfinispan(infinispanProperties)

    val user: User = cache.get(0L)
    println(user)

  }
}
