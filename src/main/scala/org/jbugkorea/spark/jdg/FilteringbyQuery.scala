package org.jbugkorea.spark.jdg

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.infinispan.client.hotrod.impl.query.RemoteQuery
import org.infinispan.client.hotrod.{RemoteCacheManager, Search, RemoteCache}
import org.infinispan.spark.rdd.InfinispanRDD
import org.infinispan.spark._
import org.jbugkorea.User

object FilteringbyQuery {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)

    // Obtain the remote cache
    val cacheManager = new RemoteCacheManager
    val cache: RemoteCache[Integer, User] = cacheManager.getCache()

    // put some sample data
    cache.put(1, new User("a", 1))
    cache.put(2, new User("b", 2))






    val infinispanHost = "127.0.0.1:11222;127.0.0.1:11372"

    val conf = new SparkConf().setAppName("spark-infinispan-wordcount-scala")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val infinispanProperties = new Properties
    infinispanProperties.put("infinispan.client.hotrod.server_list", infinispanHost)
    //    infinispanProperties.put("infinispan.rdd.cacheName", "exampleCache")





    // Filtering by a Query
    val query = Search.getQueryFactory(cacheManager.getCache())
      .from(classOf[User])
      .having("name").like("a")
      .toBuilder.build()

    val infinispanRDD = new InfinispanRDD[Integer, User](sc, configuration = infinispanProperties).filterByQuery[User](query, classOf[User])



    // create RDD from cache
    val usersRDD = infinispanRDD.values.count()

    println(usersRDD)
  }
}
