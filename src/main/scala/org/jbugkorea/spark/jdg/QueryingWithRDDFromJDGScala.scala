package org.jbugkorea.spark.jdg

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.infinispan.client.hotrod.{RemoteCache, RemoteCacheManager}
import org.infinispan.spark.rdd.InfinispanRDD
import org.jbugkorea.Book


object QueryingWithRDDFromJDGScala {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)

    // Obtain the remote cache
    val cacheManager = new RemoteCacheManager
    val cache: RemoteCache[Integer, Book] = cacheManager.getCache()

    // Put some sample data to the remote cache
    val bookOne = new Book("Linux Bible", "Negus, Chris", "2015")
    val bookTwo = new Book("Java 8 in Action", "Urma, Raoul-Gabriel", "2014")
    val bookThree = new Book("Akka", "Urma, Raoul-Gabriel", "2015")
    cache.put(1, bookOne)
    cache.put(2, bookTwo)
    cache.put(3, bookThree)

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


    // Create a SQLContext, register a data frame and table
    val sqlContext = new SQLContext(sc)

    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._

    val dataFrame = sqlContext.createDataFrame(booksRDD, classOf[Book])
    dataFrame.registerTempTable("books")

    val df: DataFrame = sqlContext.sql("SELECT author, count(*) as a from books WHERE author != 'N/A' GROUP BY author ORDER BY a desc")

    // Displays the content of the DataFrame to stdout
    df.show()

    // Print the schema in a tree format
    df.printSchema()

    // Run the Query and collect the results
    val rows: Array[Row] = df.collect()
    println(rows(0))
  }
}
