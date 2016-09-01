package org.jbugkorea.spark.jdg

import java.util.Properties

import com.google.common.collect.Sets
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller
import org.infinispan.client.hotrod.{RemoteCache, RemoteCacheManager, Search}
import org.infinispan.protostream.FileDescriptorSource
import org.infinispan.query.dsl.Query
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants
import org.infinispan.spark.rdd.InfinispanRDD
import org.jbugkorea.{Author, AuthorMarshaller, Book, BookMarshaller}

object FilteringRDDByQueryTry1 {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)

    // Obtain the remote cache
    val cacheManager = new RemoteCacheManager(
      new ConfigurationBuilder().addServer().host("127.0.0.1").port(11222).marshaller(new ProtoStreamMarshaller).build()
    )
    val serializationContext = ProtoStreamMarshaller.getSerializationContext(cacheManager)
    serializationContext.registerProtoFiles(FileDescriptorSource.fromResources("library.proto"))
    serializationContext.registerMarshaller(new BookMarshaller)
    serializationContext.registerMarshaller(new AuthorMarshaller)


    // obtain the '__protobuf_metadata' cache
    val metadataCache: RemoteCache[String, String] =
    cacheManager.getCache(
      ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);

    val schemaFileContents = "package book_sample;\nmessage Book {\n    required string title = 1;\n    required string description = 2;\n    required int32 publicationYear = 3; // no native Date type available in Protobuf\n\n    repeated Author authors = 4;\n}\nmessage Author {\n    required string name = 1;\n    required string surname = 2;\n}"
    metadataCache.put("my_protobuf_schema.proto", schemaFileContents);



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
      .setAppName("spark-infinispan-example-filter-RDD-scala")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val infinispanProperties = new Properties
    infinispanProperties.put("infinispan.client.hotrod.server_list", infinispanHost)
    infinispanProperties.put("infinispan.rdd.cacheName", "default")

    // Filtering by a Query
    val query: Query = Search.getQueryFactory(cacheManager.getCache())
      .from(classOf[Book])
      .having("description").like("desc")
      .toBuilder.build()

    val serializationContext2 = ProtoStreamMarshaller.getSerializationContext(cacheManager)
    serializationContext2.registerProtoFiles(FileDescriptorSource.fromResources("library.proto"))
    serializationContext2.registerMarshaller(new BookMarshaller)
    serializationContext2.registerMarshaller(new AuthorMarshaller)



    // Create RDD from cache
    val infinispanRDD = new InfinispanRDD[Integer, Book](sc, configuration = infinispanProperties)
      .filterByQuery[Book](query, classOf[Book])

    infinispanRDD.foreach(println)

    val booksRDD: RDD[Book] = infinispanRDD.values

    val count = booksRDD.count()

    println(count)

  }
}
