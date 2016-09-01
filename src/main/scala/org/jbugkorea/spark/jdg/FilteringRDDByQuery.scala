package org.jbugkorea.spark.jdg

import java.io._

import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller
import org.infinispan.client.hotrod.{RemoteCache, RemoteCacheManager}
import org.infinispan.protostream.FileDescriptorSource
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants
import org.jboss.as.quickstarts.datagrid.hotrod.query.domain.Person
import org.jboss.as.quickstarts.datagrid.hotrod.query.marshallers.{PersonMarshaller, PhoneNumberMarshaller, PhoneTypeMarshaller}

object FilteringRDDByQuery {

  private val PROTOBUF_DEFINITION_RESOURCE = "/quickstart/addressbook.proto"

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)

    // Obtain the remote cache
    val cacheManager = new RemoteCacheManager(
      new ConfigurationBuilder().addServer().host("127.0.0.1").port(11222).marshaller(new ProtoStreamMarshaller).build()
    )

    val cache: RemoteCache[Integer, Object] = cacheManager.getCache("addressbook_indexed")

    // Register entity marshallers on the client side ProtoStreamMarshaller instance associated with the remote cache manager.
    val serializationContext = ProtoStreamMarshaller.getSerializationContext(cacheManager)
    serializationContext.registerProtoFiles(FileDescriptorSource.fromResources(PROTOBUF_DEFINITION_RESOURCE))
    serializationContext.registerMarshaller(new PersonMarshaller)
    serializationContext.registerMarshaller(new PhoneNumberMarshaller)
    serializationContext.registerMarshaller(new PhoneTypeMarshaller)

    // register the schemas with the server too
    // obtain the '__protobuf_metadata' cache
    val metadataCache: RemoteCache[String, String] =
    cacheManager.getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME)
    metadataCache.put("my_protobuf_schema.proto", readResource(PROTOBUF_DEFINITION_RESOURCE))


    val person = new Person
    person.setId(0)
    person.setName("a")
    person.setEmail("a")

    // put the Person in cache
    cache.put(person.getId, person)


  }

  private def readResource(resourcePath: String): String = {
    val is = getClass.getResourceAsStream(resourcePath)
    val contents = org.jbugkorea.utils.Util.readResource(is)
    println(contents)
    contents
  }
}
