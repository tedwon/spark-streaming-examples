package org.jbugkorea.jdg.hotrod;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.protostream.FileDescriptorSource;
import org.infinispan.protostream.SerializationContext;
import org.jbugkorea.Author;
import org.jbugkorea.AuthorMarshaller;
import org.jbugkorea.Book;
import org.jbugkorea.BookMarshaller;

public class MyProtoStreamMarshaller {

    public static void main(String[] args) throws Exception {
        ConfigurationBuilder clientBuilder = new ConfigurationBuilder();
        clientBuilder.addServer()
                .host("127.0.0.1").port(11222)
                .marshaller(new ProtoStreamMarshaller());

        RemoteCacheManager remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
        SerializationContext serCtx =
                ProtoStreamMarshaller.getSerializationContext(remoteCacheManager);
        serCtx.registerProtoFiles(FileDescriptorSource.fromResources("library.proto"));
        serCtx.registerMarshaller(new BookMarshaller());
        serCtx.registerMarshaller(new AuthorMarshaller());

// Book and Author classes omitted for brevity
        RemoteCache<Integer, Author> cache = remoteCacheManager.getCache();
        cache.put(100, new Author("ted", "won"));

        System.out.println(cache.get(100));
    }
}
