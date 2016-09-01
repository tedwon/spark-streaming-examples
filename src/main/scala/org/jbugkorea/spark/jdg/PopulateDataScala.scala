package org.jbugkorea.spark.jdg

import org.infinispan.client.hotrod.{RemoteCache, RemoteCacheManager}
import org.jbugkorea.User


object PopulateDataScala {

  def main(args: Array[String]): Unit = {
    // Obtain the remote cache
    val cacheManager = new RemoteCacheManager
    val cache: RemoteCache[Integer, User] = cacheManager.getCache()
    cache.clear()
    (1 to 20000).foreach { idx =>
      cache.put(idx, new User(s"name$idx", idx))
      println(idx)
      Thread.sleep(100)
    }

  }

}
