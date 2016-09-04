package org.jbugkorea

import java.util

import org.infinispan.client.hotrod.{RemoteCache, RemoteCacheManager, VersionedValue}
import org.infinispan.server.test.client.hotrod.EventLogListener
import org.infinispan.spark.domain.User
import org.junit.Assert._
import org.junit.{After, Before, Test}

/**
 * My Java Test Class.
 * <p/>
 *
 * @author <a href="iamtedwon@gmail.com">Ted Won</a>
 * @version 0.1.0
 */
class InfinispanScalaTest {

  @Before
  def setUp: Unit = {
  }

  @After
  def tearDown: Unit = {
  }

  @Test
  def put = {

    val cacheManager = new RemoteCacheManager
    val cache: RemoteCache[Integer, User] = cacheManager.getCache()

    val user: User = cache.get(0L)
    println(user)

    println(cache.keySet().size())

    println(cache.get("c"))

    val key = 1
    val value = new User("a", 1)
    cache.put(key, value)
    cache.put(2, new User("b", 2))

    val result = cache.get(key)

    assertNotNull(result)

  }

  @Test
  def simple = {
    val cacheManager = new RemoteCacheManager
    val cache: RemoteCache[String, String] = cacheManager.getCache()

    val key = "mykey"
    val value = "myvalue"
    cache.put(key, value)
    val myvalue: String = cache.get(key)
    println(myvalue)
    assertEquals(value, myvalue)
    cache.remove(key)
    val myvalue2: String = cache.get(key)
    println(myvalue2)
    assertNull(myvalue2)
  }

  @Test
  def ispn7 = {
    val cacheManager = new RemoteCacheManager
    val cache: RemoteCache[String, String] = cacheManager.getCache()

    val key = "mykey"
    val value = "myvalue"
    cache.put(key, value)
    val myvalue: String = cache.get(key)
    println(myvalue)
    assertEquals(value, myvalue)
    cache.remove(key)
    val myvalue2: String = cache.get(key)
    println(myvalue2)
    assertNull(myvalue2)

    cache.clear()

    for(a <- 1 to 200000) {
      cache.put(a toString, a toString)
      cache.put(a + "RemoteCache.size() now implemented with a dedicated size() operation in the protocol, which takes both persistent store, and cluster wide cache contents into account.", a + "RemoteCache.size() now implemented with a dedicated size() operation in the protocol, which takes both persistent store, and cluster wide cache contents into account.RemoteCache.size() now implemented with a dedicated size() operation in the protocol, which takes both persistent store, and cluster wide cache contents into account.RemoteCache.size() now implemented with a dedicated size() operation in the protocol, which takes both persistent store, and cluster wide cache contents into account.RemoteCache.size() now implemented with a dedicated size() operation in the protocol, which takes both persistent store, and cluster wide cache contents into account.RemoteCache.size() now implemented with a dedicated size() operation in the protocol, which takes both persistent store, and cluster wide cache contents into account.RemoteCache.size() now implemented with a dedicated size() operation in the protocol, which takes both persistent store, and cluster wide cache contents into account.RemoteCache.size() now implemented with a dedicated size() operation in the protocol, which takes both persistent store, and cluster wide cache contents into account.RemoteCache.size() now implemented with a dedicated size() operation in the protocol, which takes both persistent store, and cluster wide cache contents into account.RemoteCache.size() now implemented with a dedicated size() operation in the protocol, which takes both persistent store, and cluster wide cache contents into account.RemoteCache.size() now implemented with a dedicated size() operation in the protocol, which takes both persistent store, and cluster wide cache contents into account.RemoteCache.size() now implemented with a dedicated size() operation in the protocol, which takes both persistent store, and cluster wide cache contents into account.RemoteCache.size() now implemented with a dedicated size() operation in the protocol, which takes both persistent store, and cluster wide cache contents into account.RemoteCache.size() now implemented with a dedicated size() operation in the protocol, which takes both persistent store, and cluster wide cache contents into account.RemoteCache.size() now implemented with a dedicated size() operation in the protocol, which takes both persistent store, and cluster wide cache contents into account.RemoteCache.size() now implemented with a dedicated size() operation in the protocol, which takes both persistent store, and cluster wide cache contents into account.RemoteCache.size() now implemented with a dedicated size() operation in the protocol, which takes both persistent store, and cluster wide cache contents into account.RemoteCache.size() now implemented with a dedicated size() operation in the protocol, which takes both persistent store, and cluster wide cache contents into account.RemoteCache.size() now implemented with a dedicated size() operation in the protocol, which takes both persistent store, and cluster wide cache contents into account.RemoteCache.size() now implemented with a dedicated size() operation in the protocol, which takes both persistent store, and cluster wide cache contents into account.RemoteCache.size() now implemented with a dedicated size() operation in the protocol, which takes both persistent store, and cluster wide cache contents into account.RemoteCache.size() now implemented with a dedicated size() operation in the protocol, which takes both persistent store, and cluster wide cache contents into account.RemoteCache.size() now implemented with a dedicated size() operation in the protocol, which takes both persistent store, and cluster wide cache contents into account.RemoteCache.size() now implemented with a dedicated size() operation in the protocol, which takes both persistent store, and cluster wide cache contents into account.RemoteCache.size() now implemented with a dedicated size() operation in the protocol, which takes both persistent store, and cluster wide cache contents into account.RemoteCache.size() now implemented with a dedicated size() operation in the protocol, which takes both persistent store, and cluster wide cache contents into account.RemoteCache.size() now implemented with a dedicated size() operation in the protocol, which takes both persistent store, and cluster wide cache contents into account.RemoteCache.size() now implemented with a dedicated size() operation in the protocol, which takes both persistent store, and cluster wide cache contents into account.RemoteCache.size() now implemented with a dedicated size() operation in the protocol, which takes both persistent store, and cluster wide cache contents into account.RemoteCache.size() now implemented with a dedicated size() operation in the protocol, which takes both persistent store, and cluster wide cache contents into account.RemoteCache.size() now implemented with a dedicated size() operation in the protocol, which takes both persistent store, and cluster wide cache contents into account.RemoteCache.size() now implemented with a dedicated size() operation in the protocol, which takes both persistent store, and cluster wide cache contents into account.RemoteCache.size() now implemented with a dedicated size() operation in the protocol, which takes both persistent store, and cluster wide cache contents into account.RemoteCache.size() now implemented with a dedicated size() operation in the protocol, which takes both persistent store, and cluster wide cache contents into account.RemoteCache.size() now implemented with a dedicated size() operation in the protocol, which takes both persistent store, and cluster wide cache contents into account.RemoteCache.size() now implemented with a dedicated size() operation in the protocol, which takes both persistent store, and cluster wide cache contents into account.RemoteCache.size() now implemented with a dedicated size() operation in the protocol, which takes both persistent store, and cluster wide cache contents into account.RemoteCache.size() now implemented with a dedicated size() operation in the protocol, which takes both persistent store, and cluster wide cache contents into account.RemoteCache.size() now implemented with a dedicated size() operation in the protocol, which takes both persistent store, and cluster wide cache contents into account.RemoteCache.size() now implemented with a dedicated size() operation in the protocol, which takes both persistent store, and cluster wide cache contents into account.")
    }

    val size: Int = cache.size()
    println(size)
    val keySet: util.Set[String] = cache.keySet()
    println(keySet.size)
//    val values: util.Collection[String] = cache.values()
    val statsMap: util.Map[String, String] = cache.stats().getStatsMap
    println(statsMap)
//    val bulk: util.Map[String, String] = cache.getBulk
//    println(bulk.size)
  }

  @Test
  def clear = {
    val cacheManager = new RemoteCacheManager
    val cache: RemoteCache[String, String] = cacheManager.getCache()
    cache.clear()
  }

  @Test
  def keyset = {
    val cacheManager = new RemoteCacheManager
    val cache: RemoteCache[String, String] = cacheManager.getCache()
    for(a <- 1 to 10000)
//    println(cache.keySet.size)
    println(cache.size)
  }

  @Test
  def listener = {
    val cacheManager = new RemoteCacheManager
    val remoteCache: RemoteCache[Int, String] = cacheManager.getCache()

    val eventListener: EventLogListener = new EventLogListener
    remoteCache.addClientListener(eventListener)
    try {
//      expectNoEvents(eventListener)
      remoteCache.putIfAbsent(1, "one")
//      expectOnlyCreatedEvent(1, eventListener)
      remoteCache.putIfAbsent(1, "again")
//      expectNoEvents(eventListener)
      remoteCache.replace(1, "newone")
//      expectOnlyModifiedEvent(1, eventListener)
      remoteCache.replaceWithVersion(1, "one", 0)
//      expectNoEvents(eventListener)
      var versioned: VersionedValue[String] = remoteCache.getVersioned(1)
      remoteCache.replaceWithVersion(1, "one", versioned.getVersion)
//      expectOnlyModifiedEvent(1, eventListener)
      remoteCache.removeWithVersion(1, 0)
//      expectNoEvents(eventListener)
      versioned = remoteCache.getVersioned(1)
      remoteCache.removeWithVersion(1, versioned.getVersion)
//      expectOnlyRemovedEvent(1, eventListener)


//       val createdEvent : ClientCacheEntryCreatedEvent = eventListener.pollEvent(ClientEvent.Type.CLIENT_CACHE_ENTRY_CREATED)
//      println(createdEvent)
    }
    finally {


    }
    remoteCache.removeClientListener(eventListener)
  }

}
