package com.github.trembita.caching.infinispan

import java.util.concurrent.{CompletableFuture, TimeUnit}
import java.util.function.Supplier

import cats.effect.IO
import com.github.trembita._
import com.github.trembita.caching._
import org.infinispan.commons.api.BasicCache
import org.infinispan.configuration.cache.{Configuration, ConfigurationBuilder}
import org.infinispan.manager.DefaultCacheManager
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

import scala.concurrent.duration._
import org.mockito.Mockito._

class InfinispanDefaultCacheSpec extends FlatSpec with BeforeAndAfterAll {
  private val configuration: Configuration = new ConfigurationBuilder().memory().build()
  private val cacheManager                 = new DefaultCacheManager(configuration)

  private val `1 second`  = ExpirationTimeout(1.second)
  private val `5 seconds` = ExpirationTimeout(5.seconds)

  override def beforeAll(): Unit = cacheManager.start()
  override def afterAll(): Unit  = cacheManager.stop()

  "infinispan caching" should "cache values of sequential pipeline" in {
    implicit val caching: Caching[IO, Sequential, Int] =
      InfinispanDefaultCaching[IO, Sequential, Int](IO(cacheManager.getCache[String, Vector[Int]]("test-1")), ExpirationTimeout(5.seconds))
        .unsafeRunSync()

    val pipeline = DataPipelineT[IO, Int](1, 2, 3, 4)

    val resultPipeline = pipeline
      .map { i =>
        i + 1
      }
      .cached("numbers")

    val result = resultPipeline.eval.unsafeRunSync()
    assert(result == Vector(2, 3, 4, 5))

    val result2 = resultPipeline.eval.unsafeRunSync()
    assert(result2 == Vector(2, 3, 4, 5))
  }

  it should "evaluate pipeline before caching" in {
    val mockCache = mock(classOf[BasicCache[String, Vector[Int]]])
    when(mockCache.getAsync("numbers")).thenReturn(CompletableFuture.supplyAsync(new Supplier[Vector[Int]] { override def get(): Vector[Int] = null }))
    when(mockCache.putIfAbsentAsync("numbers", Vector(2, 3, 4, 5))).thenReturn(CompletableFuture.completedFuture(Vector(2, 3, 4, 5)))

    implicit val caching: Caching[IO, Sequential, Int] =
      InfinispanDefaultCaching[IO, Sequential, Int](IO(mockCache), `5 seconds`)
        .unsafeRunSync()

    val pipeline = DataPipelineT[IO, Int](1, 2, 3, 4)

    val resultPipeline = pipeline
      .map { i =>
        i + 1
      }
      .cached("numbers")

    val result = try resultPipeline.eval.unsafeRunSync()
    catch {
      case e: Throwable =>
        println(e)
        e.printStackTrace()
        throw e
    }
    assert(result == Vector(2, 3, 4, 5))

    verify(mockCache, times(1)).start()
    verify(mockCache, times(1)).putIfAbsentAsync("numbers", Vector(2, 3, 4, 5), 5, TimeUnit.SECONDS)
    verify(mockCache, times(1)).getAsync("numbers")
    verifyNoMoreInteractions(mockCache)
  }

  it should "not evaluate pipeline if it was cached" in {
    val mockCache = mock(classOf[BasicCache[String, Vector[Int]]])
    when(mockCache.getAsync("numbers")).thenReturn(CompletableFuture.completedFuture(Vector(2, 3, 4, 5)))
    when(mockCache.putIfAbsentAsync("numbers", Vector(2, 3, 4, 5))).thenReturn(CompletableFuture.completedFuture(Vector(2, 3, 4, 5)))

    implicit val caching: Caching[IO, Sequential, Int] =
      InfinispanDefaultCaching[IO, Sequential, Int](IO(mockCache), `5 seconds`)
        .unsafeRunSync()

    val pipeline = DataPipelineT[IO, Int](1, 2, 3, 4)

    val resultPipeline = pipeline
      .map { i =>
        i + 1
      }
      .cached("numbers")

    val result = resultPipeline.eval.unsafeRunSync()
    assert(result == Vector(2, 3, 4, 5))

    verify(mockCache, times(1)).start()
    verify(mockCache, times(1)).getAsync("numbers")
    verifyNoMoreInteractions(mockCache)
  }
}
