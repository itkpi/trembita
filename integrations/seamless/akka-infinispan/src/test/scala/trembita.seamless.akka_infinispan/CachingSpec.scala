package trembita.seamless.akka_infinispan

import java.util.concurrent.{CompletableFuture, TimeUnit}
import java.util.function.Supplier

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import cats.effect.IO
import org.infinispan.commons.api.BasicCache
import org.infinispan.configuration.cache.{Configuration, ConfigurationBuilder}
import org.infinispan.manager.DefaultCacheManager
import org.scalatest.{BeforeAndAfterAll, FlatSpec, FlatSpecLike}

import scala.concurrent.duration._
import trembita._
import trembita.akka_streams._
import trembita.caching._
import akka.testkit.TestKit

import scala.concurrent.ExecutionContext
import org.mockito.Mockito._

class CachingSpec extends TestKit(ActorSystem("trembita-akka-pause")) with FlatSpecLike with BeforeAndAfterAll {
  implicit val _system: ActorSystem     = system
  implicit val mat: ActorMaterializer   = ActorMaterializer()(system)
  implicit val parallelism: Parallelism = Parallelism(4, ordered = false)
  implicit val ec: ExecutionContext     = system.dispatcher

  private val configuration: Configuration = new ConfigurationBuilder().memory().build()
  private val cacheManager                 = new DefaultCacheManager(configuration)

  private val `1 second`  = ExpirationTimeout(1.second)
  private val `5 seconds` = ExpirationTimeout(5.seconds)

  override def beforeAll(): Unit = cacheManager.start()
  override def afterAll(): Unit = {
    cacheManager.stop()
    _system.terminate()
  }

  "infinispan caching" should "cache values of sequential pipeline" in {
    implicit val caching: Caching[IO, Akka[NotUsed], Int] =
      InfinispanAkkaCaching[IO, NotUsed, Int](
        IO(cacheManager.getCache[String, (Vector[Int], NotUsed)]("test-1")),
        `5 seconds`
      ).unsafeRunSync()

    val expected = (1 to 20).map(_ + 1).toVector
    val pipeline = Input.fromSourceF[IO](Source(1 to 20))

    val resultPipeline = pipeline
      .map { i =>
        i + 1
      }
      .cached("numbers")

    val result = resultPipeline.into(Output.vector).run.unsafeRunSync()
    assert(result == expected)

    val result2 = resultPipeline.into(Output.vector).run.unsafeRunSync()
    assert(result2 == expected)
  }

  it should "evaluate pipeline before caching" in {
    val mockCache = mock(classOf[BasicCache[String, (Vector[Int], NotUsed)]])
    when(mockCache.getAsync("numbers")).thenReturn(CompletableFuture.supplyAsync(new Supplier[(Vector[Int], NotUsed)] {
      override def get(): (Vector[Int], NotUsed) = null
    }))

    val sourceSeq = (1 to 20).toVector
    val expected  = (1 to 20).map(_ + 1).toVector

    when(mockCache.putIfAbsentAsync("numbers", (sourceSeq, NotUsed)))
      .thenReturn(CompletableFuture.completedFuture[(Vector[Int], NotUsed)]((expected, NotUsed)))

    implicit val caching: Caching[IO, Akka[NotUsed], Int] =
      InfinispanAkkaCaching[IO, NotUsed, Int](
        IO(mockCache),
        `5 seconds`
      ).unsafeRunSync()

    val pipeline = Input.fromSourceF[IO](Source(sourceSeq))

    val resultPipeline = pipeline
      .map { i =>
        i + 1
      }
      .cached("numbers")

    val result = try resultPipeline.into(Output.vector).run.unsafeRunSync()
    catch {
      case e: Throwable =>
        println(e)
        e.printStackTrace()
        throw e
    }
    assert(result == expected)

    verify(mockCache, times(1)).start()
    verify(mockCache, times(1)).putIfAbsentAsync("numbers", (expected, NotUsed), 5, TimeUnit.SECONDS)
    verify(mockCache, times(1)).getAsync("numbers")
    verifyNoMoreInteractions(mockCache)
  }

  it should "not evaluate pipeline if it was cached" in {
    val mockCache = mock(classOf[BasicCache[String, (Vector[Int], NotUsed)]])

    val sourceSeq = (1 to 20).toVector
    val expected  = (1 to 20).map(_ + 1).toVector

    when(mockCache.getAsync("numbers")).thenReturn(CompletableFuture.completedFuture[(Vector[Int], NotUsed)]((expected, NotUsed)))

    implicit val caching: Caching[IO, Akka[NotUsed], Int] =
      InfinispanAkkaCaching[IO, NotUsed, Int](
        IO(mockCache),
        `5 seconds`
      ).unsafeRunSync()

    val pipeline = Input.fromSourceF[IO](Source(sourceSeq))

    val resultPipeline = pipeline
      .map { i =>
        i + 1
      }
      .cached("numbers")

    val result = resultPipeline.into(Output.vector).run.unsafeRunSync()
    assert(result == expected)

    verify(mockCache, times(1)).start()
    verify(mockCache, times(1)).getAsync("numbers")
    verifyNoMoreInteractions(mockCache)
  }
}
