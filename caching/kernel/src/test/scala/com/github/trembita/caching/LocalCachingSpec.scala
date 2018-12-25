package com.github.trembita.caching

import cats.effect.{IO, Timer}
import org.scalatest.FlatSpec
import com.github.trembita._
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class LocalCachingSpec extends FlatSpec {
  implicit val ioTimer: Timer[IO] = IO.timer(ExecutionContext.global)

  "local caching" should "cache values of sequential pipeline" in {
    implicit val caching: Caching[IO, Sequential, Int] =
      Caching.localCaching[IO, Sequential, Int](ExpirationTimeout(5.seconds)).unsafeRunSync()
    val pipeline = DataPipelineT[IO, Int](1, 2, 3, 4)

    var captore = 0
    val resultPipeline = pipeline
      .map { i =>
        captore += 1
        i + 1
      }
      .cached("numbers")

    val result = resultPipeline.eval.unsafeRunSync()
    assert(result == Vector(2, 3, 4, 5))
    assert(captore == 4)

    val result2 = resultPipeline.eval.unsafeRunSync()
    assert(result2 == Vector(2, 3, 4, 5))
    assert(captore == 4)
  }

  it should "expire sequential pipeline after timeout" in {
    implicit val caching: Caching[IO, Sequential, Int] =
      Caching.localCaching[IO, Sequential, Int](ExpirationTimeout(1.second)).unsafeRunSync()
    val pipeline = DataPipelineT[IO, Int](1, 2, 3, 4)

    var captore = 0
    val resultPipeline = pipeline
      .map { i =>
        captore += 1
        i + 1
      }
      .cached("numbers")

    val result = resultPipeline.eval.unsafeRunSync()
    assert(result == Vector(2, 3, 4, 5))
    assert(captore == 4)

    Thread.sleep(2000)

    val result2 = resultPipeline.eval.unsafeRunSync()
    assert(result2 == Vector(2, 3, 4, 5))
    assert(captore == 8)
  }

  it should "cache values of parallel pipeline" in {
    implicit val caching: Caching[IO, Parallel, Int] =
      Caching.localCaching[IO, Parallel, Int](ExpirationTimeout(5.seconds)).unsafeRunSync()
    val pipeline = DataPipelineT[IO, Int](1, 2, 3, 4)

    var captore = 0
    val resultPipeline = pipeline
      .to[Parallel]
      .map { i =>
        captore += 1
        i + 1
      }
      .cached("numbers")

    val result = resultPipeline.eval.unsafeRunSync()
    assert(result == Vector(2, 3, 4, 5))
    assert(captore == 4)

    val result2 = resultPipeline.eval.unsafeRunSync()
    assert(result2 == Vector(2, 3, 4, 5))
    assert(captore == 4)
  }

  it should "expire parallel pipeline after timeout" in {
    implicit val caching: Caching[IO, Parallel, Int] =
      Caching.localCaching[IO, Parallel, Int](ExpirationTimeout(1.second)).unsafeRunSync()
    val pipeline = DataPipelineT[IO, Int](1, 2, 3, 4)

    var captore = 0
    val resultPipeline = pipeline
      .to[Parallel]
      .map { i =>
        captore += 1
        i + 1
      }
      .cached("numbers")

    val result = resultPipeline.eval.unsafeRunSync()
    assert(result == Vector(2, 3, 4, 5))
    assert(captore == 4)

    Thread.sleep(2000)

    val result2 = resultPipeline.eval.unsafeRunSync()
    assert(result2 == Vector(2, 3, 4, 5))
    assert(captore == 8)
  }
}
