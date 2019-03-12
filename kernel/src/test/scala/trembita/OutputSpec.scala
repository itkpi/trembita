package trembita

import java.util.concurrent.atomic.AtomicInteger
import cats.effect._
import cats.instances.int._
import cats.instances.try_._
import org.scalatest.FlatSpec

class OutputSpec extends FlatSpec { self =>
  private val vec                                                   = Vector(1, 2, 3, 4, 5, 6)
  private val vecCheck                                              = vec.map(Right(_))
  private val ppln: BiDataPipelineT[IO, Throwable, Int, Sequential] = Input.sequentialF[IO, Throwable, Vector].create(IO(vec))
  private val sumCheck                                              = vec.sum
  private val output1                                               = Output.vector
  private val output2                                               = Output.combineAll[Throwable, Int]

  "Output.keepLeft" should "work correctly" in {
    val i = new AtomicInteger()
    val sum = ppln
      .into(output2)
      .alsoInto(Output.foreach[Int](_ => i.incrementAndGet()))
      .keepLeft
      .run
      .unsafeRunSync()

    assert(i.get() == 6)
    assert(sum contains sumCheck)
  }

  "Output.keepRight" should "work correctly" in {
    val i = new AtomicInteger()
    val sum = ppln
      .into(Output.foreach[Int](_ => i.incrementAndGet()))
      .alsoInto(output2)
      .keepRight
      .run
      .unsafeRunSync()

    assert(i.get() == 6)
    assert(sum contains sumCheck)
  }

  "Output.keepBoth" should "work correctly" in {
    val (vec, sum) = ppln
      .into(output1.withErrors[Throwable])
      .alsoInto(output2)
      .keepBoth
      .run

    assert(vec.unsafeRunSync() == vecCheck)
    assert(sum.unsafeRunSync() contains sumCheck)
  }

  it should "also work for IO backed pipeline" in {
    val (vecIO, sumIO) = ppln
      .into(output1.withErrors[Throwable])
      .alsoInto(output2)
      .keepBoth
      .run

    val (vec, sum) = (for {
      vec <- vecIO
      sum <- sumIO
    } yield vec -> sum).unsafeRunSync()

    assert(vec == vecCheck)
    assert(sum contains sumCheck)
  }

  "Output.ignoreBoth" should "work correctly" in {
    val i = new AtomicInteger()
    val j = new AtomicInteger()
    val resIO = ppln
      .into(Output.foreach[Int](_ => i.incrementAndGet()))
      .alsoInto(Output.foreach[Int](_ => j.incrementAndGet()))
      .ignoreBoth
      .run

    assert(i.get() == 0)
    assert(j.get() == 0)
    resIO.unsafeRunSync()
    assert(i.get() == 6)
    assert(j.get() == 6)
  }

  "Complex chaining" should "not evaluated pipeline several times (with non-lazy monad context)" in {
    val i = new AtomicInteger()
    val (vec, sum) = ppln
      .map { x =>
        val res = x + i.incrementAndGet()
        println(s"$x -> $res")
        res
      }
      .mapK(ioToTry)
      .into(output1.withErrors[Throwable])
      .alsoInto(output2)
      .keepBoth
      .run

    assert(vec.get == Vector(2, 4, 6, 8, 10, 12).map(Right(_)))
    assert(sum.get contains self.vec.foldLeft(Vector.empty[Int] -> 1)((acc, x) => (acc._1 :+ (x + acc._2), acc._2 + 1))._1.sum)
    assert(i.get() == 6)
  }

  "Output.ignore" should "work correctly" in {
    val i = new AtomicInteger()
    ppln
      .map { x =>
        i.incrementAndGet()
        x
      }
      .into(Output.ignore[Int])
      .run
      .unsafeRunSync()

    assert(i.get() == 6)
  }

  "Output.foldF" should "work correctly" in {
    val i = new AtomicInteger()
    val output = Output.foldF.failOnError[IO, Throwable, Int, String](zero = "") {
      case ("", x)  => IO.pure(x.toString)
      case (acc, x) => IO { s"$acc with $x" }
    }
    val result = ppln
      .map { x =>
        i.incrementAndGet()
        x
      }
      .into(output)
      .run
      .unsafeRunSync()

    assert(i.get() == 6)
    assert(result contains "1 with 2 with 3 with 4 with 5 with 6")
  }
}
