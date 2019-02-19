package trembita

import java.nio.file.Paths
import java.util.concurrent.atomic.AtomicInteger

import cats._
import cats.effect._
import org.scalatest.FlatSpec
import cats.instances.int._

class OutputSpec extends FlatSpec {
  private val vecCheck                                               = Vector(1, 2, 3, 4, 5, 6)
  private val ppln: DataPipeline[Int, Sequential]                    = Input.sequential[Vector].create(vecCheck)
  private val pplnT: BiDataPipelineT[IO, Throwable, Int, Sequential] = Input.sequentialF[IO, Throwable, Vector].create(IO(vecCheck))
  private val sumCheck                                               = vecCheck.sum

  private val output1 = Output.vector
  private val output2 = Output.combineAll[Int]

  "Output.keepLeft" should "work correctly" in {
    val i = new AtomicInteger()
    val sum = ppln
      .into(output2)
      .alsoInto(Output.foreach[Int](_ => i.incrementAndGet()))
      .keepLeft
      .run

    assert(i.get() == 6)
    assert(sum == sumCheck)
  }

  it should "also work for IO backed pipeline" in {
    val i = new AtomicInteger()
    val sum = pplnT
      .into(output2)
      .alsoInto(Output.foreach[Int](_ => i.incrementAndGet()))
      .keepLeft
      .run
      .unsafeRunSync()

    assert(i.get() == 6)
    assert(sum == sumCheck)
  }

  "Output.keepRight" should "work correctly" in {
    val i = new AtomicInteger()
    val sum = ppln
      .into(Output.foreach[Int](_ => i.incrementAndGet()))
      .alsoInto(output2)
      .keepRight
      .run

    assert(i.get() == 6)
    assert(sum == sumCheck)
  }

  it should "also work for IO backed pipeline" in {
    val i = new AtomicInteger()
    val sum = pplnT
      .into(Output.foreach[Int](_ => i.incrementAndGet()))
      .alsoInto(output2)
      .keepRight
      .run
      .unsafeRunSync()

    assert(i.get() == 6)
    assert(sum == sumCheck)
  }

  "Output.keepBoth" should "work correctly" in {
    val (vec, sum) = ppln
      .into(output1)
      .alsoInto(output2)
      .keepBoth
      .run

    assert(vec == vecCheck)
    assert(sum == sumCheck)
  }

  it should "also work for IO backed pipeline" in {
    val (vecIO, sumIO) = pplnT
      .into(output1)
      .alsoInto(output2)
      .keepBoth
      .run

    val (vec, sum) = (for {
      vec <- vecIO
      sum <- sumIO
    } yield vec -> sum).unsafeRunSync()

    assert(vec == vecCheck)
    assert(sum == sumCheck)
  }

  "Output.ignoreBoth" should "work correctly" in {
    val i = new AtomicInteger()
    val j = new AtomicInteger()
    val resIO = pplnT
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

  "Complex chaining" should "not evaluated pipeline several times" in {
    val i = new AtomicInteger()
    val (vec, sum) = ppln
      .map { x =>
        x + i.incrementAndGet()
      }
      .into(output1)
      .alsoInto(output2)
      .keepBoth
      .run

    assert(i.get() == 6)
    assert(vec == Vector(2, 4, 6, 8, 10, 12))
    assert(sum == vec.sum)
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

    assert(i.get() == 6)
  }

  "Output.foldF" should "work correctly" in {
    val i = new AtomicInteger()
    val output = Output.foldF[IO, Int, String](zero = "") {
      case ("", x)  => IO.pure(x.toString)
      case (acc, x) => IO { s"$acc with $x" }
    }
    val result = ppln
      .mapK(idTo[IO])
      .map { x =>
        i.incrementAndGet()
        x
      }
      .into(output)
      .run
      .unsafeRunSync()

    assert(i.get() == 6)
    assert(result == "1 with 2 with 3 with 4 with 5 with 6")
  }
}
