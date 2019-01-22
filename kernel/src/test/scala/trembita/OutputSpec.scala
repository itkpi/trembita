package trembita

import java.nio.file.Paths

import cats._
import cats.effect._
import org.scalatest.FlatSpec
import cats.instances.int._

class OutputSpec extends FlatSpec {
  private val vecCheck                                  = Vector(1, 2, 3, 4, 5, 6)
  private val ppln: DataPipeline[Int, Sequential]       = Input.sequential[Vector].create(vecCheck)
  private val pplnT: DataPipelineT[IO, Int, Sequential] = Input.sequentialF[IO, Vector].create(IO(vecCheck))
  private val sumCheck                                  = vecCheck.sum

  private val output1 = Output.vector
  private val output2 = Output.combineAll[Int]

  "Output.keepLeft" should "work correctly" in {
    var i = 0
    val sum = ppln
      .into(output2)
      .alsoInto(Output.foreach[Int](_ => i += 1))
      .keepLeft
      .run

    assert(i == 6)
    assert(sum == sumCheck)
  }

  it should "also work for IO backed pipeline" in {
    var i = 0
    val sum = pplnT
      .into(output2)
      .alsoInto(Output.foreach[Int](_ => i += 1))
      .keepLeft
      .run
      .unsafeRunSync()

    assert(i == 6)
    assert(sum == sumCheck)
  }

  "Output.keepRight" should "work correctly" in {
    var i = 0
    val sum = ppln
      .into(Output.foreach[Int](_ => i += 1))
      .alsoInto(output2)
      .keepRight
      .run

    assert(i == 6)
    assert(sum == sumCheck)
  }

  it should "also work for IO backed pipeline" in {
    var i = 0
    val sum = pplnT
      .into(Output.foreach[Int](_ => i += 1))
      .alsoInto(output2)
      .keepRight
      .run
      .unsafeRunSync()

    assert(i == 6)
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
    var i = 0
    var j = 0
    val resIO = pplnT
      .into(Output.foreach[Int](_ => i += 1))
      .alsoInto(Output.foreach[Int](_ => j += 1))
      .ignoreBoth
      .run

    assert(i == 0)
    assert(j == 0)
    resIO.unsafeRunSync()
    assert(i == 6)
    assert(j == 6)
  }

  "Complex chaining" should "not evaluated pipeline several times" in {
    var i = 0
    val (vec, sum) = ppln
      .map { x =>
        i += 1
        x + i
      }
      .into(output1)
      .alsoInto(output2)
      .keepBoth
      .run

    assert(i == 6)
    assert(vec == Vector(2, 4, 6, 8, 10, 12))
    assert(sum == vec.sum)
  }
}
