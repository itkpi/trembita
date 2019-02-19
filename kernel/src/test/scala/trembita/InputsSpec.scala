package trembita

import java.nio.file.Paths
import cats._
import cats.effect._
import org.scalatest.FlatSpec

class InputsSpec extends FlatSpec {
  "Input.randomInts" should "work correctly" in {
    val rands: Vector[String] = Input.random
      .create(RandomInput.props[String](n = 10, count = 100)(_.toString))
      .into(Output.vector)
      .run

    assert(rands.size == 100)
  }

  "Input.repeat" should "work correctly" in {
    val rands: Vector[String] = Input.repeat
      .create(RepeatInput.props(count = 100)("Foo"))
      .into(Output.vector)
      .run

    assert(rands.size == 100)
    assert(rands.forall(_ == "Foo"))
  }

  "Input.repeatF" should "work correctly" in {
    val rands: IO[Vector[String]] = Input
      .repeatF[IO, Throwable]
      .create(RepeatInput.propsT(count = 100)(IO { "Foo" }))
      .into(Output.vector)
      .run

    val result = rands.unsafeRunSync()
    assert(result.size == 100)
    assert(result.forall(_ == "Foo"))
  }

  "Input.repr" should "work correctly" in {
    val rands: Vector[Int] = Input
      .repr[Parallel]
      .create(Vector(1, 2, 3).par)
      .into(Output.vector)
      .run

    assert(rands == Vector(1, 2, 3))
  }

  "Input.file" should "work correctly" in {
    val rands: Vector[String] = Input.file
      .create(FileInput.props(Paths.get("kernel/src/test/resources/test.txt")))
      .into(Output.vector)
      .run

    assert(rands == Vector("foo", "bar", "baz", "foo1", "bar1", "baz1"))
  }
}
