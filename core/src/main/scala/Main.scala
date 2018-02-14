import com.honta.trembita.LazyList

import scala.concurrent.ExecutionContext


object Main {

  import ExecutionContext.Implicits.global

  def main(args: Array[String]): Unit = {
    val lazyList: LazyList[Int] = LazyList.from(
      scala.util.Random.shuffle(1 to 100000)
    )

    val first10: Iterable[Int] = lazyList.take(10)
    println(s"first 10: $first10")
    val sorted: Iterable[Int] = lazyList.sorted.take(10)
    println(s"sorted: $sorted")
    val parSorted: Iterable[Int] = lazyList.par.sorted.take(10)
    println(s"Sorted (par): $parSorted")
  }
}
