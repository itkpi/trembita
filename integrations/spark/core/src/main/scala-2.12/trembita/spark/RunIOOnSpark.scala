package trembita.spark

import cats.effect.IO
import org.apache.spark.rdd.RDD

import scala.concurrent.TimeoutException
import scala.reflect.ClassTag

class RunIOOnSpark(timeout: AsyncTimeout) extends RunOnSpark[IO] {
  def traverse[A, B: ClassTag](rdd: RDD[A])(f: A => IO[B]): RDD[B] =
    rdd.mapPartitions { partition =>
      val _f = f
      val mapped = partition.map(
        a =>
          f(a).unsafeRunTimed(timeout.duration).getOrElse {
            throw new TimeoutException(
              s"IO operation on spark timed out after ${timeout.duration}"
            )
        }
      )
      mapped
    }

  def lift[A](rdd: RDD[A]): IO[RDD[A]] = IO { rdd }
}
