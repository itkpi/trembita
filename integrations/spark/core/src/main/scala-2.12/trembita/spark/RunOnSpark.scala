package trembita.spark

import cats.Id
import org.apache.spark.rdd.RDD

import scala.language.experimental.macros
import scala.language.higherKinds
import scala.reflect.ClassTag

trait RunOnSpark[F[_]] extends Serializable {
  def traverse[A, B: ClassTag](a: RDD[A])(f: A => F[B]): RDD[B]
  def lift[A](rdd: RDD[A]): F[RDD[A]]
}

class RunIdOnSpark extends RunOnSpark[Id] {
  def lift[A](rdd: RDD[A]): Id[RDD[A]] = rdd

  def traverse[A, B: ClassTag](a: RDD[A])(f: A => Id[B]): RDD[B] = a.map(f)
}
