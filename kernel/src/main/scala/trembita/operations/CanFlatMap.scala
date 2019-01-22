package trembita.operations

import trembita.Environment
import trembita._

import scala.annotation.implicitNotFound
import scala.collection.parallel.immutable.ParVector

@implicitNotFound(
  """
    .flatMap is not allowed in ${E}.
    Please ensure right implicits imported 
    or define your custom CanFlatMap for ${E}
  """
)
trait CanFlatMap[E <: Environment] extends Serializable {
  def flatMap[A, B](fa: E#Repr[A])(f: A => E#Repr[B]): E#Repr[B]
  def flatten[A](ffa: E#Repr[E#Repr[A]]): E#Repr[A] = flatMap(ffa)(identity)
}

object CanFlatMap {
  implicit def sequentialCanFlatMap: CanFlatMap[Sequential] = new CanFlatMap[Sequential] {
    def flatMap[A, B](fa: Vector[A])(f: A => Vector[B]): Vector[B] =
      fa.flatMap(f)
  }

  implicit def parallelCanFlatMap: CanFlatMap[Parallel] = new CanFlatMap[Parallel] {
    def flatMap[A, B](fa: ParVector[A])(f: A => ParVector[B]): ParVector[B] = fa.flatMap(f)
  }
}
