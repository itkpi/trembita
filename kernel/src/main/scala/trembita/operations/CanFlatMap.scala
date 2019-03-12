package trembita.operations

import cats.{Applicative, Monad}
import trembita.Environment
import trembita._
import cats.implicits._

import scala.annotation.implicitNotFound
import scala.collection.parallel.immutable.ParVector
import scala.language.higherKinds

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
  implicit val sequentialCanFlatMap: CanFlatMap[Sequential] = new CanFlatMap[Sequential] {
    def flatMap[A, B](fa: Vector[A])(f: A => Vector[B]): Vector[B] =
      fa.flatMap(f)
  }

  implicit val parallelCanFlatMap: CanFlatMap[Parallel] = new CanFlatMap[Parallel] {
    def flatMap[A, B](fa: ParVector[A])(f: A => ParVector[B]): ParVector[B] = fa.flatMap(f)
  }
}

trait CanFlatTraverse[F[_], E <: Environment] extends Serializable {
  def flatTraverse[A, B](fa: E#Repr[A])(f: A => F[E#Repr[B]]): F[E#Repr[B]]
}

object CanFlatTraverse {
  implicit def canTraverseSequential[F[_]: Applicative]: CanFlatTraverse[F, Sequential] = new CanFlatTraverse[F, Sequential] {
    def flatTraverse[A, B](fa: Vector[A])(f: A => F[Vector[B]]): F[Vector[B]] = fa.flatTraverse(f)
  }
  implicit def canTraverseParallel[F[_]: Applicative]: CanFlatTraverse[F, Parallel] = new CanFlatTraverse[F, Parallel] {
    def flatTraverse[A, B](fa: ParVector[A])(f: A => F[ParVector[B]]): F[ParVector[B]] = fa.map(f(_).map(_.seq)).seq.flatSequence.map(_.par)
  }
}
