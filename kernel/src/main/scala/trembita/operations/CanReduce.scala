package trembita.operations

import cats.Id

import scala.annotation.implicitNotFound
import scala.collection.parallel.immutable.ParVector
import scala.language.higherKinds
import scala.reflect.ClassTag

@implicitNotFound("""
    ${F} does not support fold operation or it is not efficient.
    If you want to fold ${F}, please provide an implicit instance in scope
  """)
trait CanReduce[F[_]] extends Serializable {
  type Result[_]

  def reduceOpt[A: ClassTag](fa: F[A])(f: (A, A) => A): Result[Option[A]]

  def reduce[A: ClassTag](fa: F[A])(f: (A, A) => A): Result[A]

}

object CanReduce {
  type Aux[F[_], R0[_]] = CanReduce[F] { type Result[x] = R0[x] }

  implicit val canReduceVector: CanReduce.Aux[Vector, Id]       = CanFold.canFoldVector
  implicit val canReduceParVector: CanReduce.Aux[ParVector, Id] = CanFold.canFoldParVector
}
