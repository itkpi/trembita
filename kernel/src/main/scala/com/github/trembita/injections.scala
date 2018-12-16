package com.github.trembita

import cats.{InjectK, ~>}

import scala.collection.parallel.immutable.ParVector
import scala.language.higherKinds
import scala.reflect.ClassTag

trait InjectTaggedK[F[_], G[_]] extends Serializable {
  def apply[A: ClassTag](fa: F[A]): G[A]
}
object InjectTaggedK {
  def fromArrow[F[_], G[_]](arrow: F ~> G): InjectTaggedK[F, G] =
    new InjectTaggedK[F, G] {
      def apply[A: ClassTag](fa: F[A]): G[A] = arrow(fa)
    }
}

trait injections {
  implicit val injectVectorIntoPar: InjectTaggedK[Vector, ParVector] =
    InjectTaggedK.fromArrow(λ[Vector[?] ~> ParVector[?]](_.par))

  implicit val injectParVectorIntoSeq: InjectTaggedK[ParVector, Vector] =
    InjectTaggedK.fromArrow(λ[ParVector[?] ~> Vector[?]](_.seq))
}
