package com.github.trembita.operations

import cats.{~>, Applicative, Id}
import com.github.trembita.Environment
import shapeless.{=:!=, ∃}

import scala.annotation.implicitNotFound
import scala.collection.parallel.immutable.ParVector
import scala.language.higherKinds
import scala.reflect.ClassTag

@implicitNotFound("""
    Don't know how to transform ${F} into ${G} seamlessly.
    Please provide an implicit instance in scope if necessary
    """)
trait InjectTaggedK[F[_], G[_]] extends Serializable {
  def apply[A: ClassTag](fa: F[A]): G[A]
}
object InjectTaggedK {
  def fromArrow[F[_], G[_]](arrow: F ~> G): InjectTaggedK[F, G] =
    new InjectTaggedK[F, G] {
      def apply[A: ClassTag](fa: F[A]): G[A] = arrow(fa)
    }

  implicit val injectVectorIntoPar: InjectTaggedK[Vector, ParVector] =
    InjectTaggedK.fromArrow(λ[Vector[?] ~> ParVector[?]](_.par))

  implicit val injectParVectorIntoSeq: InjectTaggedK[ParVector, Vector] =
    InjectTaggedK.fromArrow(λ[ParVector[?] ~> Vector[?]](_.seq))

  def fromId[F0[_], F[_], G[_]](existing: InjectTaggedK[F, G])(implicit F0: Applicative[F0]): InjectTaggedK[F, λ[α => F0[G[α]]]] =
    new InjectTaggedK[F, λ[α => F0[G[α]]]] {
      override def apply[A: ClassTag](fa: F[A]): F0[G[A]] = F0.pure(existing(fa))
    }
}
