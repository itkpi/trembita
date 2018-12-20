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

  implicit def havingArrow[F[_], G[_], Ex <: Environment, Ex2 <: Environment](
      implicit existing: InjectTaggedK[Ex#Repr, λ[α => G[Ex2#Repr[α]]]],
      arrow: G ~> F,
      ev: ∃[G] =:!= ∃[Id]
  ): InjectTaggedK[Ex#Repr, λ[α => F[Ex2#Repr[α]]]] =
    new InjectTaggedK[Ex#Repr, λ[α => F[Ex2#Repr[α]]]] {
      override def apply[A: ClassTag](fa: Ex#Repr[A]): F[Ex2#Repr[A]] =
        arrow(existing(fa))
    }

  implicit def fromId[F[_], Ex <: Environment, Ex2 <: Environment](
      implicit existing: InjectTaggedK[Ex#Repr, λ[α => Ex2#Repr[α]]],
      F: Applicative[F]
  ): InjectTaggedK[Ex#Repr, λ[α => F[Ex2#Repr[α]]]] =
    new InjectTaggedK[Ex#Repr, λ[α => F[Ex2#Repr[α]]]] {
      override def apply[A: ClassTag](fa: Ex#Repr[A]): F[Ex2#Repr[A]] =
        F.pure(existing(fa))
    }
}
