package com.github.trembita.operations

import scala.annotation.implicitNotFound
import scala.collection.parallel.immutable.ParVector
import scala.language.higherKinds

@implicitNotFound("""
    ${F} does not support `join` operation natively.
    Please provide an implicit instance in scope if necessary
    """)
trait CanJoin[F[_]] extends Serializable {
  def join[A, B](fa: F[A], fb: F[B])(on: (A, B) => Boolean): F[(A, B)]

  def joinLeft[A, B](fa: F[A], fb: F[B])(
      on: (A, B) => Boolean
  ): F[(A, Option[B])]

  def joinRight[A, B](fa: F[A], fb: F[B])(
      on: (A, B) => Boolean
  ): F[(Option[A], B)]
}

object CanJoin {
  implicit val canJoinVectors: CanJoin[Vector] = new CanJoin[Vector] {
    def join[A, B](fa: Vector[A], fb: Vector[B])(on: (A, B) => Boolean): Vector[(A, B)] =
      fa.flatMap { a =>
        fb.collect {
          case b if on(a, b) => a -> b
        }
      }
    def joinLeft[A, B](fa: Vector[A], fb: Vector[B])(
        on: (A, B) => Boolean
    ): Vector[(A, Option[B])] =
      fa.flatMap { a =>
        fb.collect {
          case b if on(a, b) => a -> Some(b)
          case _             => a -> None
        }
      }

    def joinRight[A, B](fa: Vector[A], fb: Vector[B])(
        on: (A, B) => Boolean
    ): Vector[(Option[A], B)] =
      fb.flatMap { b =>
        fa.collect {
          case a if on(a, b) => Some(a) -> b
          case _             => None    -> b
        }
      }
  }

  implicit val canJoinParVectors: CanJoin[ParVector] = new CanJoin[ParVector] {
    def join[A, B](fa: ParVector[A], fb: ParVector[B])(on: (A, B) => Boolean): ParVector[(A, B)] =
      fa.flatMap { a =>
        fb.collect {
          case b if on(a, b) => a -> b
        }
      }
    def joinLeft[A, B](fa: ParVector[A], fb: ParVector[B])(
        on: (A, B) => Boolean
    ): ParVector[(A, Option[B])] =
      fa.flatMap { a =>
        fb.collect {
          case b if on(a, b) => a -> Some(b)
          case _             => a -> None
        }
      }

    def joinRight[A, B](fa: ParVector[A], fb: ParVector[B])(
        on: (A, B) => Boolean
    ): ParVector[(Option[A], B)] =
      fb.flatMap { b =>
        fa.collect {
          case a if on(a, b) => Some(a) -> b
          case _             => None    -> b
        }
      }
  }
}
