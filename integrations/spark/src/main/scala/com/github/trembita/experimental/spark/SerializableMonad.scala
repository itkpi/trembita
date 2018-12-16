package com.github.trembita.experimental.spark

import cats.{CoflatMap, Id, Monad, MonadError}
import scala.annotation.tailrec
import scala.language.higherKinds

trait SerializableMonad[F[_]] extends Monad[F] with Serializable

object SerializableMonad {
  implicit val idMonad: SerializableMonad[Id] = new SerializableMonad[Id] {
    def pure[A](x: A): Id[A] = x
    def flatMap[A, B](fa: Id[A])(f: A => Id[B]): Id[B] = f(fa)
    @tailrec def tailRecM[A, B](a: A)(f: A => Either[A, B]): B = f(a) match {
      case Left(a1) => tailRecM(a1)(f)
      case Right(b) => b
    }
  }
}

trait SerializableMonadError[F[_]]
    extends MonadError[F, Throwable]
    with Serializable

trait SerializableCoflatMap[F[_]] extends CoflatMap[F] with Serializable
