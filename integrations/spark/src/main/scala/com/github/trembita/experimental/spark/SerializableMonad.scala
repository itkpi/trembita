package com.github.trembita.experimental.spark

import cats.{CoflatMap, Monad, MonadError}
import scala.language.higherKinds

trait SerializableMonad[F[_]] extends Monad[F] with Serializable

trait SerializableMonadError[F[_]]
    extends MonadError[F, Throwable]
    with Serializable

trait SerializableCoflatMap[F[_]] extends CoflatMap[F] with Serializable
