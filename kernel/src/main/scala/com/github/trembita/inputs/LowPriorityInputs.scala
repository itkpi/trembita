package com.github.trembita.inputs

import cats.Id
import com.github.trembita.{InputT, Sequential}
import scala.language.higherKinds

trait LowPriorityInputs {
  implicit def sequentialInputF[F[_]]: InputT.Aux[F, Sequential, λ[α => F[Iterable[α]]]] =
    new IterableInputF[F]

  def empty: InputT.Aux[Id, Sequential, λ[α => Unit]] = new EmptyInput
}
