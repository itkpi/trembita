package com.datarootlabs.trembita


import scala.language.higherKinds
import cats._
import cats.implicits._
import com.datarootlabs.trembita.internal.StrictSource

sealed trait Finiteness
object Finiteness {

  sealed trait Finite extends Finiteness

  sealed trait Infinite extends Finiteness

  sealed trait Concat[T1 <: Finiteness, T2 <: Finiteness] {
    type Out <: Finiteness

    def apply[A, F[_], Ex <: Execution]
    (x: DataPipeline[A, F, T1, Ex],
     y: DataPipeline[A, F, T2, Ex])
    (implicit F: MonadError[F, Throwable], Ex: Ex)
    : DataPipeline[A, F, Out, Ex]
  }

  object Concat {
    implicit object ConcatFinitePipelines extends Concat[Finite, Finite] {
      type Out = Finite

      def apply[A, F[_], Ex <: Execution]
      (x: DataPipeline[A, F, Finite, Ex],
       y: DataPipeline[A, F, Finite, Ex])
      (implicit F: MonadError[F, Throwable], Ex: Ex)
      : DataPipeline[A, F, Finite, Ex] = new StrictSource[A, F, Finite, Ex](for {
        xeval <- x.evalFunc[A]($conforms, Ex)
        yeval <- y.evalFunc[A]($conforms, Ex)
      } yield
        Ex.toVector(
          Ex.concat[A](xeval.asInstanceOf[Ex.Repr[A]], yeval.asInstanceOf[Ex.Repr[A]])
        ).toIterator)
    }
  }

  sealed trait Zip[T1 <: Finiteness, T2 <: Finiteness] {
    type Out <: Finiteness

    def apply[A, B, F[_], Ex <: Execution]
    (x: DataPipeline[A, F, T1, Ex],
     y: DataPipeline[B, F, T2, Ex])
    (implicit F: MonadError[F, Throwable], Ex: Ex)
    : DataPipeline[(A, B), F, Out, Ex]
  }

  object Zip {
    implicit object ZipFinitePipelines extends Zip[Finite, Finite] {
      type Out = Finite
      def apply[A, B, F[_], Ex <: Execution]
      (x: DataPipeline[A, F, Finite, Ex],
       y: DataPipeline[B, F, Finite, Ex])
      (implicit F: MonadError[F, Throwable], Ex: Ex)
      : DataPipeline[(A, B), F, Finite, Ex] = new StrictSource[(A, B), F, Finite, Ex](for {
        xeval <- x.evalFunc[A]($conforms, Ex)
        yeval <- y.evalFunc[B]($conforms, Ex)
      } yield
        Ex.toVector(
          Ex.zip[A, B](xeval.asInstanceOf[Ex.Repr[A]], yeval.asInstanceOf[Ex.Repr[B]])
        ).toIterator)
    }
  }
}

