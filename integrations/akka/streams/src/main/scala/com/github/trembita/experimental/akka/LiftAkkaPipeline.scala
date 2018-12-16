package com.github.trembita.experimental.akka

import akka.NotUsed
import akka.stream.Materializer
import com.github.trembita._
import com.github.trembita.operations.LiftPipeline
import scala.reflect.ClassTag
import akka.stream.scaladsl._
import cats.Monad
import cats.syntax.functor._
import scala.concurrent.ExecutionContext
import scala.language.higherKinds

class LiftAkkaPipeline[F[_]](implicit mat: Materializer,
                             ec: ExecutionContext,
                             F: Monad[F])
    extends LiftPipeline[F, Akka] {

  def liftIterable[A: ClassTag](xs: Iterable[A]): DataPipelineT[F, A, Akka] =
    DataPipelineT
      .fromRepr[F, A, Akka](Source.fromIterator(() => xs.iterator))
      .asInstanceOf[DataPipelineT[F, A, Akka]]

  def liftIterableF[A: ClassTag](
    fa: F[Iterable[A]]
  ): DataPipelineT[F, A, Akka] =
    DataPipelineT
      .fromReprF[F, A, Akka](
        fa.map(xs => Source.fromIterator(() => xs.iterator))
      )
      .asInstanceOf[DataPipelineT[F, A, Akka]]
}
