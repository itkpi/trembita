package com.github.trembita.akka

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

class LiftAkkaPipeline[F[_]](implicit mat: Materializer, ec: ExecutionContext, F: Monad[F]) extends LiftPipeline[F, Akka[NotUsed]] {

  def liftIterable[A: ClassTag](xs: Iterable[A]): DataPipelineT[F, A, Akka[NotUsed]] =
    DataPipelineT
      .fromRepr[F, A, Akka[NotUsed]](Source.fromIterator(() => xs.iterator))

  def liftIterableF[A: ClassTag](
      fa: F[Iterable[A]]
  ): DataPipelineT[F, A, Akka[NotUsed]] =
    DataPipelineT
      .fromReprF[F, A, Akka[NotUsed]](
        fa.map(xs => Source.fromIterator(() => xs.iterator))
      )
}
